# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Any, Dict, Optional

import requests
from requests import Response

from materialize.cloudtest.config.environment_config import EnvironmentConfig
from materialize.cloudtest.util.common import retry
from materialize.cloudtest.util.controller import wait_for_connectable
from materialize.cloudtest.util.kubectl import Kubectl
from materialize.cloudtest.util.web_request import delete, get, patch


def create_environment_assignment(
    config: EnvironmentConfig,
    image: Optional[str] = None,
) -> Dict[str, Any]:
    environment_assignment = f"{config.auth.organization_id}-0"
    environment = f"environment-{environment_assignment}"

    json: dict[str, Any] = {}
    if image is not None:
        json["environmentdImageRef"] = image
    patch(
        config,
        config.controllers.region_api_server.configured_base_url(),
        "/api/region",
        json,
    )
    env_kubectl = Kubectl(config.environment_context)
    sys_kubectl = Kubectl(config.system_context)

    env_kubectl.get_retry(
        None,
        "environment",
        environment,
        10,
    )
    return sys_kubectl.get(
        None,
        "environmentassignment",
        environment_assignment,
    )


def wait_for_environmentd(config: EnvironmentConfig) -> Dict[str, Any]:
    def get_environment() -> Response:
        response = get(
            config,
            config.controllers.region_api_server.configured_base_url(),
            "/api/region",
        )
        region_info = response.json().get("regionInfo")
        assert region_info
        assert region_info.get("resolvable")
        assert region_info.get("sqlAddress")
        return response

    environment_json: Dict[str, Any] = retry(
        get_environment, 600, [AssertionError]
    ).json()
    pgwire_url = environment_json["regionInfo"]["sqlAddress"]
    (pgwire_host, pgwire_port) = pgwire_url.split(":")
    wait_for_connectable((pgwire_host, int(pgwire_port)), 300)
    return environment_json


def delete_environment_assignment(config: EnvironmentConfig) -> None:
    environment_assignment = f"{config.auth.organization_id}-0"
    environment = f"environment-{environment_assignment}"

    def delete_environment() -> None:
        delete(
            config,
            config.controllers.region_api_server.configured_base_url(),
            "/api/region",
            # we have a 60 second timeout in the region api's load balancer
            # for this call and a 5 minute timeout in the region api (which
            # is relevant when running in kind)
            timeout=305,
        )

    retry(delete_environment, 10, [requests.exceptions.HTTPError])

    env_kubectl = Kubectl(config.environment_context)
    sys_kubectl = Kubectl(config.system_context)

    assert (
        env_kubectl.get_or_none(
            namespace=None,
            resource_type="namespace",
            resource_name=environment,
        )
        is None
    )
    assert (
        env_kubectl.get_or_none(
            namespace=None,
            resource_type="environment",
            resource_name=environment,
        )
        is None
    )
    assert (
        sys_kubectl.get_or_none(
            namespace=None,
            resource_type="environmentassignment",
            resource_name=environment_assignment,
        )
        is None
    )


def wait_for_no_environmentd(config: EnvironmentConfig) -> None:
    # Confirm the Region API is not returning the environment
    def get_environment() -> None:
        res = get(
            config,
            config.controllers.region_api_server.configured_base_url(),
            "/api/region",
        )
        # a 204 indicates no region is found
        if res.status_code != 204:
            raise AssertionError()

    retry(get_environment, 600, [AssertionError])

    # Confirm the environment resource is gone
    environment_assignment = f"{config.auth.organization_id}-0"
    environment = f"environment-{environment_assignment}"
    env_kubectl = Kubectl(config.environment_context)

    def get_k8s_environment() -> None:
        assert (
            env_kubectl.get_or_none(
                namespace=None,
                resource_type="environment",
                resource_name=environment,
            )
            is None
        )

    retry(get_k8s_environment, 600, [AssertionError])

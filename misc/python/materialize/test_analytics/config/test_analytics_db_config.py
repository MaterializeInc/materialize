# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os

from materialize import buildkite
from materialize.mz_env_util import get_cloud_hostname
from materialize.mzcompose.composition import Composition
from materialize.test_analytics.config.mz_db_config import MzDbConfig


def create_test_analytics_config(c: Composition) -> MzDbConfig:
    """This requires the "mz" service in the composition."""
    app_password = os.getenv("PRODUCTION_ANALYTICS_MZ_CLI_APP_PASSWORD") or os.getenv(
        "PRODUCTION_ANALYTICS_APP_PASSWORD"
    )

    if app_password is not None:
        hostname = get_cloud_hostname(c, app_password=app_password)
    else:
        hostname = "unknown"

    return create_test_analytics_config_with_hostname(hostname)


def create_test_analytics_config_with_hostname(hostname: str) -> MzDbConfig:
    username = os.getenv("PRODUCTION_ANALYTICS_USERNAME", "infra+bot@materialize.com")
    app_password = os.getenv("PRODUCTION_ANALYTICS_APP_PASSWORD")
    config = create_test_analytics_config_with_credentials(
        hostname, username, app_password
    )

    if hostname == "unknown":
        print("Disabling test-analytics, host is unknown")
        config.enabled = False

    return config


def create_test_analytics_config_with_credentials(
    hostname: str, username: str, app_password: str | None
) -> MzDbConfig:
    database = "raw"
    search_path = "test_analytics"
    cluster = "test_analytics"

    # disable test_analytics access when running locally
    enabled = buildkite.is_in_buildkite()

    return MzDbConfig(
        hostname=hostname,
        username=username,
        app_password=app_password,
        database=database,
        search_path=search_path,
        cluster=cluster,
        enabled=enabled,
        application_name="test-analytics",
    )

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent

from materialize.cloudtest.app.materialize_application import MaterializeApplication
from materialize.cloudtest.config.environment_config import EnvironmentConfig
from materialize.cloudtest.util.environment import wait_for_environmentd
from materialize.cloudtest.util.kubectl import kubectl_get


def test_simple(
    app: MaterializeApplication,
    config: EnvironmentConfig,
):
    check_crd_creation(
        config.system_context,
        config.environment_context,
    )

    environment = wait_for_environmentd(config)
    pg_wire_url = environment["regionInfo"]["sqlAddress"]

    app.testdrive.run(
        input=dedent(
            """
        > SELECT 1;
        1
        """,
        ),
        materialize_url=f"postgres://{pg_wire_url}",
    )


def check_crd_creation(
    system_context: str,
    environment_context: str,
):
    kubectl_get(
        system_context,
        None,
        "crd",
        "environmentassignments.materialize.cloud",
    )
    kubectl_get(
        environment_context,
        None,
        "crd",
        "environments.materialize.cloud",
    )

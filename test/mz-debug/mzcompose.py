# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Basic test for mz-debug
"""

from dataclasses import dataclass

from materialize import spawn
from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz_debug import MzDebug

SERVICES = [
    Materialized(
        ports=[
            "6875:6875",
            "6877:6877",
        ]
    ),
    MzDebug(),
]


@dataclass
class TestCase:
    name: str
    dbt_env: dict[str, str]
    materialized_options: list[str]
    materialized_image: str | None = None


test_cases = [
    TestCase(
        name="no-tls-cloud",
        materialized_options=[],
        dbt_env={},
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.up("materialized", Service("mz-debug", idle=True))
    c.invoke("cp", "mz-debug:/usr/local/bin/mz-debug", ".")
    container_id = c.container_id("materialized")
    if container_id is None:
        raise ValueError("Failed to get materialized container ID")

    spawn.runv(
        [
            "./mz-debug",
            "emulator",
            "--docker-container-id",
            container_id,
            "--mz-connection-url",
            "postgres://mz_system@localhost:6877/materialize",
        ]
    )

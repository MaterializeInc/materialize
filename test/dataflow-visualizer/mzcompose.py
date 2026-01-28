# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
E2E browser tests for the dataflow visualizer React components.
Tests the /memory and /hierarchical-memory endpoints on port 6876.
"""
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.service import Service
from materialize.mzcompose.services.materialized import Materialized

SERVICES = [
    Materialized(),
    Service(
        name="playwright",
        config={
            "image": "mcr.microsoft.com/playwright:v1.58.0-jammy",
            "volumes": [
                ".:/workdir",
            ],
            "environment": [
                "MZ_HOST=materialized",
            ],
        },
    ),
]


def workflow_default(c: Composition) -> None:
    """Run dataflow visualizer E2E tests"""
    c.up("materialized")
    c.run("playwright", "/workdir/run-tests.sh")

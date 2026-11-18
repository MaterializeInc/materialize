# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Basic test for Postgres-compatible connections from Python programming language.
"""

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.service import Service
from materialize.mzcompose.services.materialized import Materialized

SERVICES = [
    Materialized(),
    Service(
        name="python",
        config={
            # renovate: datasource=docker depName=python versioning=docker
            "image": "python:3.14.7-trixie@sha256:0876e54cf728d89fd9d0fdaf5837b9ee879ea5fbbd6fd0cddbe5eb0cce3f5f9e",
            "volumes": [
                "../../../:/workdir",
            ],
            "environment": [
                "PGHOST=materialized",
            ],
        },
    ),
]


def workflow_default(c: Composition) -> None:
    c.up("materialized")
    c.run("python", "/workdir/test/lang/python/test.sh")

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Basic test for Postgres-compatible connections from C# programming language.
"""

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.service import Service
from materialize.mzcompose.services.materialized import Materialized

SERVICES = [
    Materialized(),
    Service(
        name="csharp",
        config={
            # renovate: datasource=docker depName=mcr.microsoft.com/dotnet/sdk versioning=docker
            "image": "mcr.microsoft.com/dotnet/sdk:8.0@sha256:1b955f179b02df4f0eb5fadedaa898cbe44d620a64aab52eec72c8e91d1355b5",
            "volumes": [
                "../../../:/workdir",
            ],
        },
    ),
]


def workflow_default(c: Composition) -> None:
    c.up("materialized")
    c.run("csharp", "/workdir/test/lang/csharp/test.sh")

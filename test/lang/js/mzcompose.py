# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Basic test for Postgres-compatible connections from JavaScript programming language.
"""

from materialize.mzcompose.composition import Composition, Service
from materialize.mzcompose.services.materialized import Materialized

SERVICES = [
    Materialized(),
    Service(
        name="js",
        config={
            "image": "node:20.10.0",
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
    c.run("js", "/workdir/test/lang/js/test.sh")

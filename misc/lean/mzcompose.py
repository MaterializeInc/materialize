# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Builds and checks the Lean 4 models in this directory.
"""

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.service import Service

SERVICES = [
    Service(
        name="lean",
        config={
            "mzbuild": "lean",
            "volumes": [".:/src:ro"],
        },
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run `lake` with the given arguments, `lake build` by default."""
    parser.add_argument("lake_args", nargs="*", default=["build"])
    args = parser.parse_args()
    c.run("lean", *args.lake_args, rm=True)

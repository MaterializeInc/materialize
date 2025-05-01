# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from dataclasses import dataclass

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mcp import Mcp

SERVICES = [Materialized(), Mcp()]


@dataclass
class TestCase:
    name: str
    materialized_options: list[str]
    materialized_image: str | None = None


test_cases = [
    TestCase(
        name="mcp-server",
        materialized_options=[],
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "filter", nargs="?", default="", help="limit to test cases matching filter"
    )
    parser.add_argument(
        "-k", nargs="?", default=None, help="limit tests by keyword expressions"
    )
    parser.add_argument("-s", action="store_true", help="don't suppress output")
    args = parser.parse_args()

    for test_case in test_cases:
        if args.filter in test_case.name:
            print(f"> Running test case {test_case.name}")
            materialized = Materialized(
                options=test_case.materialized_options,
                image=test_case.materialized_image,
                volumes_extra=["secrets:/secrets"],
                default_replication_factor=1,
                additional_system_parameter_defaults={
                    "default_cluster_replication_factor": "1"
                },
            )
            test_args = ["tests/"]
            if args.k:
                test_args.append(f"-k {args.k}")
            if args.s:
                test_args.append("-s")

            with c.test_case(test_case.name):
                with c.override(materialized):
                    c.down()
                    c.up("materialized")

                    c.run(
                        "mcp",
                        "uv",
                        "run",
                        "pytest",
                        *test_args,
                        env_extra={
                            "MZ_DSN": "postgres://materialize@materialized:6875/materialize",
                        },
                    )

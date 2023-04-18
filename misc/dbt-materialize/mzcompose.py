# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from dataclasses import dataclass
from typing import Dict, List, Optional

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import Materialized, Redpanda, Service

SERVICES = [
    Materialized(),
    Redpanda(),
    Service(
        "dbt-test",
        {
            "mzbuild": "dbt-materialize",
            "environment": [
                "TMPDIR=/share/tmp",
            ],
            "volumes": [
                "secrets:/secrets",
                "tmp:/share/tmp",
            ],
        },
    ),
]


@dataclass
class TestCase:
    name: str
    dbt_env: Dict[str, str]
    materialized_options: List[str]
    materialized_image: Optional[str] = None


test_cases = [
    TestCase(
        name="no-tls-cloud",
        materialized_options=[],
        dbt_env={},
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "filter", nargs="?", default="", help="limit to test cases matching filter"
    )
    parser.add_argument(
        "-k", nargs="?", default=None, help="limit tests by keyword expressions"
    )
    args = parser.parse_args()

    for test_case in test_cases:
        if args.filter in test_case.name:
            print(f"> Running test case {test_case.name}")
            materialized = Materialized(
                options=test_case.materialized_options,
                image=test_case.materialized_image,
                volumes_extra=["secrets:/secrets"],
            )
            test_args = ["dbt-materialize/tests"]
            if args.k:
                test_args.append(f"-k {args.k}")

            with c.test_case(test_case.name):
                with c.override(materialized):
                    c.down()
                    c.up("redpanda")
                    c.up("materialized")
                    c.run(
                        "dbt-test",
                        "pytest",
                        *test_args,
                        env_extra={
                            "DBT_HOST": "materialized",
                            "KAFKA_ADDR": "redpanda:9092",
                            "SCHEMA_REGISTRY_URL": "http://schema-registry:8081",
                            **test_case.dbt_env,
                        },
                    )

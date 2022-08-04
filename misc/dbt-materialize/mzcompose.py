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
from materialize.mzcompose.services import Materialized, Redpanda, Service, TestCerts

SERVICES = [
    TestCerts(),
    Materialized(),
    Redpanda(),
    Service(
        "dbt-test",
        {
            "mzbuild": "dbt-materialize",
            "depends_on": ["test-certs"],
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
        name="no-tls",
        materialized_options=[],
        dbt_env={},
    ),
    TestCase(
        name="tls",
        materialized_options=[
            "--tls-mode=verify-ca",
            "--tls-cert=/secrets/materialized.crt",
            "--tls-key=/secrets/materialized.key",
            "--tls-ca=/secrets/ca.crt",
        ],
        dbt_env={
            "DBT_SSLCERT": "/secrets/postgres.crt",
            "DBT_SSLKEY": "/secrets/postgres.key",
            "DBT_SSLROOTCERT": "/secrets/ca.crt",
        },
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Runs the dbt adapter test suite against Materialize in various configurations."""
    parser.add_argument(
        "filter", nargs="?", default="", help="limit to test cases matching filter"
    )
    args = parser.parse_args()

    for test_case in test_cases:
        if args.filter in test_case.name:
            print(f"> Running test case {test_case.name}")
            materialized = Materialized(
                options=test_case.materialized_options,
                image=test_case.materialized_image,
                depends_on=["test-certs"],
                volumes_extra=["secrets:/secrets"],
            )

            with c.test_case(test_case.name):
                with c.override(materialized):
                    c.down()
                    c.start_and_wait_for_tcp(services=["redpanda"])
                    c.up("materialized")
                    c.wait_for_tcp(host="materialized", port=6875)
                    c.run(
                        "dbt-test",
                        "pytest",
                        "dbt-materialize/test",
                        env_extra={
                            "DBT_HOST": "materialized",
                            "KAFKA_ADDR": "redpanda:9092",
                            "SCHEMA_REGISTRY_URL": "http://schema-registry:8081",
                            **test_case.dbt_env,
                        },
                    )

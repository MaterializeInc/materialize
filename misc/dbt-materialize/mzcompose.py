# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Basic test for the Data build tool (dbt) integration of Materialize
"""

import os
from dataclasses import dataclass
from textwrap import dedent
from typing import Dict, List, Optional

from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.dbt import Dbt
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    Materialized(),
    Redpanda(),
    Dbt(),
    Testdrive(seed=1),
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
    parser.add_argument("-s", action="store_true", help="don't suppress output")
    args = parser.parse_args()

    c.up(Service("dbt", idle=True))

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
            test_args = ["dbt-materialize/tests"]
            if args.k:
                test_args.append(f"-k {args.k}")
            if args.s:
                test_args.append("-s")

            with c.test_case(test_case.name):
                with c.override(materialized):
                    c.down()
                    c.up(
                        "redpanda",
                        "materialized",
                        Service("testdrive", idle=True),
                    )

                    # Create a topic that some tests rely on
                    c.testdrive(
                        input=dedent(
                            """
                                $ kafka-create-topic topic=test-source partitions=1
                                $ kafka-create-topic topic=test-sink partitions=1
                                """
                        )
                    )

                    # Set enable_create_table_from_source to true
                    c.testdrive(
                        input=dedent(
                            """
                                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                                ALTER SYSTEM SET enable_create_table_from_source = true
                                """
                        )
                    )

                    # Give the test harness permission to modify the built-in
                    # objects as necessary.
                    for what in [
                        "DATABASE materialize",
                        "SCHEMA materialize.public",
                        "CLUSTER quickstart",
                    ]:
                        c.sql(
                            service="materialized",
                            user="mz_system",
                            port=6877,
                            sql=f"ALTER {what} OWNER TO materialize",
                        )

                    # Create two databases that some tests rely on
                    c.sql(
                        service="materialized",
                        user="materialize",
                        sql=dedent(
                            """
                            CREATE DATABASE test_database_1;
                            CREATE DATABASE test_database_2;
                            CREATE TABLE test_database_1.public.table1 (id int);
                            CREATE TABLE test_database_2.public.table2 (id int);
                            """
                        ),
                    )

                    c.run(
                        "dbt",
                        "pytest",
                        f"--splits={os.getenv('BUILDKITE_PARALLEL_JOB_COUNT', 1)}",
                        f"--group={int(os.getenv('BUILDKITE_PARALLEL_JOB', 0)) + 1}",
                        *test_args,
                        env_extra={
                            "DBT_HOST": "materialized",
                            "KAFKA_ADDR": "redpanda:9092",
                            "SCHEMA_REGISTRY_URL": "http://schema-registry:8081",
                            **test_case.dbt_env,
                        },
                    )

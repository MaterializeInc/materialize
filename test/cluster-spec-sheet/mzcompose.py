# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Deploy the current version on a real Staging Cloud, and run some basic
verifications, like ingesting data from Kafka and Redpanda Cloud using AWS
Privatelink. Runs only on main and release branches.
"""

import argparse
import os
import time
from typing import Any

import pg8000
import pg8000.native

from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.mz import Mz

REGION = "aws/us-east-1"
ENVIRONMENT = os.getenv("ENVIRONMENT", "staging")
USERNAME = os.getenv("MZ_USERNAME", "infra+nightly-canary@materialize.com")
APP_PASSWORD = os.getenv("MZ_APP_PASSWORD")

SERVICES = [
    Mz(
        region=REGION,
        environment=ENVIRONMENT,
        app_password=APP_PASSWORD or "",
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Deploy the current source to the cloud and run tests."""

    parser.add_argument(
        "--cleanup",
        default=False,
        action=argparse.BooleanOptionalAction,
        help="Destroy the region at the end of the workflow.",
    )
    parser.add_argument(
        "--version-check",
        default=False,
        action=argparse.BooleanOptionalAction,
        help="Perform a version check.",
    )

    parser.parse_args()

    with open(f"results_{int(time.time())}.csv", "w") as f:
        run_scenario(c, "tpcc_sf_10", results_file=f)
    # print("Running .td files ...")
    # td(c, text="> CREATE CLUSTER canary_sources SIZE '25cc'")
    #
    # def process(filename: str) -> None:
    #     td(c, filename, redpanda=redpanda)
    #
    # c.test_parts(files, process)


def run_scenario(c: Composition, scenario: str, results_file: Any) -> None:
    assert APP_PASSWORD is not None
    connection = pg8000.native.Connection(
        user=USERNAME, password=APP_PASSWORD, host=c.cloud_hostname(), port=6875
    )

    results_file.write("test_name,cluster_size,repetition,size_bytes,time_ms\n")

    def add_result(
        name: str, replica_size: str, repetition: int, size_bytes: int, time: float
    ) -> None:
        results_file.write(
            f"{scenario},{name},{replica_size},{repetition},{size_bytes},{int(time * 1_000)}\n"
        )

    def run_query(query: str, **params) -> list | None:
        print(f"> {query}")
        return connection.run(query, **params)

    def measure(
        name: str,
        replica_size: str,
        size_bytes: int,
        query: list[str],
        before: list[str] = [],
        repetitions: int = 1,
    ) -> None:
        print(f"Running {name} for {replica_size} with {repetitions} repetitions...")
        if before:
            for before_query in before:
                run_query(before_query)
        for repetition in range(repetitions):
            start_time = time.time()
            for query_part in query:
                run_query(query_part)
            end_time = time.time()
            add_result(
                name, replica_size, repetition, size_bytes, (end_time - start_time)
            )

    def size_of_dataflow(object: str) -> int:
        result = run_query(
            f"SELECT size FROM mz_introspection.mz_dataflow_arrangement_sizes WHERE name LIKE :name;",
            name=object,
        )
        return int(result[0][0])

    def size_of_storage(object: str) -> int:
        result = run_query(
            "select size_bytes from mz_objects o, mz_recent_storage_usage rsu where o.name LIKE :name and o.id = rsu.object_id;",
            name=object,
        )
        return int(result[0][0].split()[0])

    run_query("DROP TABLE IF EXISTS t;")
    run_query("CREATE TABLE t (a int);")
    run_query("INSERT INTO t VALUES (1);")
    # run_query("DROP SOURCE IF EXISTS lgtpch CASCADE;")
    # run_query("DROP CLUSTER IF EXISTS lg CASCADE")
    # run_query("CREATE CLUSTER lg SIZE '50cc'")
    # run_query("CREATE SOURCE lgtpch IN CLUSTER lg FROM LOAD GENERATOR TPCH (SCALE FACTOR 10, TICK INTERVAL 0.1) FOR ALL TABLES;")
    run_query("SELECT COUNT(*) > 0 FROM region;")

    lineitem_size = size_of_storage("lineitem")
    print(f"Lineitem size: {lineitem_size}")

    for replica_size in [
        "100cc",
        "200cc",
        "300cc",
        "400cc",
        "800cc",
        "1600cc",
        "3200cc",
    ]:  # , "6400cc", "128C"]:
        # Create a cluster with the specified size
        run_query("DROP CLUSTER IF EXISTS c CASCADE")
        run_query(f"CREATE CLUSTER c SIZE '{replica_size}'")
        run_query("SET cluster = 'c';")
        run_query("SELECT * FROM t;")

        # Create index
        measure(
            "create_index",
            replica_size,
            size_bytes=lineitem_size,
            before=[
                "DROP INDEX IF EXISTS lineitem_primary_idx CASCADE",
                "SELECT * FROM t;",
            ],
            query=[
                "CREATE DEFAULT INDEX ON lineitem;",
                "SELECT count(*) > 0 FROM lineitem;",
            ],
        )

        time.sleep(3)

        index_size = size_of_dataflow("%lineitem_primary_idx")
        print(f"Index size: {index_size}")

        # Peek against index
        measure(
            "peek_index_key_slow_path",
            replica_size,
            size_bytes=index_size,
            query=[
                "WITH data AS (SELECT * FROM lineitem WHERE l_orderkey = 123412341234 and l_linenumber = 123) SELECT * FROM data, t;",
            ],
            repetitions=10,
        )

        # Peek against index
        measure(
            "peek_index_key_fast_path",
            replica_size,
            size_bytes=index_size,
            query=[
                "SELECT * FROM lineitem WHERE l_orderkey = 123412341234 and l_linenumber = 123",
            ],
            repetitions=10,
        )

        # Peek against index
        measure(
            "peek_index_non_key_slow_path",
            replica_size,
            size_bytes=index_size,
            query=[
                "WITH data AS (SELECT * FROM lineitem WHERE l_tax = 123) SELECT * FROM data, t;",
            ],
        )

        # Peek against index
        measure(
            "peek_index_non_key_fast_path",
            replica_size,
            size_bytes=index_size,
            query=[
                "SELECT * FROM lineitem WHERE l_tax = 123;",
            ],
        )

        # Restart index
        measure(
            "index_restart",
            replica_size,
            size_bytes=index_size,
            before=[
                "SELECT count(*) > 0 FROM lineitem;",
            ],
            query=[
                "ALTER CLUSTER c SET (REPLICATION FACTOR 0);",
                "ALTER CLUSTER c SET (REPLICATION FACTOR 1);",
                "SET cluster = 'c';",
                "SELECT * FROM t;",
            ],
        )

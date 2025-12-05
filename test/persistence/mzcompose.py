# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Basic tests for Persistence layer.
"""

import os
import time
from argparse import Namespace
from textwrap import dedent

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Redpanda(),
    Mz(app_password=""),
    Materialized(),
    Testdrive(no_reset=True),
]

td_test = os.environ.pop("TD_TEST", "*")


def start_deps(
    c: Composition, args_or_parser: WorkflowArgumentParser | Namespace
) -> None:

    if isinstance(args_or_parser, Namespace):
        args = args_or_parser
    else:
        args_or_parser.add_argument(
            "--redpanda",
            action="store_true",
            help="run against Redpanda instead of the Confluent Platform",
        )
        args = args_or_parser.parse_args()

    if args.redpanda:
        dependencies = ["redpanda"]
    else:
        dependencies = ["zookeeper", "kafka", "schema-registry"]

    c.up(*dependencies)


def workflow_kafka_sources(
    c: Composition, args_or_parser: WorkflowArgumentParser | Namespace
) -> None:
    start_deps(c, args_or_parser)

    seed = round(time.time())

    c.up("materialized")

    c.run_testdrive_files(f"--seed={seed}", f"kafka-sources/*{td_test}*-before.td")

    c.kill("materialized")
    c.up("materialized")

    # And restart again, for extra stress
    c.kill("materialized")
    c.up("materialized")

    c.run_testdrive_files(f"--seed={seed}", f"kafka-sources/*{td_test}*-after.td")

    # Do one more restart, just in case and just confirm that Mz is able to come up
    c.kill("materialized")
    c.up("materialized")

    c.kill("materialized")
    c.rm("materialized", "testdrive", destroy_volumes=True)
    c.rm_volumes("mzdata")


def workflow_user_tables(
    c: Composition, args_or_parser: WorkflowArgumentParser | Namespace
) -> None:
    start_deps(c, args_or_parser)

    seed = round(time.time())

    c.up("materialized")

    c.run_testdrive_files(
        f"--seed={seed}",
        f"user-tables/table-persistence-before-{td_test}.td",
    )

    c.kill("materialized")
    c.up("materialized")

    c.kill("materialized")
    c.up("materialized")

    c.run_testdrive_files(
        f"--seed={seed}",
        f"user-tables/table-persistence-after-{td_test}.td",
    )

    c.kill("materialized")
    c.rm("materialized", "testdrive", destroy_volumes=True)
    c.rm_volumes("mzdata")


def workflow_failpoints(c: Composition, parser: WorkflowArgumentParser) -> None:
    start_deps(c, parser)

    for failpoint in [
        "fileblob_set_sync",
        "fileblob_delete_before",
        "fileblob_delete_after",
        "insert_timestamp_bindings_before",
        "insert_timestamp_bindings_after",
    ]:
        for action in ["return", "panic", "sleep(1000)"]:
            run_one_failpoint(c, failpoint, action)


def run_one_failpoint(c: Composition, failpoint: str, action: str) -> None:
    print(f">>> Running failpoint test for failpoint {failpoint} with action {action}")

    seed = round(time.time())

    c.up("materialized")

    c.run_testdrive_files(
        f"--seed={seed}",
        f"--var=failpoint={failpoint}",
        f"--var=action={action}",
        "failpoints/before.td",
    )

    time.sleep(2)
    # kill Mz if the failpoint has not killed it
    c.kill("materialized")
    c.up("materialized")

    c.run_testdrive_files(f"--seed={seed}", "failpoints/after.td")

    c.kill("materialized")
    c.rm("materialized", "testdrive", destroy_volumes=True)
    c.rm_volumes("mzdata")


def workflow_compaction(c: Composition) -> None:
    with c.override(
        Materialized(options=["--metrics-scraping-interval=1s"]),
    ):
        c.up("materialized")

        c.run_testdrive_files("compaction/compaction.td")

        c.kill("materialized")

    c.rm("materialized", "testdrive", destroy_volumes=True)

    c.rm_volumes("mzdata")


def workflow_inspect_shard(c: Composition) -> None:
    """Regression test for https://github.com/MaterializeInc/materialize/pull/21098"""
    c.up("materialized")
    c.sql(
        dedent(
            """
            CREATE TABLE foo (
                big0 string, big1 string, big2 string, big3 string, big4 string, big5 string,
                barTimestamp string,
                big6 string, big7 string
            );
            INSERT INTO foo VALUES (
                repeat('x', 1024), repeat('x', 1024), repeat('x', 1024), repeat('x', 1024), repeat('x', 1024), repeat('x', 1024),
                repeat('SENTINEL', 2048),
                repeat('x', 1024), repeat('x', 1024)
            );
            SELECT * FROM foo;
            """
        )
    )
    object_id = c.sql_query(
        "SELECT id from mz_objects where name = 'foo'", port=6877, user="mz_system"
    )[0][0]
    json_dict = c.sql_query(
        f"INSPECT SHARD '{object_id}'", port=6877, user="mz_system"
    )[0][0]
    parts = [
        part
        for batch in json_dict["batches"]
        for part_run in batch["part_runs"]
        for part in part_run[1]
    ]
    non_empty_part = next(part for part in parts if part["encoded_size_bytes"] > 0)
    cols = non_empty_part["stats"]["cols"]["ok"]

    # Leading columns are present in the stats
    assert "SENTINEL" in cols["bartimestamp"]["lower"]
    assert "SENTINEL" in cols["bartimestamp"]["upper"]

    for col_name in ["big0", "big1", "big2"]:
        assert cols[col_name]["lower"].endswith("xxx")
        assert cols[col_name]["upper"].endswith("xxy")

    # Trailing columns not represented because of stats size limits
    for col_name in ["big3", "big4", "big5"]:
        assert col_name not in cols


def workflow_default(c: Composition) -> None:
    def process(name: str) -> None:
        if name == "default":
            return

        if name in ["failpoints", "compaction"]:
            # Legacy tests, not currently operational
            return

        with c.test_case(name):
            c.workflow(name)

    c.test_parts(list(c.workflows.keys()), process)

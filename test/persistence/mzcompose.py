# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import time
from argparse import Namespace
from typing import Union

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Kafka,
    Materialized,
    Redpanda,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Redpanda(),
    Materialized(),
    Testdrive(no_reset=True),
]

td_test = os.environ.pop("TD_TEST", "*")


def start_deps(
    c: Composition, args_or_parser: Union[WorkflowArgumentParser, Namespace]
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

    c.start_and_wait_for_tcp(services=dependencies)


def workflow_kafka_sources(
    c: Composition, args_or_parser: Union[WorkflowArgumentParser, Namespace]
) -> None:
    start_deps(c, args_or_parser)

    seed = round(time.time())

    c.up("materialized")
    c.wait_for_materialized("materialized")

    c.run("testdrive", f"--seed={seed}", f"kafka-sources/*{td_test}*-before.td")

    c.kill("materialized")
    c.up("materialized")
    c.wait_for_materialized("materialized")

    # And restart again, for extra stress
    c.kill("materialized")
    c.up("materialized")
    c.wait_for_materialized("materialized")

    c.run("testdrive", f"--seed={seed}", f"kafka-sources/*{td_test}*-after.td")

    # Do one more restart, just in case and just confirm that Mz is able to come up
    c.kill("materialized")
    c.up("materialized")
    c.wait_for_materialized("materialized")

    c.kill("materialized")
    c.rm("materialized", "testdrive", destroy_volumes=True)
    c.rm_volumes("mzdata", "pgdata")


def workflow_user_tables(c: Composition) -> None:
    seed = round(time.time())

    c.up("materialized")
    c.wait_for_materialized()

    c.run(
        "testdrive",
        f"--seed={seed}",
        f"user-tables/table-persistence-before-{td_test}.td",
    )

    c.kill("materialized")
    c.up("materialized")

    c.run(
        "testdrive",
        f"--seed={seed}",
        f"user-tables/table-persistence-after-{td_test}.td",
    )

    c.kill("materialized")
    c.rm("materialized", "testdrive", destroy_volumes=True)
    c.rm_volumes("mzdata", "pgdata")


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
    c.wait_for_materialized()

    c.run(
        "testdrive",
        f"--seed={seed}",
        f"--var=failpoint={failpoint}",
        f"--var=action={action}",
        "failpoints/before.td",
    )

    time.sleep(2)
    # kill Mz if the failpoint has not killed it
    c.kill("materialized")
    c.up("materialized")
    c.wait_for_materialized()

    c.run("testdrive", f"--seed={seed}", "failpoints/after.td")

    c.kill("materialized")
    c.rm("materialized", "testdrive", destroy_volumes=True)
    c.rm_volumes("mzdata", "pgdata")


def workflow_compaction(c: Composition) -> None:
    with c.override(
        Materialized(
            options=f"--metrics-scraping-interval=1s",
        )
    ):
        c.up("materialized")
        c.wait_for_materialized()

        c.run("testdrive", "compaction/compaction.td")

        c.kill("materialized")

    c.rm("materialized", "testdrive", destroy_volumes=True)

    c.rm_volumes("mzdata", "pgdata")

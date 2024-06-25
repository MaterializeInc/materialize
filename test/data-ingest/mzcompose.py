# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
import time

from pg8000.exceptions import InterfaceError

from materialize.data_ingest.executor import (
    KafkaExecutor,
    KafkaRoundtripExecutor,
    MySqlExecutor,
)
from materialize.data_ingest.workload import *  # noqa: F401 F403
from materialize.data_ingest.workload import WORKLOADS, execute_workload
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda

SERVICES = [
    Postgres(),
    MySql(),
    Redpanda(
        auto_create_topics=False,
        ports=[30123],
    ),
    # Fixed port so that we keep the same port after restarting Mz in disruptions
    Materialized(
        ports=["16875:6875"],
        additional_system_parameter_defaults={"enable_table_keys": "true"},
    ),
    Clusterd(name="clusterd1", options=["--scratch-directory=/mzdata/source_data"]),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--seed", metavar="SEED", type=str, default=str(int(time.time()))
    )
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument(
        "--runtime", default=600, type=int, help="Runtime in seconds per workload"
    )
    parser.add_argument(
        "--workload",
        metavar="WORKLOAD",
        type=str,
        action="append",
        help="Workload(s) to run.",
    )

    args = parser.parse_args()

    workloads = (
        [globals()[workload] for workload in args.workload]
        if args.workload
        else WORKLOADS
    )

    print(f"--- Random seed is {args.seed}")

    services = (
        "materialized",
        "redpanda",
        "postgres",
        "mysql",
    )
    c.up(*services)

    conn = c.sql_connection()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            """CREATE CONNECTION IF NOT EXISTS kafka_conn
               FOR KAFKA BROKER 'redpanda:9092', SECURITY PROTOCOL PLAINTEXT"""
        )
        cur.execute(
            """CREATE CONNECTION IF NOT EXISTS csr_conn
               FOR CONFLUENT SCHEMA REGISTRY
               URL 'http://redpanda:8081'"""
        )
    conn.autocommit = False
    conn.close()

    executor_classes = [MySqlExecutor, KafkaRoundtripExecutor, KafkaExecutor]
    ports = {s: c.default_port(s) for s in services}

    try:
        for i, workload_class in enumerate(workloads):
            random.seed(args.seed)
            print(f"--- Testing workload {workload_class.__name__}")
            execute_workload(
                executor_classes,
                workload_class(c),
                i,
                ports,
                args.runtime,
                args.verbose,
            )
    except InterfaceError as e:
        if "network error" in str(e):
            # temporary error, invited to retry
            exit(75)

        raise

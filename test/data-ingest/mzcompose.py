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

from materialize.data_ingest.executor import KafkaExecutor
from materialize.data_ingest.workload import *  # noqa: F401 F403
from materialize.data_ingest.workload import Workload, execute_workload
from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Clusterd,
    Kafka,
    Materialized,
    Postgres,
    SchemaRegistry,
    Zookeeper,
)

SERVICES = [
    Postgres(),
    Zookeeper(),
    Kafka(
        auto_create_topics=False,
        port="30123:30123",
        allow_host_ports=True,
        environment_extra=[
            "KAFKA_ADVERTISED_LISTENERS=HOST://localhost:30123,PLAINTEXT://kafka:9092",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        ],
    ),
    SchemaRegistry(),
    Materialized(),
    Clusterd(name="clusterd1", options=["--scratch-directory=/mzdata/source_data"]),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--seed", metavar="SEED", type=str, default=str(int(time.time()))
    )
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument(
        "--runtime", default=60, type=int, help="Runtime in seconds per workload"
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
        else Workload.__subclasses__()
    )

    print(f"--- Random seed is {args.seed}")

    services = ("materialized", "zookeeper", "kafka", "schema-registry", "postgres")
    c.up(*services)

    conn = c.sql_connection()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            """CREATE CONNECTION IF NOT EXISTS kafka_conn
               FOR KAFKA BROKER 'kafka:9092'"""
        )
        cur.execute(
            """CREATE CONNECTION IF NOT EXISTS csr_conn
               FOR CONFLUENT SCHEMA REGISTRY
               URL 'http://schema-registry:8081'"""
        )
    conn.autocommit = False

    executor_classes = [KafkaExecutor]
    ports = {s: c.default_port(s) for s in services}

    for i, workload_class in enumerate(workloads):
        random.seed(args.seed)
        print(f"--- Testing workload {workload_class.__name__}")
        execute_workload(
            executor_classes,
            workload_class(),
            conn,
            i,
            ports,
            args.runtime,
            args.verbose,
        )

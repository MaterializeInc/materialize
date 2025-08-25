# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Test that ingests large amounts of data from Kafka/Postgres/MySQL/SQL Server
and verifies that Materialize can handle it correctly by comparing the results.
"""

import random
import time

from materialize import buildkite
from materialize.data_ingest.executor import (
    KafkaExecutor,
    MySqlExecutor,
    SqlServerExecutor,
)
from materialize.data_ingest.workload import *  # noqa: F401 F403
from materialize.data_ingest.workload import WORKLOADS, execute_workload
from materialize.mzcompose import get_default_system_parameters
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.azurite import Azurite
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.postgres import (
    CockroachOrPostgresMetadata,
    Postgres,
)
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.sql_server import (
    SqlServer,
    setup_sql_server_testing,
)
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper

SERVICES = [
    Postgres(),
    MySql(),
    SqlServer(),
    Zookeeper(),
    Kafka(
        auto_create_topics=False,
        ports=["30123:30123"],
        allow_host_ports=True,
        environment_extra=[
            "KAFKA_ADVERTISED_LISTENERS=HOST://localhost:30123,PLAINTEXT://kafka:9092",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        ],
    ),
    SchemaRegistry(),
    CockroachOrPostgresMetadata(),
    Minio(setup_materialize=True),
    Azurite(),
    Testdrive(),
    # Overridden below
    Materialized(),
    Materialized(name="materialized2"),
    Clusterd(name="clusterd1", scratch_directory="/mzdata/source_data"),
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
    parser.add_argument(
        "--azurite", action="store_true", help="Use Azurite as blob store instead of S3"
    )
    parser.add_argument("--replicas", type=int, default=2, help="use multiple replicas")

    args = parser.parse_args()

    workloads = buildkite.shard_list(
        sorted(
            (
                [globals()[workload] for workload in args.workload]
                if args.workload
                else list(WORKLOADS)
            ),
            key=lambda w: w.__name__,
        ),
        lambda w: w.__name__,
    )
    print(
        f"Workloads in shard with index {buildkite.get_parallelism_index()}: {workloads}"
    )

    print(f"--- Random seed is {args.seed}")

    services = (
        "materialized",
        "zookeeper",
        "kafka",
        "schema-registry",
        "postgres",
        "mysql",
        "sql-server",
    )

    # TODO: Reenable when database-issues#8657 is fixed
    # executor_classes = [MySqlExecutor, SqlServerExecutor, KafkaRoundtripExecutor, KafkaExecutor]
    executor_classes = [MySqlExecutor, SqlServerExecutor, KafkaExecutor]

    with c.override(
        # Fixed port so that we keep the same port after restarting Mz in disruptions
        Materialized(
            ports=["16875:6875", "16877:6877"],
            external_blob_store=True,
            blob_store_is_azure=args.azurite,
            external_metadata_store=True,
            system_parameter_defaults=get_default_system_parameters(zero_downtime=True),
            additional_system_parameter_defaults={"unsafe_enable_table_keys": "true"},
            sanity_restart=False,
            support_external_clusterd=True,
        ),
        Materialized(
            name="materialized2",
            ports=["26875:6875", "26877:6877"],
            external_blob_store=True,
            blob_store_is_azure=args.azurite,
            external_metadata_store=True,
            system_parameter_defaults=get_default_system_parameters(zero_downtime=True),
            additional_system_parameter_defaults={"unsafe_enable_table_keys": "true"},
            sanity_restart=False,
            support_external_clusterd=True,
        ),
    ):
        c.up(*services)
        setup_sql_server_testing(c)

        if args.replicas > 1:
            c.sql(
                "ALTER SYSTEM SET max_replicas_per_cluster = 32",
                user="mz_system",
                port=6877,
            )
            c.sql("DROP CLUSTER quickstart CASCADE", user="mz_system", port=6877)
            replica_names = [f"r{replica_id}" for replica_id in range(0, args.replicas)]
            replica_string = ",".join(
                f"{replica_name} (SIZE 'scale=1,workers=4')"
                for replica_name in replica_names
            )
            c.sql(
                f"CREATE CLUSTER quickstart REPLICAS ({replica_string})",
                user="mz_system",
                port=6877,
            )
            c.sql(
                "GRANT ALL PRIVILEGES ON CLUSTER quickstart TO materialize",
                user="mz_system",
                port=6877,
            )

        c.sql(
            "CREATE CLUSTER singlereplica SIZE = 'scale=1,workers=4', REPLICATION FACTOR = 1",
        )

        conn = c.sql_connection()
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                """CREATE CONNECTION IF NOT EXISTS kafka_conn
                   FOR KAFKA BROKER 'kafka:9092', SECURITY PROTOCOL PLAINTEXT"""
            )
            cur.execute(
                """CREATE CONNECTION IF NOT EXISTS csr_conn
                   FOR CONFLUENT SCHEMA REGISTRY
                   URL 'http://schema-registry:8081'"""
            )
        conn.autocommit = False
        conn.close()

        ports = {s: c.default_port(s) for s in services}
        ports["materialized2"] = 26875
        mz_service = "materialized"
        deploy_generation = 0

        for i, workload_class in enumerate(workloads):
            random.seed(args.seed)
            print(f"--- Testing workload {workload_class.__name__}")
            workload = workload_class(args.azurite, c, mz_service, deploy_generation)
            execute_workload(
                executor_classes,
                workload,
                i,
                ports,
                args.runtime,
                args.verbose,
            )
            mz_service = workload.mz_service
            deploy_generation = workload.deploy_generation

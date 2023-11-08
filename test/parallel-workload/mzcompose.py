# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.service import Service
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Mc, Minio
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.parallel_workload.parallel_workload import parse_common_args, run
from materialize.parallel_workload.settings import Complexity, Scenario

SERVICES = [
    Cockroach(setup_materialize=True),
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
    Minio(setup_materialize=True),
    Mc(),
    Materialized(
        external_cockroach=True,
        restart="on-failure",
        external_minio=True,
        ports=["6975:6875", "6976:6876", "6977:6877"],
    ),
    Service("sqlsmith", {"mzbuild": "sqlsmith"}),
    Service(
        name="persistcli",
        config={"mzbuild": "jobs"},
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parse_common_args(parser)
    args = parser.parse_args()

    print(f"--- Random seed is {args.seed}")
    service_names = [
        "cockroach",
        "postgres",
        "zookeeper",
        "kafka",
        "schema-registry",
        "minio",
        "materialized",
    ]
    c.up(*service_names)
    c.up("mc", persistent=True)
    c.exec(
        "mc",
        "mc",
        "alias",
        "set",
        "persist",
        "http://minio:9000/",
        "minioadmin",
        "minioadmin",
    )
    c.exec("mc", "mc", "version", "enable", "persist/persist")

    ports = {s: c.default_port(s) for s in service_names}
    ports["http"] = c.port("materialized", 6876)
    ports["mz_system"] = c.port("materialized", 6877)
    # try:
    run(
        "localhost",
        ports,
        args.seed,
        args.runtime,
        Complexity(args.complexity),
        Scenario(args.scenario),
        args.threads,
        args.naughty_identifiers,
        args.fast_startup,
        c,
    )
    # TODO: Only ignore errors that will be handled by parallel-workload, not others
    # except Exception:
    #     print("--- Execution of parallel-workload failed")
    #     print_exc()
    #     # Don't fail the entire run. We ran into a crash,
    #     # ci-logged-errors-detect will handle this if it's an unknown failure.
    #     return

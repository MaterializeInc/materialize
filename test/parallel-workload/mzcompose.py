# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
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
    Materialized(
        external_cockroach=True,
        restart="on-failure",
        ports=["6975:6875", "6976:6876", "6977:6877"],
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
        "materialized",
    ]
    c.up(*service_names)

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
        c,
    )
    # TODO: Only ignore errors that will be handled by parallel-workload, not others
    # except Exception:
    #     print("--- Execution of parallel-workload failed")
    #     print_exc()
    #     # Don't fail the entire run. We ran into a crash,
    #     # ci-logged-errors-detect will handle this if it's an unknown failure.
    #     return
    # Restart mz
    c.kill("materialized")
    c.up("materialized")
    # Verify that things haven't blown up
    c.sql("SELECT 1")

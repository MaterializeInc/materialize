# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
from enum import Enum

from materialize import buildkite
from materialize.checks.all_checks import *  # noqa: F401 F403
from materialize.checks.checks import Check
from materialize.checks.executors import MzcomposeExecutor, MzcomposeExecutorParallel
from materialize.checks.scenarios import *  # noqa: F401 F403
from materialize.checks.scenarios import Scenario
from materialize.checks.scenarios_backup_restore import *  # noqa: F401 F403
from materialize.checks.scenarios_migration import *  # noqa: F401 F403
from materialize.checks.scenarios_platform_v2 import *  # noqa: F401 F403
from materialize.checks.scenarios_upgrade import *  # noqa: F401 F403
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.debezium import Debezium
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Mc, Minio
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.persistcli import Persistcli
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.ssh_bastion_host import SshBastionHost
from materialize.mzcompose.services.test_certs import TestCerts
from materialize.mzcompose.services.testdrive import Testdrive as TestdriveService
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.util import all_subclasses

TESTDRIVE_DEFAULT_TIMEOUT = os.environ.get("PLATFORM_CHECKS_TD_TIMEOUT", "300s")

SERVICES = [
    TestCerts(),
    Cockroach(
        setup_materialize=True,
        # Workaround for #19810
        restart="on-failure:5",
    ),
    Minio(setup_materialize=True),
    Mc(),
    Postgres(),
    MySql(),
    Zookeeper(),
    Kafka(
        auto_create_topics=True,
        depends_on_extra=["test-certs"],
        advertised_listeners=[
            # Using lowercase listener names here bypasses some too-helpful
            # checks in the Docker entrypoint that (incorrectly) attempt to
            # assess the validity of the authentication configuration.
            "plaintext://kafka:9092",
            "ssl://kafka:9093",
            "mssl://kafka:9094",
            "sasl_plaintext://kafka:9095",
            "sasl_ssl://kafka:9096",
            "sasl_mssl://kafka:9097",
        ],
        environment_extra=[
            "ZOOKEEPER_SASL_ENABLED=FALSE",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,ssl:SSL,mssl:SSL,sasl_plaintext:SASL_PLAINTEXT,sasl_ssl:SASL_SSL,sasl_mssl:SASL_SSL",
            "KAFKA_INTER_BROKER_LISTENER_NAME=plaintext",
            "KAFKA_SASL_ENABLED_MECHANISMS=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512",
            "KAFKA_SSL_KEY_PASSWORD=mzmzmz",
            "KAFKA_SSL_KEYSTORE_LOCATION=/etc/kafka/secrets/kafka.keystore.jks",
            "KAFKA_SSL_KEYSTORE_PASSWORD=mzmzmz",
            "KAFKA_SSL_TRUSTSTORE_LOCATION=/etc/kafka/secrets/kafka.truststore.jks",
            "KAFKA_SSL_TRUSTSTORE_PASSWORD=mzmzmz",
            "KAFKA_OPTS=-Djava.security.auth.login.config=/etc/kafka/jaas.config",
            "KAFKA_LISTENER_NAME_MSSL_SSL_CLIENT_AUTH=required",
            "KAFKA_LISTENER_NAME_SASL__MSSL_SSL_CLIENT_AUTH=required",
            "KAFKA_AUTHORIZER_CLASS_NAME=kafka.security.authorizer.AclAuthorizer",
            "KAFKA_SUPER_USERS=User:materialize;User:CN=materialized;User:ANONYMOUS",
        ],
        volumes=[
            "secrets:/etc/kafka/secrets",
            "./kafka.jaas.config:/etc/kafka/jaas.config",
        ],
    ),
    SchemaRegistry(),
    Debezium(),
    Clusterd(
        name="clusterd_compute_1"
    ),  # Started by some Scenarios, defined here only for the teardown
    Materialized(
        external_cockroach=True,
        external_minio=True,
        sanity_restart=False,
        volumes_extra=["secrets:/share/secrets"],
        catalog_store="persist",
    ),
    TestdriveService(
        default_timeout=TESTDRIVE_DEFAULT_TIMEOUT,
        materialize_params={"statement_timeout": f"'{TESTDRIVE_DEFAULT_TIMEOUT}'"},
        no_reset=True,
        seed=1,
        entrypoint_extra=[
            "--var=replicas=1",
            f"--var=default-replica-size={Materialized.Size.DEFAULT_SIZE}-{Materialized.Size.DEFAULT_SIZE}",
            f"--var=default-storage-size={Materialized.Size.DEFAULT_SIZE}-1",
        ],
        volumes_extra=["secrets:/share/secrets"],
    ),
    Persistcli(),
    SshBastionHost(),
]


class ExecutionMode(Enum):
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    ONEATATIME = "oneatatime"

    def __str__(self) -> str:
        return self.value


def setup(c: Composition) -> None:
    c.up("testdrive", persistent=True)
    c.up("cockroach")

    c.up(
        "test-certs",
        "zookeeper",
        "kafka",
        "schema-registry",
        "postgres",
        "mysql",
        "debezium",
        "minio",
        "ssh-bastion-host",
    )

    c.enable_minio_versioning()

    # Add `materialize` SCRAM user to Kafka.
    c.exec(
        "kafka",
        "kafka-configs",
        "--bootstrap-server=localhost:9092",
        "--alter",
        "--add-config=SCRAM-SHA-256=[password=sekurity],SCRAM-SHA-512=[password=sekurity]",
        "--entity-type=users",
        "--entity-name=materialize",
    )


def teardown(c: Composition) -> None:
    c.rm(*[s.name for s in SERVICES], stop=True, destroy_volumes=True)
    c.rm_volumes("mzdata", "tmp", force=True)


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    # c.silent = True
    parser.add_argument(
        "--scenario", metavar="SCENARIO", type=str, help="Scenario to run."
    )

    parser.add_argument(
        "--check", metavar="CHECK", type=str, action="append", help="Check(s) to run."
    )

    parser.add_argument(
        "--execution-mode",
        type=ExecutionMode,
        choices=list(ExecutionMode),
        default=ExecutionMode.SEQUENTIAL,
    )

    parser.add_argument(
        "--seed",
        metavar="SEED",
        type=str,
        help="Seed for shuffling checks in sequential run.",
    )

    args = parser.parse_args()

    if args.scenario:
        assert args.scenario in globals(), f"scenario {args.scenario} does not exist"
        scenarios = [globals()[args.scenario]]
    else:
        scenarios = all_subclasses(Scenario)

    if args.check:
        all_checks = {check.__name__: check for check in all_subclasses(Check)}
        for check in args.check:
            assert check in all_checks, f"check {check} does not exist"
        checks = [all_checks[check] for check in args.check]
    else:
        checks = list(all_subclasses(Check))

    shard = buildkite.get_parallelism_index()
    shard_count = buildkite.get_parallelism_count()

    if shard_count > 1:
        checks.sort(key=lambda c: c.__name__)
        checks = checks[shard::shard_count]
        print(
            f"Selected checks in job with index {shard}: {[c.__name__ for c in checks]}"
        )

    executor = MzcomposeExecutor(composition=c)
    for scenario_class in scenarios:
        assert issubclass(
            scenario_class, Scenario
        ), f"{scenario_class} is not a Scenario. Maybe you meant to specify a Check via --check ?"

        print(f"Testing scenario {scenario_class}...")

        executor_class = (
            MzcomposeExecutorParallel
            if args.execution_mode is ExecutionMode.PARALLEL
            else MzcomposeExecutor
        )
        executor = executor_class(composition=c)

        if args.execution_mode in [ExecutionMode.SEQUENTIAL, ExecutionMode.PARALLEL]:
            setup(c)
            scenario = scenario_class(checks=checks, executor=executor, seed=args.seed)
            scenario.run()
            teardown(c)
        elif args.execution_mode is ExecutionMode.ONEATATIME:
            for check in checks:
                print(f"Running individual check {check}, scenario {scenario_class}")
                setup(c)
                scenario = scenario_class(checks=[check], executor=executor)
                scenario.run()
                teardown(c)
        else:
            assert False

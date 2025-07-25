# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Write a single set of .td fragments for a particular feature or functionality
and then execute them in upgrade, 0dt-upgrade, restart, recovery and failure
contexts.
"""

import os
from enum import Enum

from materialize import buildkite
from materialize.checks.all_checks import *  # noqa: F401 F403
from materialize.checks.checks import Check
from materialize.checks.executors import MzcomposeExecutor, MzcomposeExecutorParallel
from materialize.checks.features import Features
from materialize.checks.scenarios import *  # noqa: F401 F403
from materialize.checks.scenarios import Scenario, SystemVarChange
from materialize.checks.scenarios_backup_restore import *  # noqa: F401 F403
from materialize.checks.scenarios_upgrade import *  # noqa: F401 F403
from materialize.checks.scenarios_zero_downtime import *  # noqa: F401 F403
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.azurite import Azurite
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.debezium import Debezium
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Mc, Minio
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.persistcli import Persistcli
from materialize.mzcompose.services.postgres import (
    CockroachOrPostgresMetadata,
    Postgres,
)
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.mzcompose.services.ssh_bastion_host import SshBastionHost
from materialize.mzcompose.services.test_certs import TestCerts
from materialize.mzcompose.services.testdrive import Testdrive as TestdriveService
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.util import all_subclasses

TESTDRIVE_DEFAULT_TIMEOUT = os.environ.get("PLATFORM_CHECKS_TD_TIMEOUT", "300s")


def create_mzs(
    azurite: bool,
    default_replication_factor: int,
    additional_system_parameter_defaults: dict[str, str] | None = None,
) -> list[TestdriveService | Materialized]:
    return [
        Materialized(
            name=mz_name,
            external_metadata_store=True,
            external_blob_store=True,
            blob_store_is_azure=azurite,
            sanity_restart=False,
            volumes_extra=["secrets:/share/secrets"],
            additional_system_parameter_defaults=additional_system_parameter_defaults,
            default_replication_factor=default_replication_factor,
        )
        for mz_name in ["materialized", "mz_1", "mz_2", "mz_3", "mz_4", "mz_5"]
    ] + [
        TestdriveService(
            default_timeout=TESTDRIVE_DEFAULT_TIMEOUT,
            materialize_params={"statement_timeout": f"'{TESTDRIVE_DEFAULT_TIMEOUT}'"},
            external_blob_store=True,
            blob_store_is_azure=azurite,
            no_reset=True,
            seed=1,
            entrypoint_extra=[
                "--var=replicas=1",
                f"--var=default-replica-size={Materialized.Size.DEFAULT_SIZE}-{Materialized.Size.DEFAULT_SIZE}",
                f"--var=default-storage-size={Materialized.Size.DEFAULT_SIZE}-1",
            ],
            volumes_extra=["secrets:/share/secrets"],
        )
    ]


SERVICES = [
    TestCerts(),
    CockroachOrPostgresMetadata(
        # Workaround for database-issues#5899
        restart="on-failure:5",
    ),
    Minio(setup_materialize=True, additional_directories=["copytos3"]),
    Azurite(),
    Mc(),
    Postgres(),
    MySql(),
    SqlServer(),
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
    *create_mzs(azurite=False, default_replication_factor=1),
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
    c.up(
        "test-certs",
        "zookeeper",
        "kafka",
        "schema-registry",
        "postgres",
        "mysql",
        "debezium",
        "ssh-bastion-host",
        {"name": "testdrive", "persistent": True},
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

    parser.add_argument(
        "--system-param",
        type=str,
        action="append",
        nargs="*",
        help="System parameters to set in Materialize, i.e. what you would set with `ALTER SYSTEM SET`",
    )

    parser.add_argument(
        "--features",
        nargs="*",
        help="A list of features (e.g. azurite, sql_server), to enable.",
    )

    parser.add_argument(
        "--default-replication-factor",
        type=int,
        default=2,
        help="Default replication factor for clusters",
    )

    args = parser.parse_args()
    features = Features(args.features)

    if args.scenario:
        assert args.scenario in globals(), f"scenario {args.scenario} does not exist"
        scenarios = [globals()[args.scenario]]
    else:
        base_scenarios = {SystemVarChange}
        scenarios = all_subclasses(Scenario) - base_scenarios

    if args.check:
        all_checks = {check.__name__: check for check in all_subclasses(Check)}
        for check in args.check:
            assert check in all_checks, f"check {check} does not exist"
        checks = [all_checks[check] for check in args.check]
    else:
        checks = list(all_subclasses(Check))

    if features.sql_server_enabled():
        c.up("sql-server")

    checks.sort(key=lambda ch: ch.__name__)
    checks = buildkite.shard_list(checks, lambda ch: ch.__name__)
    if buildkite.get_parallelism_index() != 0 or buildkite.get_parallelism_count() != 1:
        print(
            f"Checks in shard with index {buildkite.get_parallelism_index()}: {[c.__name__ for c in checks]}"
        )

    additional_system_parameter_defaults = {}
    for val in args.system_param or []:
        x = val[0].split("=", maxsplit=1)
        assert len(x) == 2, f"--system-param '{val}' should be the format <key>=<val>"
        additional_system_parameter_defaults[x[0]] = x[1]

    with c.override(
        *create_mzs(
            features.azurite_enabled(),
            args.default_replication_factor,
            additional_system_parameter_defaults,
        )
    ):
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

            execution_mode = args.execution_mode

            if execution_mode in [ExecutionMode.SEQUENTIAL, ExecutionMode.PARALLEL]:
                setup(c)
                scenario = scenario_class(
                    checks=checks,
                    executor=executor,
                    features=features,
                    seed=args.seed,
                )
                scenario.run()
            elif execution_mode is ExecutionMode.ONEATATIME:
                for check in checks:
                    print(
                        f"Running individual check {check}, scenario {scenario_class}"
                    )
                    c.override_current_testcase_name(
                        f"Check '{check}' with scenario '{scenario_class}'"
                    )
                    setup(c)
                    scenario = scenario_class(
                        checks=[check],
                        executor=executor,
                        features=features,
                        seed=args.seed,
                    )
                    scenario.run()
            else:
                raise RuntimeError(f"Unsupported execution mode: {execution_mode}")

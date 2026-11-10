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

import argparse
import os
from contextlib import ExitStack
from enum import Enum
from unittest import SkipTest

from materialize import buildkite
from materialize.checks.actions import Action
from materialize.checks.all_checks import *  # noqa: F403
from materialize.checks.checks import Check
from materialize.checks.executors import MzcomposeExecutor, MzcomposeExecutorParallel
from materialize.checks.features import Features
from materialize.checks.mzcompose_actions import StartClusterdCompute, StartMz
from materialize.checks.scenarios import *  # noqa: F403
from materialize.checks.scenarios import Scenario, SystemVarChange
from materialize.checks.scenarios_backup_restore import *  # noqa: F403
from materialize.checks.scenarios_upgrade import *  # noqa: F403
from materialize.checks.scenarios_zero_downtime import *  # noqa: F403
from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.azurite import Azurite
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.debezium import Debezium
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Mc, Minio
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.persistcli import Persistcli
from materialize.mzcompose.services.polaris import Polaris, PolarisBootstrap
from materialize.mzcompose.services.postgres import Postgres, PostgresMetadata
from materialize.mzcompose.services.pubsub import Pubsub
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.mzcompose.services.ssh_bastion_host import SshBastionHost
from materialize.mzcompose.services.test_certs import TestCerts
from materialize.mzcompose.services.testdrive import Testdrive as TestdriveService
from materialize.ui import UIError
from materialize.util import all_subclasses

TESTDRIVE_DEFAULT_TIMEOUT = os.environ.get("PLATFORM_CHECKS_TD_TIMEOUT", "300s")


def create_mzs(
    azurite: bool,
    default_replication_factor: int,
    additional_system_parameter_defaults: dict[str, str] | None = None,
    external_metadata_store: bool = True,
    external_blob_store: bool = True,
) -> list[TestdriveService | Materialized]:
    return [
        Materialized(
            name=mz_name,
            # TODO: Switch to default (CockroachOrPostgresMetadata) when
            # https://linear.app/materializeinc/issue/SS-284 is solved
            metadata_store="postgres-metadata",
            external_metadata_store=external_metadata_store,
            external_blob_store=external_blob_store,
            blob_store_is_azure=azurite,
            sanity_restart=False,
            volumes_extra=["secrets:/share/secrets"],
            additional_system_parameter_defaults=additional_system_parameter_defaults,
            default_replication_factor=default_replication_factor,
            support_external_clusterd=True,
        )
        for mz_name in ["materialized", "mz_1", "mz_2", "mz_3", "mz_4", "mz_5"]
    ] + [
        TestdriveService(
            default_timeout=TESTDRIVE_DEFAULT_TIMEOUT,
            materialize_params={"statement_timeout": f"'{TESTDRIVE_DEFAULT_TIMEOUT}'"},
            external_blob_store=external_blob_store,
            blob_store_is_azure=azurite,
            no_reset=True,
            seed=1,
            entrypoint_extra=[
                "--var=replicas=1",
                f"--var=default-replica-size=scale={Materialized.Size.DEFAULT_SIZE},workers={Materialized.Size.DEFAULT_SIZE}",
                f"--var=default-storage-size={Materialized.Size.DEFAULT_SIZE}-1",
            ],
            volumes_extra=["secrets:/share/secrets"],
        )
    ]


SERVICES = [
    Pubsub(),
    TestCerts(),
    PostgresMetadata(
        # Workaround for database-issues#5899
        restart="on-failure:5",
    ),
    Minio(setup_materialize=True, additional_directories=["copytos3"]),
    Azurite(),
    Mc(),
    Postgres(volumes=["secrets:/certs:ro"]),
    MySql(),
    SqlServer(),
    Polaris(),
    PolarisBootstrap(),
    Kafka(
        # The Self-Managed upgrade scenarios exercise historical Mz versions
        # whose embedded librdkafka cannot SCRAM-auth against Kafka 4.x. Pin
        # platform-checks to the previous Confluent Platform major (Apache
        # Kafka 3.9, KRaft) until those Mz versions age out of the support
        # window.
        tag="7.9.4",
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
        # Move the KRaft controller port off 9093, which is used for SSL above.
        controller_port=29093,
        environment_extra=[
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,ssl:SSL,mssl:SSL,sasl_plaintext:SASL_PLAINTEXT,sasl_ssl:SASL_SSL,sasl_mssl:SASL_SSL",
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
            "KAFKA_AUTHORIZER_CLASS_NAME=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
            "KAFKA_SUPER_USERS=User:materialize;User:CN=materialized;User:ANONYMOUS",
            # Bootstrap SCRAM users at storage-format time. See
            # `misc/mzcompose/kafka/ensure-with-scram.sh` for why the runtime
            # `kafka-configs --add-config=SCRAM-*` path is unreliable on
            # Kafka 4.x KRaft.
            "KAFKA_INIT_SCRAM_USERS="
            "SCRAM-SHA-256=[name=materialize,password=sekurity];"
            "SCRAM-SHA-512=[name=materialize,password=sekurity]",
        ],
        volumes=[
            "secrets:/etc/kafka/secrets",
            "./kafka.jaas.config:/etc/kafka/jaas.config",
            "../../misc/mzcompose/kafka/ensure-with-scram.sh:/etc/confluent/docker/ensure",
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


def setup(c: Composition, external_blob_store: bool) -> None:
    dependencies = [
        "test-certs",
        "kafka",
        "schema-registry",
        "postgres",
        "mysql",
        "sql-server",
        "debezium",
        "ssh-bastion-host",
        Service("testdrive", idle=True),
    ]

    if external_blob_store:
        dependencies.extend(
            [
                "minio",
                Service("mc", idle=True),
            ]
        )

    c.up(*dependencies)

    if external_blob_store:
        c.enable_minio_versioning()

    # The `materialize` SCRAM user is bootstrapped by Kafka's storage-format
    # step via `KAFKA_INIT_SCRAM_USERS` (see `ensure-with-scram.sh`), so no
    # runtime `kafka-configs` call is needed here.


def check_native_upgrade_boundary(actions: list[Action]) -> None:
    # These action tags select published monolithic images. None selects the
    # local native build, including builds changed by BumpVersion. This is image
    # provenance, not a SQL-version compatibility threshold.
    starts = [
        action
        for action in actions
        if isinstance(action, (StartMz, StartClusterdCompute))
    ]
    released = sorted({str(action.tag) for action in starts if action.tag is not None})
    if released and any(action.tag is None for action in starts):
        raise SkipTest(
            "Unsupported: pre-native published images "
            + ", ".join(released)
            + " and local native controllerd/adapterd share an upgrade path. "
            "No catalog conversion or reset workaround."
        )


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
        help="A list of features (e.g. azurite), to enable.",
    )

    parser.add_argument(
        "--default-replication-factor",
        type=int,
        default=2,
        help="Default replication factor for clusters",
    )

    parser.add_argument(
        "--external-metadata-store", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument(
        "--external-blob-store", action=argparse.BooleanOptionalAction, default=True
    )

    args = parser.parse_args()
    features = Features(args.features)

    if args.scenario:
        assert args.scenario in globals(), f"scenario {args.scenario} does not exist"
        scenarios = [globals()[args.scenario]]
    else:
        base_scenarios = {SystemVarChange}
        scenarios = sorted(
            all_subclasses(Scenario) - base_scenarios, key=lambda s: s.__name__
        )

    if args.check:
        all_checks = {check.__name__: check for check in all_subclasses(Check)}
        for check in args.check:
            assert check in all_checks, f"check {check} does not exist"
        checks = [all_checks[check] for check in args.check]
    else:
        checks = list(all_subclasses(Check))

    checks.sort(key=lambda ch: ch.__name__)
    checks = buildkite.shard_list(checks, lambda ch: ch.__name__)
    if buildkite.get_parallelism_index() != 0 or buildkite.get_parallelism_count() != 1:
        print(
            f"Checks in shard with index {buildkite.get_parallelism_index()}: {[c.__name__ for c in checks]}"
        )

    additional_system_parameter_defaults = {}
    for val in args.system_param or []:
        for param in val:
            x = param.split("=", maxsplit=1)
            assert (
                len(x) == 2
            ), f"--system-param '{param}' should be the format <key>=<val>"
            additional_system_parameter_defaults[x[0]] = x[1]

    executor_class = (
        MzcomposeExecutorParallel
        if args.execution_mode is ExecutionMode.PARALLEL
        else MzcomposeExecutor
    )

    with ExitStack() as overrides:
        ran_scenario = False

        def reset_environment() -> None:
            # Defer workflow-level acquisition and service resets until the plan
            # is accepted. The local CLI may acquire initial images before dispatch.
            nonlocal ran_scenario
            if ran_scenario:
                c.down(destroy_volumes=True, sanity_restart_mz=False)
            else:
                overrides.enter_context(
                    c.override(
                        *create_mzs(
                            features.azurite_enabled(),
                            args.default_replication_factor,
                            additional_system_parameter_defaults,
                            external_blob_store=args.external_blob_store,
                            external_metadata_store=args.external_metadata_store,
                        )
                    )
                )
            ran_scenario = True
            setup(c, args.external_blob_store)

        for scenario_class in scenarios:
            assert issubclass(
                scenario_class, Scenario
            ), f"{scenario_class} is not a Scenario. Maybe you meant to specify a Check via --check ?"

            batches = (
                [[check] for check in checks]
                if args.execution_mode is ExecutionMode.ONEATATIME
                else [checks]
            )
            for selected_checks in batches:
                case_name = scenario_class.__name__
                if args.execution_mode is ExecutionMode.ONEATATIME:
                    case_name += f"/{selected_checks[0].__name__}"
                with c.test_case(case_name):
                    scenario = scenario_class(
                        checks=selected_checks,
                        executor=executor_class(composition=c),
                        features=features,
                        seed=args.seed,
                        additional_system_parameter_defaults=additional_system_parameter_defaults,
                        default_replication_factor=args.default_replication_factor,
                    )
                    actions = scenario.actions()
                    check_native_upgrade_boundary(actions)
                    reset_environment()
                    scenario.run(actions)
                errors = c.test_results[case_name].errors
                if errors:
                    for error in errors:
                        location = error.location_as_file_name()
                        if location and error.line_number is not None:
                            location += f":{error.line_number}"
                        error.test_case_name_override = (
                            f"{case_name}/{location}" if location else case_name
                        )
                    # A failed parallel check can leave other checks running, and
                    # a failed version-bump scenario can leave source mutations.
                    # Preserve fail-fast behavior instead of resetting underneath
                    # that work or running another case against changed sources.
                    raise UIError(f"Platform scenario {case_name} failed")

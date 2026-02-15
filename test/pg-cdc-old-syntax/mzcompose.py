# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Native Postgres source tests, functional.
"""

from random import Random

import pg8000

from materialize.mz_0dt_upgrader import (
    Materialized0dtUpgrader,
    generate_materialized_upgrade_args,
)
from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.metadata_store import (
    METADATA_STORE,
    CockroachOrPostgresMetadata,
)
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.test_certs import TestCerts
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.toxiproxy import Toxiproxy
from materialize.pg_cdc import (
    kill_pg_and_mz,
    workflow_replication_disabled,  # noqa: F401
    workflow_wal_level,  # noqa: F401
)
from materialize.pg_cdc import (
    workflow_cdc as _workflow_cdc,
)
from materialize.pg_cdc import (
    workflow_replication_slots as _workflow_replication_slots,
)
from materialize.pg_cdc import (
    workflow_silent_connection_drop as _workflow_silent_connection_drop,
)
from materialize.postgres_util import (
    PostgresRecvlogical,
    create_postgres,
    get_targeted_pg_version,
    get_testdrive_ssl_args,
)
from materialize.source_table_migration import (
    verify_sources_after_source_table_migration,
)
from materialize.version_list import get_compatible_upgrade_from_versions

SERVICES = [
    Mz(app_password=""),
    Materialized(
        volumes_extra=["secrets:/share/secrets"],
        additional_system_parameter_defaults={
            "log_filter": "mz_storage::source::postgres=trace,debug,info,warn,error"
        },
        external_blob_store=True,
        default_replication_factor=2,
    ),
    Materialized(
        name="mz_1",
    ),
    Materialized(
        name="mz_2",
    ),
    Testdrive(),
    CockroachOrPostgresMetadata(),
    Minio(setup_materialize=True),
    TestCerts(),
    Toxiproxy(),
    create_postgres(pg_version=None),
    PostgresRecvlogical(
        replication_slot_name="", publication_name=""
    ),  # Overriden below
]


# TODO: redesign ceased status database-issues#7687
# Test that how subsource statuses work across a variety of scenarios
# def workflow_statuses(c: Composition, parser: WorkflowArgumentParser) -> None:
#     c.up("materialized", "postgres", "toxiproxy")
#     c.run_testdrive_files("status/01-setup.td")

#     with c.override(Testdrive(no_reset=True)):
#         # Restart mz
#         c.kill("materialized")
#         c.up("materialized")

#         c.run_testdrive_files(
#             "status/02-after-mz-restart.td",
#             "status/03-toxiproxy-interrupt.td",
#             "status/04-drop-publication.td",
#         )


def workflow_replication_slots(c: Composition, parser: WorkflowArgumentParser) -> None:
    _workflow_replication_slots(c, parser, max_replication_slots=3)


def workflow_silent_connection_drop(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    _workflow_silent_connection_drop(c, parser, connect_fn=pg8000.connect)


def workflow_cdc(c: Composition, parser: WorkflowArgumentParser) -> None:
    _workflow_cdc(c, parser, exclude=["exclude-columns.td"])


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.run_all_workflows(
        exclude=["migration", "migration-multi-version-upgrade"],
        internally_sharded=["cdc"],
        args=parser.args,
        between_workflows=kill_pg_and_mz,
    )


def workflow_migration(c: Composition, parser: WorkflowArgumentParser) -> None:
    sharded_files = c.glob_test_files(parser)

    ssl_args_dict = get_testdrive_ssl_args(c)
    testdrive_ssl_args = ssl_args_dict["testdrive_args"]
    volumes_extra = ssl_args_dict["volumes_extra"]

    testdrive_args = (
        testdrive_ssl_args + Materialized.default_testdrive_size_args() + ["--no-reset"]
    )

    pg_version = get_targeted_pg_version(parser)

    for file in sharded_files:
        mz_old = Materialized(
            name="materialized",
            volumes_extra=volumes_extra,
            external_metadata_store=True,
            external_blob_store=True,
            additional_system_parameter_defaults={
                "log_filter": "mz_storage::source::postgres=trace,debug,info,warn,error"
            },
            default_replication_factor=2,
        )

        mz_new = Materialized(
            name="materialized",
            volumes_extra=volumes_extra,
            external_metadata_store=True,
            external_blob_store=True,
            additional_system_parameter_defaults={
                "log_filter": "mz_storage::source::postgres=trace,debug,info,warn,error",
                "force_source_table_syntax": "true",
            },
            default_replication_factor=2,
        )
        with c.override(mz_old, create_postgres(pg_version=pg_version)):
            c.up("materialized", "test-certs", "postgres")

            print(f"Running {file} with mz_old")

            c.run_testdrive_files(
                *testdrive_args,
                file,
            )
            c.kill("materialized", wait=True)

            with c.override(mz_new):
                c.up("materialized")

                print("Running mz_new")
                verify_sources_after_source_table_migration(c, file)

                c.kill("materialized", wait=True)
                c.kill("postgres", wait=True)
                c.kill(METADATA_STORE, wait=True)
                c.rm("materialized")
                c.rm(METADATA_STORE)
                c.rm("postgres")
                c.rm_volumes("mzdata")


def workflow_migration_multi_version_upgrade(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Multiversion upgrade with the source versioning migration.
    """
    pg_version = get_targeted_pg_version(parser)

    parser.add_argument(
        "--mode",
        type=str,
        choices=["random", "earliest-to-current"],
        default="earliest-to-current",
        help="Upgrade mode: 'random' for random version to upgrade from, 'earliest-to-current' for upgrading from the earliest upgradeable version to the current version.",
    )

    parser.add_argument(
        "--seed",
        type=str,
        default=None,
        help="Random seed to use for upgrade path selection",
    )

    parser.add_argument(
        "filter",
        nargs="*",
        default=["*.td"],
        help="limit to only the files matching filter",
    )

    args = parser.parse_args()

    sharded_files = c.shard_test_files(args.filter)

    ssl_args_dict = get_testdrive_ssl_args(c)
    testdrive_ssl_args = ssl_args_dict["testdrive_args"]
    volumes_extra = ssl_args_dict["volumes_extra"]

    testdrive_args = (
        testdrive_ssl_args + Materialized.default_testdrive_size_args() + ["--no-reset"]
    )

    compatible_versions = get_compatible_upgrade_from_versions()

    if args.mode == "random":
        random_initial_version = Random(args.seed).choice(compatible_versions)
        versions = [random_initial_version, None]
    else:
        versions = [compatible_versions[0], None]

    materialize_service_instances = []

    upgrade_args_list = generate_materialized_upgrade_args(versions)

    for i, upgrade_args in enumerate(upgrade_args_list):
        log_filter = "mz_storage::source::postgres=trace,debug,info,warn,error"

        # Enable source versioning migration at the end (final version)
        enable_source_migration_arg = (
            {"force_source_table_syntax": "true"}
            if i == len(upgrade_args_list) - 1
            else {}
        )

        materialize_service_instances.append(
            Materialized(
                **upgrade_args,
                external_blob_store=True,
                volumes_extra=volumes_extra,
                additional_system_parameter_defaults={
                    log_filter: log_filter,
                    **enable_source_migration_arg,
                },
            )
        )

    upgrade_path = Materialized0dtUpgrader(c, materialize_service_instances)

    upgrade_path.print_upgrade_path()

    for file in sharded_files:

        with c.override(create_postgres(pg_version=pg_version)):
            c.up("test-certs", "postgres")
            initial_materialized_service, upgrade_steps = upgrade_path.initialize()
            c.run_testdrive_files(
                *testdrive_args,
                file,
                mz_service=initial_materialized_service.name,
            )

            for step in upgrade_steps:
                step.upgrade()

            # Verify the source table migration at the end.
            last_materialized_service = upgrade_steps[-1].new_service
            print(
                f"Verifying source table migration for version {last_materialized_service.config.get('image')}"
            )
            verify_sources_after_source_table_migration(
                c, file, service=last_materialized_service.name
            )
            upgrade_path.cleanup()
            c.kill("postgres", wait=True)
            c.rm("postgres")
            c.rm_volumes("mzdata")

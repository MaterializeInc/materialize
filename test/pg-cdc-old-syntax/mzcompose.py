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

import glob
import time

import pg8000
from pg8000 import Connection

from materialize import MZ_ROOT, buildkite
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.service import Service, ServiceConfig
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import (
    METADATA_STORE,
    CockroachOrPostgresMetadata,
    Postgres,
)
from materialize.mzcompose.services.test_certs import TestCerts
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.toxiproxy import Toxiproxy
from materialize.source_table_migration import (
    get_new_image_for_source_table_migration_test,
    get_old_image_for_source_table_migration_test,
    verify_sources_after_source_table_migration,
)

# Set the max slot WAL keep size to 10MB
DEFAULT_PG_EXTRA_COMMAND = ["-c", "max_slot_wal_keep_size=10"]


class PostgresRecvlogical(Service):
    """
    Command to start a replication.
    """

    def __init__(self, replication_slot_name: str, publication_name: str) -> None:
        command: list[str] = [
            "pg_recvlogical",
            "--start",
            "--slot",
            f"{replication_slot_name}",
            "--file",
            "-",
            "--dbname",
            "postgres",
            "--host",
            "postgres",
            "--port",
            "5432",
            "--username",
            "postgres",
            "--no-password",
            "-o",
            "proto_version=1",
            "-o",
            f"publication_names={publication_name}",
        ]
        config: ServiceConfig = {"mzbuild": "postgres"}

        config.update(
            {
                "command": command,
                "allow_host_ports": True,
                "ports": ["5432"],
                "environment": ["PGPASSWORD=postgres"],
            }
        )

        super().__init__(name="pg_recvlogical", config=config)


def create_postgres(
    pg_version: str | None, extra_command: list[str] = DEFAULT_PG_EXTRA_COMMAND
) -> Postgres:
    if pg_version is None:
        image = None
    else:
        image = f"postgres:{pg_version}"

    return Postgres(image=image, extra_command=extra_command)


SERVICES = [
    Mz(app_password=""),
    Materialized(
        volumes_extra=["secrets:/share/secrets"],
        additional_system_parameter_defaults={
            "log_filter": "mz_storage::source::postgres=trace,debug,info,warn,error"
        },
        external_blob_store=False,
        default_replication_factor=2,
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


def get_targeted_pg_version(parser: WorkflowArgumentParser) -> str | None:
    parser.add_argument(
        "--pg-version",
        type=str,
    )

    args, _ = parser.parse_known_args()
    pg_version = args.pg_version

    if pg_version is not None:
        print(f"Running with Postgres version {pg_version}")

    return pg_version


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
    pg_version = get_targeted_pg_version(parser)
    with c.override(
        create_postgres(
            pg_version=pg_version, extra_command=["-c", "max_replication_slots=3"]
        )
    ):
        c.up("materialized", "postgres")
        c.run_testdrive_files("override/replication-slots.td")


def workflow_wal_level(c: Composition, parser: WorkflowArgumentParser) -> None:
    pg_version = get_targeted_pg_version(parser)
    for wal_level in ["replica", "minimal"]:
        with c.override(
            create_postgres(
                pg_version=pg_version,
                extra_command=[
                    "-c",
                    "max_wal_senders=0",
                    "-c",
                    f"wal_level={wal_level}",
                ],
            )
        ):
            c.up("materialized", "postgres")
            c.run_testdrive_files("override/insufficient-wal-level.td")


def workflow_replication_disabled(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    pg_version = get_targeted_pg_version(parser)
    with c.override(
        create_postgres(
            pg_version=pg_version, extra_command=["-c", "max_wal_senders=0"]
        )
    ):
        c.up("materialized", "postgres")
        c.run_testdrive_files("override/replication-disabled.td")


def workflow_silent_connection_drop(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Test that mz can regain a replication slot that is used by another service.
    """

    pg_version = get_targeted_pg_version(parser)
    with c.override(
        create_postgres(
            pg_version=pg_version,
            extra_command=[
                "-c",
                "wal_sender_timeout=0",
            ],
        ),
    ):
        c.up("postgres")

        pg_conn = pg8000.connect(
            host="localhost",
            user="postgres",
            password="postgres",
            port=c.default_port("postgres"),
        )

        _verify_exactly_n_replication_slots_exist(pg_conn, n=0)

        c.up("materialized")

        c.run_testdrive_files(
            "--no-reset",
            f"--var=default-replica-size={Materialized.Size.DEFAULT_SIZE}-{Materialized.Size.DEFAULT_SIZE}",
            "override/silent-connection-drop-part-1.td",
        )

        _verify_exactly_n_replication_slots_exist(pg_conn, n=1)

        _await_postgres_replication_slot_state(
            pg_conn,
            await_active=False,
            error_message="Replication slot is still active",
        )

        _claim_postgres_replication_slot(c, pg_conn)

        _await_postgres_replication_slot_state(
            pg_conn,
            await_active=True,
            error_message="Replication slot has not been claimed",
        )

        c.run_testdrive_files("--no-reset", "override/silent-connection-drop-part-2.td")

        _verify_exactly_n_replication_slots_exist(pg_conn, n=1)


def _await_postgres_replication_slot_state(
    pg_conn: Connection, await_active: bool, error_message: str
) -> None:
    for i in range(1, 5):
        is_active = _is_postgres_activation_slot_active(pg_conn)

        if is_active == await_active:
            return
        else:
            time.sleep(1)

    raise RuntimeError(error_message)


def _get_postgres_replication_slot_name(pg_conn: Connection) -> str:
    cursor = pg_conn.cursor()
    cursor.execute("SELECT slot_name FROM pg_replication_slots;")
    return cursor.fetchall()[0][0]


def _claim_postgres_replication_slot(c: Composition, pg_conn: Connection) -> None:
    replicator = PostgresRecvlogical(
        replication_slot_name=_get_postgres_replication_slot_name(pg_conn),
        publication_name="mz_source",
    )

    with c.override(replicator):
        c.up(replicator.name)


def _is_postgres_activation_slot_active(pg_conn: Connection) -> bool:
    cursor = pg_conn.cursor()
    cursor.execute("SELECT active FROM pg_replication_slots;")
    is_active = cursor.fetchall()[0][0]
    return is_active


def _verify_exactly_n_replication_slots_exist(pg_conn: Connection, n: int) -> None:
    cursor = pg_conn.cursor()
    cursor.execute("SELECT count(*) FROM pg_replication_slots;")
    count_slots = cursor.fetchall()[0][0]
    assert (
        count_slots == n
    ), f"Expected {n} replication slot(s) but found {count_slots} slot(s)"


def workflow_cdc(c: Composition, parser: WorkflowArgumentParser) -> None:
    pg_version = get_targeted_pg_version(parser)

    parser.add_argument(
        "filter",
        nargs="*",
        default=["*.td"],
        help="limit to only the files matching filter",
    )
    args = parser.parse_args()

    matching_files = []
    for filter in args.filter:
        matching_files.extend(
            glob.glob(filter, root_dir=MZ_ROOT / "test" / "pg-cdc-old-syntax")
        )
    sharded_files: list[str] = buildkite.shard_list(
        sorted(matching_files), lambda file: file
    )
    print(f"Files: {sharded_files}")

    c.up({"name": "test-certs", "persistent": True})
    ssl_ca = c.run("test-certs", "cat", "/secrets/ca.crt", capture=True).stdout
    ssl_cert = c.run("test-certs", "cat", "/secrets/certuser.crt", capture=True).stdout
    ssl_key = c.run("test-certs", "cat", "/secrets/certuser.key", capture=True).stdout
    ssl_wrong_cert = c.run(
        "test-certs", "cat", "/secrets/postgres.crt", capture=True
    ).stdout
    ssl_wrong_key = c.run(
        "test-certs", "cat", "/secrets/postgres.key", capture=True
    ).stdout

    with c.override(create_postgres(pg_version=pg_version)):
        c.up("materialized", "test-certs", "postgres")
        c.test_parts(
            sharded_files,
            lambda file: c.run_testdrive_files(
                f"--var=ssl-ca={ssl_ca}",
                f"--var=ssl-cert={ssl_cert}",
                f"--var=ssl-key={ssl_key}",
                f"--var=ssl-wrong-cert={ssl_wrong_cert}",
                f"--var=ssl-wrong-key={ssl_wrong_key}",
                f"--var=default-replica-size={Materialized.Size.DEFAULT_SIZE}-{Materialized.Size.DEFAULT_SIZE}",
                f"--var=default-storage-size={Materialized.Size.DEFAULT_SIZE}-1",
                file,
            ),
        )


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    def process(name: str) -> None:
        if name in ("default", "migration"):
            return

        # TODO: Flaky, reenable when database-issues#7611 is fixed
        if name == "statuses":
            return

        # TODO: Flaky, reenable when database-issues#8447 is fixed
        if name == "silent-connection-drop":
            return

        c.kill("postgres")
        c.rm("postgres")
        c.kill("materialized")
        c.rm("materialized")

        with c.test_case(name):
            c.workflow(name, *parser.args)

    workflows_with_internal_sharding = ["cdc"]
    sharded_workflows = workflows_with_internal_sharding + buildkite.shard_list(
        [
            w
            for w in c.workflows
            if w not in workflows_with_internal_sharding and w != "migration"
        ],
        lambda w: w,
    )
    print(
        f"Workflows in shard with index {buildkite.get_parallelism_index()}: {sharded_workflows}"
    )
    c.test_parts(sharded_workflows, process)


def workflow_migration(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "filter",
        nargs="*",
        default=["*.td"],
        help="limit to only the files matching filter",
    )
    args = parser.parse_args()

    matching_files = []
    for filter in args.filter:
        matching_files.extend(
            glob.glob(filter, root_dir=MZ_ROOT / "test" / "pg-cdc-old-syntax")
        )
    sharded_files: list[str] = buildkite.shard_list(
        sorted(matching_files), lambda file: file
    )
    print(f"Files: {sharded_files}")

    c.up({"name": "test-certs", "persistent": True})
    ssl_ca = c.run("test-certs", "cat", "/secrets/ca.crt", capture=True).stdout
    ssl_cert = c.run("test-certs", "cat", "/secrets/certuser.crt", capture=True).stdout
    ssl_key = c.run("test-certs", "cat", "/secrets/certuser.key", capture=True).stdout
    ssl_wrong_cert = c.run(
        "test-certs", "cat", "/secrets/postgres.crt", capture=True
    ).stdout
    ssl_wrong_key = c.run(
        "test-certs", "cat", "/secrets/postgres.key", capture=True
    ).stdout

    pg_version = get_targeted_pg_version(parser)

    for file in sharded_files:
        mz_old = Materialized(
            name="materialized",
            image=get_old_image_for_source_table_migration_test(),
            volumes_extra=["secrets:/share/secrets"],
            external_metadata_store=True,
            external_blob_store=True,
            additional_system_parameter_defaults={
                "log_filter": "mz_storage::source::postgres=trace,debug,info,warn,error"
            },
            default_replication_factor=2,
        )

        mz_new = Materialized(
            name="materialized",
            image=get_new_image_for_source_table_migration_test(),
            volumes_extra=["secrets:/share/secrets"],
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
                f"--var=ssl-ca={ssl_ca}",
                f"--var=ssl-cert={ssl_cert}",
                f"--var=ssl-key={ssl_key}",
                f"--var=ssl-wrong-cert={ssl_wrong_cert}",
                f"--var=ssl-wrong-key={ssl_wrong_key}",
                f"--var=default-replica-size={Materialized.Size.DEFAULT_SIZE}-{Materialized.Size.DEFAULT_SIZE}",
                f"--var=default-storage-size={Materialized.Size.DEFAULT_SIZE}-1",
                "--no-reset",
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

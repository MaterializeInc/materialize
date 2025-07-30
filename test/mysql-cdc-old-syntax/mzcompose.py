# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Functional test for the native (non-Debezium) MySQL sources.
"""

import glob
import threading
from textwrap import dedent

from materialize import MZ_ROOT, buildkite
from materialize.mysql_util import (
    retrieve_invalid_ssl_context_for_mysql,
    retrieve_ssl_context_for_mysql,
)
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import (
    METADATA_STORE,
    CockroachOrPostgresMetadata,
)
from materialize.mzcompose.services.test_certs import TestCerts
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.source_table_migration import (
    get_new_image_for_source_table_migration_test,
    get_old_image_for_source_table_migration_test,
    verify_sources_after_source_table_migration,
)


def create_mysql(mysql_version: str) -> MySql:
    return MySql(version=mysql_version)


def create_mysql_replica(mysql_version: str) -> MySql:
    return MySql(
        name="mysql-replica",
        version=mysql_version,
        additional_args=[
            "--gtid_mode=ON",
            "--enforce_gtid_consistency=ON",
            "--skip-replica-start",
            "--server-id=2",
        ],
    )


SERVICES = [
    Mz(app_password=""),
    Materialized(
        external_blob_store=False,
        additional_system_parameter_defaults={
            "log_filter": "mz_storage::source::mysql=trace,info"
        },
        default_replication_factor=2,
    ),
    create_mysql(MySql.DEFAULT_VERSION),
    create_mysql_replica(MySql.DEFAULT_VERSION),
    TestCerts(),
    CockroachOrPostgresMetadata(),
    Minio(setup_materialize=True),
    Testdrive(default_timeout="60s"),
]


def get_targeted_mysql_version(parser: WorkflowArgumentParser) -> str:
    parser.add_argument(
        "--mysql-version",
        default=MySql.DEFAULT_VERSION,
        type=str,
    )

    args, _ = parser.parse_known_args()
    print(f"Running with MySQL version {args.mysql_version}")
    return args.mysql_version


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    def process(name: str) -> None:
        if name in ("default", "migration"):
            return
        with c.test_case(name):
            c.workflow(name, *parser.args)

    workflows_with_internal_sharding = ["cdc"]
    sharded_workflows = workflows_with_internal_sharding + buildkite.shard_list(
        [w for w in c.workflows if w not in workflows_with_internal_sharding],
        lambda w: w,
    )
    print(
        f"Workflows in shard with index {buildkite.get_parallelism_index()}: {sharded_workflows}"
    )
    c.test_parts(sharded_workflows, process)


def workflow_cdc(c: Composition, parser: WorkflowArgumentParser) -> None:
    mysql_version = get_targeted_mysql_version(parser)

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
            glob.glob(filter, root_dir=MZ_ROOT / "test" / "mysql-cdc-old-syntax")
        )
    sharded_files: list[str] = buildkite.shard_list(
        sorted(matching_files), lambda file: file
    )
    print(f"Files: {sharded_files}")

    with c.override(create_mysql(mysql_version)):
        c.up("materialized", "mysql")

        valid_ssl_context = retrieve_ssl_context_for_mysql(c)
        wrong_ssl_context = retrieve_invalid_ssl_context_for_mysql(c)

        c.sources_and_sinks_ignored_from_validation.add("drop_table")

        c.test_parts(
            sharded_files,
            lambda file: c.run_testdrive_files(
                f"--var=ssl-ca={valid_ssl_context.ca}",
                f"--var=ssl-client-cert={valid_ssl_context.client_cert}",
                f"--var=ssl-client-key={valid_ssl_context.client_key}",
                f"--var=ssl-wrong-ca={wrong_ssl_context.ca}",
                f"--var=ssl-wrong-client-cert={wrong_ssl_context.client_cert}",
                f"--var=ssl-wrong-client-key={wrong_ssl_context.client_key}",
                f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
                "--var=mysql-user-password=us3rp4ssw0rd",
                f"--var=default-replica-size={Materialized.Size.DEFAULT_SIZE}-{Materialized.Size.DEFAULT_SIZE}",
                f"--var=default-storage-size={Materialized.Size.DEFAULT_SIZE}-1",
                file,
            ),
        )


def workflow_replica_connection(c: Composition, parser: WorkflowArgumentParser) -> None:
    mysql_version = get_targeted_mysql_version(parser)
    with c.override(create_mysql(mysql_version), create_mysql_replica(mysql_version)):
        c.up("materialized", "mysql", "mysql-replica")
        c.run_testdrive_files(
            f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
            "override/10-replica-connection.td",
        )


def workflow_schema_change_restart(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Validates that a schema change done to a table after the MySQL source is created
    but before the snapshot is completed is detected after a restart.
    """

    mysql_version = get_targeted_mysql_version(parser)
    with c.override(create_mysql(mysql_version)):
        c.up("materialized", "mysql")
        c.run_testdrive_files(
            f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
            "schema-restart/before-restart.td",
        )

    with c.override(Testdrive(no_reset=True), create_mysql(mysql_version)):
        # Restart mz
        c.kill("materialized")
        c.up("materialized")

        c.run_testdrive_files(
            f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
            "schema-restart/after-restart.td",
        )


def _make_inserts(*, txns: int, txn_size: int) -> tuple[str, int]:
    sql = "\n".join(
        [
            f"""
            SET @i:=0;
            INSERT INTO many_inserts (f2) SELECT @i:=@i+1 FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT {txn_size};
            """
            for i in range(0, txns)
        ]
    )
    records = txns * txn_size
    return (sql, records)


def workflow_many_inserts(c: Composition, parser: WorkflowArgumentParser) -> None:
    """
    Tests a scenario that caused a consistency issue in the past. We insert a
    large number of rows into a table, then create a source for that table while
    simultaneously inserting many more rows into the table in a background
    thread, then finally verify that the correct count of rows is captured by
    the source.

    In earlier incarnations of the MySQL source, the source accidentally failed
    to snapshot inside of a repeatable read transaction.
    """
    mysql_version = get_targeted_mysql_version(parser)
    with c.override(create_mysql(mysql_version)):
        c.up("materialized", "mysql", {"name": "testdrive", "persistent": True})

        # Records to before creating the source.
        (initial_sql, initial_records) = _make_inserts(txns=1, txn_size=1_000_000)

        # Records to insert concurrently with creating the source.
        (concurrent_sql, concurrent_records) = _make_inserts(txns=1000, txn_size=100)

        # Set up the MySQL server with the initial records, set up the connection to
        # the MySQL server in Materialize.
        c.testdrive(
            dedent(
                f"""
                $ postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
                ALTER SYSTEM SET max_mysql_connections = 100

                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                > CREATE SECRET IF NOT EXISTS mysqlpass AS '{MySql.DEFAULT_ROOT_PASSWORD}'
                > CREATE CONNECTION IF NOT EXISTS mysql_conn TO MYSQL (HOST mysql, USER root, PASSWORD SECRET mysqlpass)

                $ mysql-execute name=mysql
                DROP DATABASE IF EXISTS public;
                CREATE DATABASE public;
                USE public;
                DROP TABLE IF EXISTS many_inserts;
                CREATE TABLE many_inserts (pk SERIAL PRIMARY KEY, f2 BIGINT);
                """
            )
            + dedent(initial_sql)
            + dedent(
                """
                > DROP SOURCE IF EXISTS s1 CASCADE;
                """
            )
        )

    # Start inserting in the background.

    def do_inserts(c: Composition):
        x = dedent(
            f"""
            $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

            $ mysql-execute name=mysql
            USE public;
            {concurrent_sql}
            """
        )
        c.testdrive(args=["--no-reset"], input=x)

    insert_thread = threading.Thread(target=do_inserts, args=(c,))
    print("--- Start many concurrent inserts")
    insert_thread.start()

    # Create the source.
    c.testdrive(
        args=["--no-reset"],
        input=dedent(
            """
            > CREATE SOURCE s1
                FROM MYSQL CONNECTION mysql_conn
                FOR TABLES (public.many_inserts);
            """
        ),
    )

    # Ensure the source eventually sees the right number of records.
    insert_thread.join()

    print("--- Validate concurrent inserts")
    c.testdrive(
        args=["--no-reset"],
        input=dedent(
            f"""
            > SELECT count(*) FROM many_inserts
            {initial_records + concurrent_records}
            """
        ),
    )


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
            glob.glob(filter, root_dir=MZ_ROOT / "test" / "mysql-cdc-old-syntax")
        )
    sharded_files: list[str] = buildkite.shard_list(
        sorted(matching_files), lambda file: file
    )
    print(f"Files: {sharded_files}")

    mysql_version = get_targeted_mysql_version(parser)

    for file in sharded_files:

        mz_old = Materialized(
            name="materialized",
            image=get_old_image_for_source_table_migration_test(),
            external_metadata_store=True,
            external_blob_store=True,
            additional_system_parameter_defaults={
                "log_filter": "mz_storage::source::mysql=trace,info"
            },
            default_replication_factor=2,
        )

        mz_new = Materialized(
            name="materialized",
            image=get_new_image_for_source_table_migration_test(),
            external_metadata_store=True,
            external_blob_store=True,
            additional_system_parameter_defaults={
                "log_filter": "mz_storage::source::mysql=trace,info",
                "force_source_table_syntax": "true",
            },
            default_replication_factor=2,
        )

        with c.override(mz_old, create_mysql(mysql_version)):
            c.up("materialized", "mysql")

            print(f"Running {file} with mz_old")

            valid_ssl_context = retrieve_ssl_context_for_mysql(c)
            wrong_ssl_context = retrieve_invalid_ssl_context_for_mysql(c)

            c.sources_and_sinks_ignored_from_validation.add("drop_table")

            c.run_testdrive_files(
                f"--var=ssl-ca={valid_ssl_context.ca}",
                f"--var=ssl-client-cert={valid_ssl_context.client_cert}",
                f"--var=ssl-client-key={valid_ssl_context.client_key}",
                f"--var=ssl-wrong-ca={wrong_ssl_context.ca}",
                f"--var=ssl-wrong-client-cert={wrong_ssl_context.client_cert}",
                f"--var=ssl-wrong-client-key={wrong_ssl_context.client_key}",
                f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
                "--var=mysql-user-password=us3rp4ssw0rd",
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
                c.kill("mysql", wait=True)
                c.kill(METADATA_STORE, wait=True)
                c.rm("materialized")
                c.rm(METADATA_STORE)
                c.rm("mysql")
                c.rm_volumes("mzdata")

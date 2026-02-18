# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Shared workflow functions for mysql-cdc and mysql-cdc-old-syntax tests."""

import threading
from textwrap import dedent

from materialize.mysql_util import (
    create_mysql,
    create_mysql_replica,
    get_targeted_mysql_version,
    retrieve_invalid_ssl_context_for_mysql,
    retrieve_ssl_context_for_mysql,
)
from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.testdrive import Testdrive


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


def workflow_cdc(c: Composition, parser: WorkflowArgumentParser) -> None:
    mysql_version = get_targeted_mysql_version(parser)
    sharded_files = c.glob_test_files(parser)

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
                *MySql.default_testdrive_args(),
                "--var=mysql-user-password=us3rp4ssw0rd",
                *Materialized.default_testdrive_size_args(),
                file,
            ),
        )


def workflow_replica_connection(c: Composition, parser: WorkflowArgumentParser) -> None:
    mysql_version = get_targeted_mysql_version(parser)
    with c.override(create_mysql(mysql_version), create_mysql_replica(mysql_version)):
        c.up("materialized", "mysql", "mysql-replica")
        c.run_testdrive_files(
            *MySql.default_testdrive_args(),
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
            *MySql.default_testdrive_args(),
            "schema-restart/before-restart.td",
        )

    with c.override(Testdrive(no_reset=True), create_mysql(mysql_version)):
        c.restart_mz()

        c.run_testdrive_files(
            *MySql.default_testdrive_args(),
            "schema-restart/after-restart.td",
        )


def workflow_many_inserts(
    c: Composition,
    parser: WorkflowArgumentParser,
    create_source_sql: str,
) -> None:
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
        c.up("materialized", "mysql", Service("testdrive", idle=True))

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
        input=dedent(create_source_sql),
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

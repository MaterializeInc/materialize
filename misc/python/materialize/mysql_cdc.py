# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Shared workflow functions for mysql-cdc and mysql-cdc-old-syntax tests."""

from materialize.mysql_util import (
    create_mysql,
    create_mysql_replica,
    get_targeted_mysql_version,
    retrieve_invalid_ssl_context_for_mysql,
    retrieve_ssl_context_for_mysql,
)
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
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

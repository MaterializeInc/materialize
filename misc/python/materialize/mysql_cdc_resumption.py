# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Shared helpers and scenario functions for MySQL CDC resumption tests.

Used by both test/mysql-cdc-resumption and test/mysql-cdc-resumption-old-syntax.
"""

import time
from typing import Any

import pymysql

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.mysql import MySql


def run_testdrive_files(c: Composition, *files: str, mysql_host: str = "mysql") -> None:
    c.run_testdrive_files(
        *MySql.default_testdrive_args(),
        f"--var=mysql-host={mysql_host}",
        *files,
    )


def create_mysql_connection(c: Composition) -> Any:
    return pymysql.connect(
        host="localhost",
        user="root",
        password=MySql.DEFAULT_ROOT_PASSWORD,
        database="mysql",
        port=c.default_port("mysql"),
    )


def initialize(c: Composition, create_source: bool = True) -> None:
    c.down(destroy_volumes=True)
    c.up("materialized", "mysql", "toxiproxy")

    run_testdrive_files(
        c,
        "configure-toxiproxy.td",
        "configure-mysql.td",
        "populate-tables.td",
    )

    if create_source:
        run_testdrive_files(
            c,
            "--var=mysql-source-host=toxiproxy",
            "create-source.td",
        )


def restart_mysql(c: Composition) -> None:
    c.kill("mysql")
    c.up("mysql")


def end(c: Composition) -> None:
    """Validate the data at the end."""
    run_testdrive_files(c, "verify-data.td", "cleanup.td")


def _purge_bin_logs(cur: Any) -> None:
    cur.execute("FLUSH BINARY LOGS")
    time.sleep(2)
    cur.execute("PURGE BINARY LOGS BEFORE now()")
    cur.execute("FLUSH BINARY LOGS")


def _corrupt_bin_log(c: Composition) -> None:
    run_testdrive_files(c, "wait-for-snapshot.td")

    c.kill("materialized")

    mysql_conn = create_mysql_connection(c)

    mysql_conn.autocommit(True)
    with mysql_conn.cursor() as cur:
        cur.execute("INSERT INTO public.t1 VALUES (1, 'text')")

        _purge_bin_logs(cur)

        cur.execute("INSERT INTO public.t1 VALUES (2, 'text')")

    c.up("materialized")


def disconnect_mysql_during_snapshot(c: Composition) -> None:
    run_testdrive_files(
        c,
        "toxiproxy-close-connection.td",
        "toxiproxy-restore-connection.td",
        "delete-rows-t1.td",
        "delete-rows-t2.td",
        "alter-table.td",
        "alter-mz.td",
    )


def restart_mysql_during_snapshot(c: Composition) -> None:
    restart_mysql(c)

    run_testdrive_files(
        c,
        "delete-rows-t1.td",
        "delete-rows-t2.td",
        "alter-table.td",
        "alter-mz.td",
    )


def restart_mz_during_snapshot(c: Composition) -> None:
    run_testdrive_files(c, "alter-mz.td")
    c.restart_mz()

    run_testdrive_files(c, "delete-rows-t1.td", "delete-rows-t2.td", "alter-table.td")


def disconnect_mysql_during_replication(c: Composition) -> None:
    run_testdrive_files(
        c,
        "wait-for-snapshot.td",
        "delete-rows-t1.td",
        "delete-rows-t2.td",
        "alter-table.td",
        "alter-mz.td",
        "toxiproxy-close-connection.td",
        "toxiproxy-restore-connection.td",
    )


def restart_mysql_during_replication(c: Composition) -> None:
    run_testdrive_files(
        c,
        "wait-for-snapshot.td",
        "delete-rows-t1.td",
        "alter-table.td",
        "alter-mz.td",
    )

    restart_mysql(c)

    run_testdrive_files(c, "delete-rows-t2.td")


def restart_mz_during_replication(c: Composition) -> None:
    run_testdrive_files(
        c,
        "wait-for-snapshot.td",
        "delete-rows-t1.td",
        "alter-table.td",
        "alter-mz.td",
    )

    c.restart_mz()

    run_testdrive_files(c, "delete-rows-t2.td")


def fix_mysql_schema_while_mz_restarts(c: Composition) -> None:
    run_testdrive_files(
        c,
        "delete-rows-t1.td",
        "delete-rows-t2.td",
        "alter-table.td",
        "alter-mz.td",
        "verify-data.td",
        "alter-table-fix.td",
    )
    c.restart_mz()


def verify_no_snapshot_reingestion(c: Composition) -> None:
    """Confirm that Mz does not reingest the entire snapshot on restart by
    revoking its SELECT privileges
    """
    run_testdrive_files(c, "wait-for-snapshot.td", "mysql-disable-select-permission.td")

    c.restart_mz()

    run_testdrive_files(
        c,
        "delete-rows-t1.td",
        "delete-rows-t2.td",
        "alter-table.td",
        "alter-mz.td",
    )


def reset_master_gtid(c: Composition) -> None:
    """Confirm behavior after resetting GTID in MySQL"""
    run_testdrive_files(c, "reset-master.td")

    run_testdrive_files(
        c,
        "delete-rows-t1.td",
        "verify-source-stalled.td",
    )


def corrupt_bin_log_to_stall_source(c: Composition) -> None:
    """
    Switch off mz, modify data in mysql, and purge the bin-log so that mz hasn't seen all entries in the replication
    stream.
    """
    _corrupt_bin_log(c)

    run_testdrive_files(
        c,
        "verify-source-stalled.td",
    )


def corrupt_bin_log_and_add_sub_source(c: Composition) -> None:
    """
    Corrupt the bin log, add a sub source, and expect it to be working.
    """

    _corrupt_bin_log(c)

    run_testdrive_files(
        c,
        "populate-table-t3.td",
        "alter-source-add-subsource.td",
        "verify-t3.td",
    )


def transaction_with_rollback(c: Composition) -> None:
    """
    Rollback a tx in MySQL.
    """

    # needed for verify-data to succeed in the end (triggered by the workflow)
    run_testdrive_files(
        c,
        "delete-rows-t1.td",
        "delete-rows-t2.td",
    )

    mysql_conn = create_mysql_connection(c)

    mysql_conn.autocommit(False)
    with mysql_conn.cursor() as cur:
        cur.execute("INSERT INTO public.t1 VALUES (1, 'text')")

        time.sleep(2)

        cur.execute("INSERT INTO public.t1 VALUES (2, 'text')")

        time.sleep(2)

        cur.execute("ROLLBACK")

    run_testdrive_files(
        c,
        "verify-source-running.td",
    )

    # needed for verify-data to succeed in the end (triggered by the workflow)
    run_testdrive_files(
        c,
        "alter-table.td",
        "alter-mz.td",
    )


def mysql_out_of_disk_space(c: Composition) -> None:
    run_testdrive_files(
        c,
        "wait-for-snapshot.td",
        "delete-rows-t1.td",
    )

    fill_file = "/var/lib/mysql/fill_file"
    c.exec(
        "mysql",
        "bash",
        "-c",
        f"dd if=/dev/zero of={fill_file} bs=1024 count=$[1024*512] || true",
    )
    print("Sleeping for 30 seconds ...")
    time.sleep(30)
    c.exec("mysql", "bash", "-c", f"rm {fill_file}")

    run_testdrive_files(c, "delete-rows-t2.td", "alter-table.td", "alter-mz.td")


def backup_restore_mysql(c: Composition) -> None:
    # Backup MySQL, wait for completion
    backup_file = "backup.sql"
    c.exec(
        "mysql",
        "bash",
        "-c",
        f"export MYSQL_PWD={MySql.DEFAULT_ROOT_PASSWORD} && mysqldump --all-databases -u root --set-gtid-purged=OFF > {backup_file}",
    )
    run_testdrive_files(c, "delete-rows-t1.td")

    run_testdrive_files(c, "verify-rows-deleted-t1.td")

    # Restart MySQL service
    c.stop("mysql")
    c.up("mysql")

    c.exec(
        "mysql",
        "bash",
        "-c",
        f"export MYSQL_PWD={MySql.DEFAULT_ROOT_PASSWORD} && mysql -u root < {backup_file}",
    )

    run_testdrive_files(c, "verify-mysql-select.td")

    # TODO: database-issues#7683: one of the two following commands must succeed
    # run_testdrive_files(c, "verify-rows-after-restore-t1.td")
    # run_testdrive_files(c, "verify-source-failed.td")


def create_source_after_logs_expiration(
    c: Composition, bin_log_expiration_in_sec: int
) -> None:
    """Populate tables, delete rows, and create the source after the log expiration in MySQL took place"""

    run_testdrive_files(c, "delete-rows-t1.td")

    sleep_duration = bin_log_expiration_in_sec + 2
    print(f"Sleeping for {sleep_duration} sec")
    time.sleep(sleep_duration)

    mysql_conn = create_mysql_connection(c)
    with mysql_conn.cursor() as cur:
        cur.execute("FLUSH BINARY LOGS")

    restart_mysql(c)

    # not really necessary, still do it
    mysql_conn = create_mysql_connection(c)
    mysql_conn.autocommit(True)
    with mysql_conn.cursor() as cur:
        cur.execute("FLUSH BINARY LOGS")

    run_testdrive_files(
        c,
        "--var=mysql-source-host=toxiproxy",
        "create-source.td",
    )

    run_testdrive_files(c, "verify-rows-deleted-t1.td")


def logs_expiration_while_mz_down(
    c: Composition, bin_log_expiration_in_sec: int
) -> None:
    """Switch off mz, conduct changes in MySQL, let MySQL bin logs expire, and start mz"""

    run_testdrive_files(
        c,
        "--var=mysql-source-host=toxiproxy",
        "create-source.td",
    )

    c.kill("materialized")

    mysql_conn = create_mysql_connection(c)
    mysql_conn.autocommit(True)
    with mysql_conn.cursor() as cur:
        cur.execute("DELETE FROM public.t1 WHERE f1 % 2 = 0;")

    sleep_duration = bin_log_expiration_in_sec + 2
    print(f"Sleeping for {sleep_duration} sec")
    time.sleep(sleep_duration)

    restart_mysql(c)

    mysql_conn = create_mysql_connection(c)
    mysql_conn.autocommit(True)

    # conduct a further change to be added to the bin log
    with mysql_conn.cursor() as cur:
        cur.execute("UPDATE public.t1 SET f2 = NULL;")
        cur.execute("FLUSH BINARY LOGS")

    c.up("materialized")

    run_testdrive_files(c, "verify-source-stalled.td")

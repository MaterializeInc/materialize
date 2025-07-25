# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Functional test for the native (non-Debezium) MySQL sources, using Toxiproxy to
simulate bad network as well as checking how Materialize handles binary log
corruptions.
"""

import time
from typing import Any

import pymysql
from psycopg import Cursor

from materialize import buildkite
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.alpine import Alpine
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql, create_mysql_server_args
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.toxiproxy import Toxiproxy

SERVICES = [
    Alpine(),
    Mz(app_password=""),
    Materialized(default_replication_factor=2),
    MySql(),
    MySql(
        name="mysql-replica-1",
        port=3308,
        version=MySql.DEFAULT_VERSION,
        additional_args=create_mysql_server_args(server_id="2", is_master=False),
    ),
    MySql(
        name="mysql-replica-2",
        port=3309,
        version=MySql.DEFAULT_VERSION,
        additional_args=create_mysql_server_args(server_id="3", is_master=False),
    ),
    Toxiproxy(),
    Testdrive(
        no_reset=True,
        default_timeout="300s",
    ),
]


def workflow_default(c: Composition) -> None:
    def process(name: str) -> None:
        if name == "default":
            return

        # TODO(def-): Reenable when database-issues#7775 is fixed
        if name in ("bin-log-manipulations", "short-bin-log-retention"):
            return

        # clear to avoid issues
        c.kill("mysql")
        c.rm("mysql")

        with c.test_case(name):
            c.workflow(name)

    workflows_with_internal_sharding = [
        "disruptions",
        "bin-log-manipulations",
        "short-bin-log-retention",
    ]
    sharded_workflows = workflows_with_internal_sharding + buildkite.shard_list(
        [w for w in c.workflows if w not in workflows_with_internal_sharding],
        lambda w: w,
    )
    print(
        f"Workflows in shard with index {buildkite.get_parallelism_index()}: {sharded_workflows}"
    )
    c.test_parts(sharded_workflows, process)


def workflow_disruptions(c: Composition) -> None:
    """Test MySQL direct replication's failure handling by
    disrupting replication at various stages using Toxiproxy or service restarts
    """

    scenarios = [
        mysql_out_of_disk_space,
        disconnect_mysql_during_snapshot,
        disconnect_mysql_during_replication,
        restart_mysql_during_snapshot,
        restart_mz_during_snapshot,
        restart_mysql_during_replication,
        restart_mz_during_replication,
        fix_mysql_schema_while_mz_restarts,
        verify_no_snapshot_reingestion,
        transaction_with_rollback,
    ]

    scenarios = buildkite.shard_list(scenarios, lambda s: s.__name__)
    print(
        f"Scenarios in shard with index {buildkite.get_parallelism_index()}: {[s.__name__ for s in scenarios]}"
    )

    for scenario in scenarios:
        overrides = (
            [MySql(volumes=["sourcedata_512Mb:/var/lib/mysql"])]
            if scenario == mysql_out_of_disk_space
            else []
        )
        with c.override(*overrides):
            print(
                f"--- Running scenario {scenario.__name__} with overrides: {overrides}"
            )
            initialize(c)
            scenario(c)
            end(c)


def workflow_backup_restore(c: Composition) -> None:
    with c.override(
        Materialized(sanity_restart=False, default_replication_factor=2),
    ):
        scenario = backup_restore_mysql
        print(f"--- Running scenario {scenario.__name__}")
        initialize(c)
        scenario(c)
        # No end confirmation here, since we expect the source to be in a bad state


def workflow_bin_log_manipulations(c: Composition) -> None:
    with c.override(
        Materialized(sanity_restart=False, default_replication_factor=2),
    ):
        scenarios = [
            reset_master_gtid,
            corrupt_bin_log_to_stall_source,
            corrupt_bin_log_and_add_sub_source,
        ]

        scenarios = buildkite.shard_list(scenarios, lambda s: s.__name__)
        print(
            f"Scenarios in shard with index {buildkite.get_parallelism_index()}: {[s.__name__ for s in scenarios]}"
        )

        for scenario in scenarios:
            print(f"--- Running scenario {scenario.__name__}")
            initialize(c)
            scenario(c)
            # No end confirmation here, since we expect the source to be in a bad state


def workflow_short_bin_log_retention(c: Composition) -> None:
    bin_log_expiration_in_sec = 2
    args = MySql.DEFAULT_ADDITIONAL_ARGS.copy()
    args.append(f"--binlog_expire_logs_seconds={bin_log_expiration_in_sec}")

    with c.override(
        Materialized(sanity_restart=False, default_replication_factor=2),
        MySql(additional_args=args),
    ):
        scenarios = [logs_expiration_while_mz_down, create_source_after_logs_expiration]

        scenarios = buildkite.shard_list(scenarios, lambda s: s.__name__)
        print(
            f"Scenarios in shard with index {buildkite.get_parallelism_index()}: {[s.__name__ for s in scenarios]}"
        )

        for scenario in scenarios:
            print(f"--- Running scenario {scenario.__name__}")
            initialize(c, create_source=False)
            scenario(
                c,
                bin_log_expiration_in_sec,
            )
            # No end confirmation here


def workflow_master_changes(c: Composition) -> None:
    """
    mysql-replica-1 and mysql-replica-2 replicate mysql. The source is attached to mysql-replica-2. mysql is
    killed and mysql-replica-1 becomes the new master. mysql-replica-2 is configured to replicate from mysql-replica-1.
    """

    with c.override(
        Materialized(sanity_restart=False, default_replication_factor=2),
        MySql(
            name="mysql-replica-1",
            port=3308,
            version=MySql.DEFAULT_VERSION,
            additional_args=create_mysql_server_args(server_id="2", is_master=False),
        ),
        MySql(
            name="mysql-replica-2",
            port=3309,
            version=MySql.DEFAULT_VERSION,
            additional_args=create_mysql_server_args(server_id="3", is_master=False),
        ),
    ):
        initialize(c, create_source=False)

        host_data_master = "mysql"
        port_data_master = 3306
        host_for_mz_source = "mysql-replica-2"
        port_for_mz_source = 3309

        c.up("mysql-replica-1", "mysql-replica-2")

        # configure mysql-replica-1 to replicate mysql
        run_testdrive_files(
            c,
            f"--var=mysql-replication-master-host={host_data_master}",
            f"--var=mysql-replication-master-port={port_data_master}",
            "configure-replica.td",
            mysql_host="mysql-replica-1",
            mysql_port=3308,
        )
        # configure mysql-replica-2 to replicate mysql
        run_testdrive_files(
            c,
            f"--var=mysql-replication-master-host={host_data_master}",
            f"--var=mysql-replication-master-port={port_data_master}",
            "configure-replica.td",
            mysql_host="mysql-replica-2",
            mysql_port=3309,
        )
        # wait for mysql-replica-2 to replicate the current state
        time.sleep(15)
        run_testdrive_files(
            c,
            f"--var=mysql-source-host={host_for_mz_source}",
            f"--var=mysql-source-port={port_for_mz_source}",
            "verify-mysql-source-select-all.td",
        )
        # create source pointing to mysql-replica-2
        run_testdrive_files(
            c,
            f"--var=mysql-source-host={host_for_mz_source}",
            f"--var=mysql-source-port={port_for_mz_source}",
            "create-source.td",
        )

        run_testdrive_files(
            c,
            "verify-source-running.td",
        )

        c.kill("mysql")
        host_data_master = "mysql-replica-1"
        port_data_master = 3308

        # let mysql-replica-2 replicate from mysql-replica-1
        run_testdrive_files(
            c,
            f"--var=mysql-replication-master-host={host_data_master}",
            f"--var=mysql-replication-master-port={port_data_master}",
            "configure-replica.td",
            mysql_host=host_for_mz_source,
            mysql_port=port_for_mz_source,
        )

        run_testdrive_files(
            c,
            "verify-source-running.td",
        )

        # delete rows in mysql-replica-1
        run_testdrive_files(
            c,
            "delete-rows-t1.td",
            mysql_host=host_data_master,
            mysql_port=port_data_master,
        )

        # It may take some time until mysql-replica-2 catches up.
        time.sleep(15)

        run_testdrive_files(
            c,
            "verify-rows-deleted-t1.td",
        )


def workflow_switch_to_replica_and_kill_master(c: Composition) -> None:
    """
    mysql-replica-1 replicates mysql. The source is attached to mysql. mz switches the connection to mysql-replica-1.
    Changing the connection should not brick the source and the source should still work if mysql is killed.
    """

    with c.override(
        Materialized(sanity_restart=False, default_replication_factor=2),
        MySql(
            name="mysql-replica-1",
            port=3308,
            version=MySql.DEFAULT_VERSION,
            additional_args=create_mysql_server_args(server_id="2", is_master=False),
        ),
    ):
        initialize(c)

        host_data_master = "mysql"
        port_data_master = 3306
        host_for_mz_source = "mysql"
        port_for_mz_source = 3306

        c.up("mysql-replica-1")

        # configure replica
        run_testdrive_files(
            c,
            f"--var=mysql-replication-master-host={host_data_master}",
            f"--var=mysql-replication-master-port={port_data_master}",
            "configure-replica.td",
            mysql_host="mysql-replica-1",
            mysql_port=3308,
        )

        # give the replica some time to replicate the current state
        time.sleep(3)

        run_testdrive_files(
            c,
            "delete-rows-t1.td",
            "verify-rows-deleted-t1.td",
        )

        # change connection to replica
        host_for_mz_source = "mysql-replica-1"
        port_for_mz_source = 3308
        run_testdrive_files(
            c,
            f"--var=mysql-source-host={host_for_mz_source}",
            f"--var=mysql-source-port={port_for_mz_source}",
            "alter-source-connection.td",
        )

        run_testdrive_files(
            c,
            "verify-source-running.td",
        )

        c.kill("mysql")

        time.sleep(3)

        run_testdrive_files(
            c,
            "verify-source-running.td",
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
            "--var=mysql-source-port=3307",
            "create-source.td",
        )


def restart_mysql(c: Composition) -> None:
    c.kill("mysql")
    c.up("mysql")


def restart_mz(c: Composition) -> None:
    c.kill("materialized")
    c.up("materialized")


def end(c: Composition) -> None:
    """Validate the data at the end."""
    run_testdrive_files(c, "verify-data.td", "cleanup.td")


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
    restart_mz(c)

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

    restart_mz(c)

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
    restart_mz(c)


def verify_no_snapshot_reingestion(c: Composition) -> None:
    """Confirm that Mz does not reingest the entire snapshot on restart by
    revoking its SELECT privileges
    """
    run_testdrive_files(c, "wait-for-snapshot.td", "mysql-disable-select-permission.td")

    restart_mz(c)

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


def _purge_bin_logs(cur: Cursor) -> None:
    cur.execute("FLUSH BINARY LOGS")
    time.sleep(2)
    cur.execute("PURGE BINARY LOGS BEFORE now()")
    cur.execute("FLUSH BINARY LOGS")


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
        "--var=mysql-source-port=3307",
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
        "--var=mysql-source-port=3307",
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


def run_testdrive_files(
    c: Composition, *files: str, mysql_host: str = "mysql", mysql_port: int = 3306
) -> None:
    c.run_testdrive_files(
        f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
        f"--var=mysql-host={mysql_host}",
        f"--var=mysql-port={mysql_port}",
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

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time

import pymysql

from materialize import buildkite
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.alpine import Alpine
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql, create_mysql_server_args
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.toxiproxy import Toxiproxy

SERVICES = [
    Alpine(),
    Materialized(),
    MySql(),
    Toxiproxy(),
    Testdrive(
        no_reset=True,
        default_timeout="300s",
    ),
]


def workflow_default(c: Composition) -> None:
    for name in c.workflows:
        # clear to avoid issues
        c.kill("mysql")
        c.rm("mysql")

        if name == "default":
            continue

        with c.test_case(name):
            c.workflow(name)


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
        Materialized(sanity_restart=False),
    ):
        scenario = backup_restore_mysql
        print(f"--- Running scenario {scenario.__name__}")
        initialize(c)
        scenario(c)
        # No end confirmation here, since we expect the source to be in a bad state


def workflow_bin_log_manipulations(c: Composition) -> None:
    with c.override(
        Materialized(sanity_restart=False),
    ):
        scenarios = [reset_master_gtid, corrupt_bin_log]
        for scenario in scenarios:
            print(f"--- Running scenario {scenario.__name__}")
            initialize(c)
            scenario(c)
            # No end confirmation here, since we expect the source to be in a bad state


def workflow_master_changes(c: Composition) -> None:
    """
    mysql-replica-1 and mysql-replica-2 replicate mysql. The source is attached to mysql-replica-2. mysql is
    killed and mysql-replica-1 becomes the new master. mysql-replica-2 is configured to replicate from mysql-replica-1.
    """

    with c.override(
        Materialized(sanity_restart=False),
        MySql(
            name="mysql-replica-1",
            version=MySql.DEFAULT_VERSION,
            additional_args=create_mysql_server_args(server_id="2", is_master=False),
        ),
        MySql(
            name="mysql-replica-2",
            version=MySql.DEFAULT_VERSION,
            additional_args=create_mysql_server_args(server_id="3", is_master=False),
        ),
    ):
        initialize(c, create_source=False)

        host_data_master = "mysql"
        host_for_mz_source = "mysql-replica-2"

        c.up("mysql-replica-1", "mysql-replica-2")

        # configure mysql-replica-1 to replicate mysql
        run_testdrive_files(
            c,
            f"--var=mysql-replication-master-host={host_data_master}",
            "configure-replica.td",
            mysql_host="mysql-replica-1",
        )
        # configure mysql-replica-2 to replicate mysql
        run_testdrive_files(
            c,
            f"--var=mysql-replication-master-host={host_data_master}",
            "configure-replica.td",
            mysql_host="mysql-replica-2",
        )
        # create source pointing to mysql-replica-2
        run_testdrive_files(
            c,
            f"--var=mysql-source-host={host_for_mz_source}",
            "create-source.td",
        )

        run_testdrive_files(
            c,
            "verify-source-running.td",
        )

        c.kill("mysql")
        host_data_master = "mysql-replica-1"

        # let mysql-replica-2 replicate from mysql-replica-1
        run_testdrive_files(
            c,
            f"--var=mysql-replication-master-host={host_data_master}",
            "configure-replica.td",
            mysql_host=host_for_mz_source,
        )

        run_testdrive_files(
            c,
            "verify-source-running.td",
        )

        # delete rows in mysql-replica-1
        run_testdrive_files(c, "delete-rows-t1.td", mysql_host=host_data_master)

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
        Materialized(sanity_restart=False),
        MySql(
            name="mysql-replica-1",
            version=MySql.DEFAULT_VERSION,
            additional_args=create_mysql_server_args(server_id="2", is_master=False),
        ),
    ):
        initialize(c)

        host_data_master = "mysql"
        host_for_mz_source = "mysql"

        c.up("mysql-replica-1")

        # configure replica
        run_testdrive_files(
            c,
            f"--var=mysql-replication-master-host={host_data_master}",
            "configure-replica.td",
            mysql_host="mysql-replica-1",
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
        run_testdrive_files(
            c,
            f"--var=mysql-source-host={host_for_mz_source}",
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


def workflow_switch_to_lagging_replica(c: Composition) -> None:
    """
    mysql-replica-1 replicates mysql. The source is attached to mysql. mysql-replica-1 is configured to have a
    replication delay and lags behind. mz switches the connection to mysql-replica-1, which exhibits a lower GTID.
    """

    with c.override(
        Materialized(sanity_restart=False),
        MySql(
            name="mysql-replica-1",
            version=MySql.DEFAULT_VERSION,
            additional_args=create_mysql_server_args(server_id="2", is_master=False),
        ),
    ):
        initialize(c)

        host_data_master = "mysql"
        host_for_mz_source = "mysql"
        replica_lag_in_sec = 300

        c.up("mysql-replica-1")

        # configure replica
        run_testdrive_files(
            c,
            f"--var=mysql-replication-master-host={host_data_master}",
            "configure-replica.td",
            mysql_host="mysql-replica-1",
        )

        # give the replica some time to replicate the current state
        time.sleep(3)

        # add delay to the replica
        run_testdrive_files(
            c,
            f"--var=mysql-replication-master-host={host_data_master}",
            f"--var=mysql-replica-delay-in-sec={replica_lag_in_sec}",
            "configure-replica-with-delay.td",
            mysql_host="mysql-replica-1",
        )

        run_testdrive_files(
            c,
            "delete-rows-t1.td",
            "verify-rows-deleted-t1.td",
        )

        # change connection to replica lagging behind
        host_for_mz_source = "mysql-replica-1"
        run_testdrive_files(
            c,
            f"--var=mysql-source-host={host_for_mz_source}",
            "alter-source-connection.td",
        )

        # TODO: #25836 (no detection of lower GTID)
        # run_testdrive_files(
        #     c,
        #     "verify-source-stalled.td",
        # )

        # No end confirmation here, since we expect the source to be in a bad state


def initialize(c: Composition, create_source: bool = True) -> None:
    c.down(destroy_volumes=True)
    c.up("materialized", "mysql", "toxiproxy")

    run_testdrive_files(
        c,
        "configure-toxiproxy.td",
        "configure-mysql.td",
        "populate-tables.td",
        "configure-materialize.td",
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
        # "alter-mz.td",
    )


def restart_mysql_during_snapshot(c: Composition) -> None:
    restart_mysql(c)

    run_testdrive_files(
        c,
        "delete-rows-t1.td",
        "delete-rows-t2.td",
        "alter-table.td",
        # "alter-mz.td",
    )


def restart_mz_during_snapshot(c: Composition) -> None:
    # run_testdrive_files(c, "alter-mz.td")
    restart_mz(c)

    run_testdrive_files(c, "delete-rows-t1.td", "delete-rows-t2.td", "alter-table.td")


def disconnect_mysql_during_replication(c: Composition) -> None:
    run_testdrive_files(
        c,
        "wait-for-snapshot.td",
        "delete-rows-t1.td",
        "delete-rows-t2.td",
        "alter-table.td",
        # "alter-mz.td",
        "toxiproxy-close-connection.td",
        "toxiproxy-restore-connection.td",
    )


def restart_mysql_during_replication(c: Composition) -> None:
    run_testdrive_files(
        c,
        "wait-for-snapshot.td",
        "delete-rows-t1.td",
        "alter-table.td",
        # "alter-mz.td",
    )

    restart_mysql(c)

    run_testdrive_files(c, "delete-rows-t2.td")


def restart_mz_during_replication(c: Composition) -> None:
    run_testdrive_files(
        c,
        "wait-for-snapshot.td",
        "delete-rows-t1.td",
        "alter-table.td",
        # "alter-mz.td",
    )

    restart_mz(c)

    run_testdrive_files(c, "delete-rows-t2.td")


def fix_mysql_schema_while_mz_restarts(c: Composition) -> None:
    run_testdrive_files(
        c,
        "delete-rows-t1.td",
        "delete-rows-t2.td",
        "alter-table.td",
        # "alter-mz.td",
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
        # "alter-mz.td",
    )


def reset_master_gtid(c: Composition) -> None:
    """Confirm behavior after resetting GTID in MySQL"""
    run_testdrive_files(c, "reset-master.td")

    run_testdrive_files(
        c,
        "delete-rows-t1.td",
        "verify-source-stalled.td",
    )


def corrupt_bin_log(c: Composition) -> None:
    """
    Switch off mz, modify data in mysql, and purge the bin-log so that mz hasn't seen all entries in the replication
    stream.
    """

    c.kill("materialized")

    mysql_conn = pymysql.connect(
        host="localhost",
        user="root",
        password=MySql.DEFAULT_ROOT_PASSWORD,
        database="mysql",
        port=c.default_port("mysql"),
    )

    mysql_conn.autocommit(True)
    with mysql_conn.cursor() as cur:
        cur.execute("INSERT INTO public.t1 VALUES (1, 'text')")

        cur.execute("FLUSH BINARY LOGS")
        time.sleep(2)
        cur.execute("PURGE BINARY LOGS BEFORE now()")

        cur.execute("INSERT INTO public.t1 VALUES (2, 'text')")

    c.up("materialized")

    run_testdrive_files(
        c,
        "verify-source-stalled.td",
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

    run_testdrive_files(
        c,
        "delete-rows-t2.td",
        "alter-table.td",
        # "alter-mz.td"
    )


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

    # TODO: #25760: one of the two following commands must succeed
    # run_testdrive_files(c, "verify-rows-after-restore-t1.td")
    # run_testdrive_files(c, "verify-source-failed.td")


def run_testdrive_files(c: Composition, *files: str, mysql_host: str = "mysql") -> None:
    c.run_testdrive_files(
        f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
        f"--var=mysql-host={mysql_host}",
        *files,
    )

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time

from materialize import buildkite
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.alpine import Alpine
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql
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

    shard = buildkite.get_parallelism_index()
    shard_count = buildkite.get_parallelism_count()

    if shard_count > 1:
        scenarios = scenarios[shard::shard_count]
        print(f"Selected scenarios in job with index {shard}")

    print(f"Scenarios: {[s.__name__ for s in scenarios]}")

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


def workflow_reset_gtid(c: Composition) -> None:
    with c.override(
        Materialized(sanity_restart=False),
    ):
        scenario = reset_master_gtid
        print(f"--- Running scenario {scenario.__name__}")
        initialize(c)
        scenario(c)
        # No end confirmation here, since we expect the source to be in a bad state


def initialize(c: Composition) -> None:
    c.down(destroy_volumes=True)
    c.up("materialized", "mysql", "toxiproxy")

    run_testdrive_files(
        c,
        "configure-toxiproxy.td",
        "configure-mysql.td",
        "populate-tables.td",
        "configure-materialize.td",
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

    # TODO: #25760: one of the two following commands must succeed
    # run_testdrive_files(c, "verify-rows-after-restore-t1.td")
    # run_testdrive_files(c, "verify-source-failed.td")


def run_testdrive_files(
    c: Composition,
    *files: str,
) -> None:
    c.run_testdrive_files(
        f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}", *files
    )

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
SQL Server Source tests with interruptions, test that Materialize can recover.
"""

from materialize import buildkite
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.alpine import Alpine
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.toxiproxy import Toxiproxy

SERVICES = [
    Alpine(),
    Mz(app_password=""),
    Materialized(),
    SqlServer(),
    Toxiproxy(),
    Testdrive(no_reset=True, default_timeout="300s"),
]


def workflow_default(c: Composition) -> None:
    def process(name: str) -> None:
        if name == "default":
            return

        # clear to avoid issues
        c.kill("sql-server")
        c.rm("sql-server")

        with c.test_case(name):
            c.workflow(name)

    c.test_parts(list(c.workflows.keys()), process)


def workflow_disruptions(c: Composition) -> None:
    """Test Sql Server direct replication's failure handling by
    disrupting replication at various stages using Toxiproxy or service restarts
    """

    # TODO: most of these should likely be converted to cluster tests

    scenarios = [
        sql_server_out_of_disk_space,
        disconnect_sql_server_during_snapshot,
        disconnect_sql_server_during_replication,
        restart_sql_server_during_snapshot,
        restart_mz_during_snapshot,
        restart_sql_server_during_replication,
        restart_mz_during_replication,
        fix_sql_server_schema_while_mz_restarts,
        verify_no_snapshot_reingestion,
    ]

    scenarios = buildkite.shard_list(scenarios, lambda s: s.__name__)
    print(
        f"Scenarios in shard with index {buildkite.get_parallelism_index()}: {[s.__name__ for s in scenarios]}"
    )

    for scenario in scenarios:
        overrides = (
            [SqlServer(volumes=["sourcedata_512Mb:/var/lib/mssql/data"])]
            if scenario == sql_server_out_of_disk_space
            else []
        )
        with c.override(*overrides):
            print(
                f"--- Running scenario {scenario.__name__} with overrides: {overrides}"
            )
            c.override_current_testcase_name(
                f"Scenario '{scenario.__name__}' of workflow_disruptions"
            )
            initialize(c)
            scenario(c)
            end(c)


def workflow_backup_restore(c: Composition) -> None:
    scenarios = [
        backup_restore_sql_server,
    ]
    scenarios = buildkite.shard_list(scenarios, lambda s: s.__name__)
    print(
        f"Scenarios in shard with index {buildkite.get_parallelism_index()}: {[s.__name__ for s in scenarios]}"
    )

    with c.override(
        Materialized(sanity_restart=False),
        Alpine(volumes=["mssqldata:/var/lib/mssql/data", "tmp:/scratch"]),
        SqlServer(volumes=["mssqldata:/var/lib/mssql/data", "tmp:/scratch"]),
    ):
        for scenario in scenarios:
            print(f"--- Running scenario {scenario.__name__}")
            initialize(c)
            scenario(c)
            # No end confirmation here, since we expect the source to be in a bad state


def initialize(c: Composition) -> None:
    c.down(destroy_volumes=True)
    c.up("materialized", "sql-server", "toxiproxy")

    c.run_testdrive_files(
        "configure-toxiproxy.td",
        "populate-tables.td",
        "configure-sql-server.td",
        "configure-materialize.td",
    )


def restart_sql_server(c: Composition) -> None:
    c.kill("sql-server")
    c.up("sql-server")


def restart_mz(c: Composition) -> None:
    c.kill("materialized")
    c.up("materialized")


def end(c: Composition) -> None:
    """Validate the data at the end."""
    c.run_testdrive_files("verify-data.td", "cleanup.td")


def disconnect_sql_server_during_snapshot(c: Composition) -> None:
    c.run_testdrive_files(
        "toxiproxy-close-connection.td",
        "toxiproxy-restore-connection.td",
        "delete-rows-t1.td",
        "delete-rows-t2.td",
        "alter-table.td",
        "alter-mz.td",
    )


def restart_sql_server_during_snapshot(c: Composition) -> None:
    restart_sql_server(c)

    c.run_testdrive_files(
        "delete-rows-t1.td",
        "delete-rows-t2.td",
        "alter-table.td",
        "alter-mz.td",
    )


def restart_mz_during_snapshot(c: Composition) -> None:
    c.run_testdrive_files("alter-mz.td")
    restart_mz(c)

    c.run_testdrive_files("delete-rows-t1.td", "delete-rows-t2.td", "alter-table.td")


def disconnect_sql_server_during_replication(c: Composition) -> None:
    c.run_testdrive_files(
        "wait-for-snapshot.td",
        "delete-rows-t1.td",
        "delete-rows-t2.td",
        "alter-table.td",
        "alter-mz.td",
        "toxiproxy-close-connection.td",
        "toxiproxy-restore-connection.td",
    )


def restart_sql_server_during_replication(c: Composition) -> None:
    c.run_testdrive_files(
        "wait-for-snapshot.td",
        "delete-rows-t1.td",
        "alter-table.td",
        "alter-mz.td",
    )

    restart_sql_server(c)

    c.run_testdrive_files("delete-rows-t2.td")


def restart_mz_during_replication(c: Composition) -> None:
    c.run_testdrive_files(
        "wait-for-snapshot.td",
        "delete-rows-t1.td",
        "alter-table.td",
        "alter-mz.td",
    )

    restart_mz(c)

    c.run_testdrive_files("delete-rows-t2.td")


def fix_sql_server_schema_while_mz_restarts(c: Composition) -> None:
    c.run_testdrive_files(
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
    c.run_testdrive_files(
        "wait-for-snapshot.td", "sql-server-disable-select-permission.td"
    )

    restart_mz(c)

    c.run_testdrive_files(
        "delete-rows-t1.td",
        "delete-rows-t2.td",
        "alter-table.td",
        "alter-mz.td",
    )


def sql_server_out_of_disk_space(c: Composition) -> None:
    # c.run_testdrive_files(
    #    "wait-for-snapshot.td",
    #    "delete-rows-t1.td",
    # )

    # fill_file = "/var/lib/mssql/data/fill_file"
    # c.exec(
    #    "sqlcmd",
    #    "bash",
    #    "-c",
    #    f"dd if=/dev/zero of={fill_file} bs=1024 count=$[1024*512] || true",
    # )
    # print("Sleeping for 30 seconds ...")
    # time.sleep(30)
    # c.exec("postgres", "bash", "-c", f"rm {fill_file}")

    # c.run_testdrive_files("delete-rows-t2.td", "alter-table.td", "alter-mz.td")
    raise Exception("unimplemented")


def backup_restore_sql_server(c: Composition) -> None:
    # c.exec(
    #    "postgres",
    #    "bash",
    #    "-c",
    #    "echo 'local replication all trust\n' >> /share/conf/pg_hba.conf",
    # )
    ## Tell postgres to reload config
    # c.kill("postgres", signal="HUP", wait=False)

    ## Backup postgres, wait for completion
    # backup_dir = "/scratch/backup"
    # c.exec(
    #    "postgres",
    #    "bash",
    #    "-c",
    #    f"mkdir {backup_dir} && chown -R postgres:postgres {backup_dir}",
    # )
    # c.exec(
    #    "postgres",
    #    "pg_basebackup",
    #    "-U",
    #    "postgres",
    #    "-X",
    #    "stream",
    #    "-c",
    #    "fast",
    #    "-D",
    #    backup_dir,
    # )
    # c.run_testdrive_files("delete-rows-t1.td")

    ## Stop postgres service
    # c.stop("postgres")

    ## Perform a point-in-time recovery from the backup to postgres
    # c.run("alpine", "/bin/sh", "-c", "rm -rf /var/lib/postgresql/data/*")
    # c.run("alpine", "/bin/sh", "-c", f"cp -r {backup_dir}/* /var/lib/postgresql/data/")
    # c.run("alpine", "/bin/sh", "-c", "touch /var/lib/postgresql/data/recovery.signal")
    # c.run(
    #    "alpine",
    #    "/bin/sh",
    #    "-c",
    #    "echo \"restore_command 'cp /var/lib/mssql/data/pg_wal/%f %p'\n\" >> /var/lib/mssql/data/postgresql.conf",
    # )

    ## Wait for postgres to become usable again
    # c.up("sql-server")
    # c.run_testdrive_files("verify-sql-server-select.td")

    ## Check state of the postgres source
    # c.run_testdrive_files("verify-source-failed.td")
    raise Exception("unimplemented")

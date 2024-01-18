# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import time

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.alpine import Alpine
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.toxiproxy import Toxiproxy

SERVICES = [
    Alpine(),
    Materialized(),
    Postgres(),
    Toxiproxy(),
    Testdrive(no_reset=True, default_timeout="300s"),
]


def workflow_default(c: Composition) -> None:
    """Test Postgres direct replication's failure handling by
    disrupting replication at various stages using Toxiproxy or service restarts
    """

    # TODO: most of these should likely be converted to cluster tests

    scenarios = [
        pg_out_of_disk_space,
        disconnect_pg_during_snapshot,
        disconnect_pg_during_replication,
        restart_pg_during_snapshot,
        restart_mz_during_snapshot,
        restart_pg_during_replication,
        restart_mz_during_replication,
        fix_pg_schema_while_mz_restarts,
        verify_no_snapshot_reingestion,
    ]

    parallel_job_index = int(os.environ.get("BUILDKITE_PARALLEL_JOB", 0))
    parallel_job_count = int(os.environ.get("BUILDKITE_PARALLEL_JOB_COUNT", 1))

    if parallel_job_count > 1:
        scenarios = scenarios[parallel_job_index::parallel_job_count]
        print(f"Selected scenarios in job with index {parallel_job_index}")

    print(f"Scenarios: {[s.__name__ for s in scenarios]}")

    for scenario in scenarios:
        overrides = (
            [Postgres(volumes=["pgdata_512Mb:/var/lib/postgresql/data"])]
            if scenario == pg_out_of_disk_space
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
        Alpine(volumes=["pgdata:/var/lib/postgresql/data", "tmp:/scratch"]),
        Postgres(volumes=["pgdata:/var/lib/postgresql/data", "tmp:/scratch"]),
    ):
        for scenario in [
            backup_restore_pg,
        ]:
            print(f"--- Running scenario {scenario.__name__}")
            initialize(c)
            scenario(c)
            # No end confirmation here, since we expect the source to be in a bad state


def initialize(c: Composition) -> None:
    c.down(destroy_volumes=True)
    c.up("materialized", "postgres", "toxiproxy")

    c.run(
        "testdrive",
        "configure-toxiproxy.td",
        "populate-tables.td",
        "configure-postgres.td",
        "configure-materalize.td",
    )


def restart_pg(c: Composition) -> None:
    c.kill("postgres")
    c.up("postgres")


def restart_mz(c: Composition) -> None:
    c.kill("materialized")
    c.up("materialized")


def end(c: Composition) -> None:
    """Validate the data at the end."""
    c.run("testdrive", "verify-data.td", "cleanup.td")


def disconnect_pg_during_snapshot(c: Composition) -> None:
    c.run(
        "testdrive",
        "toxiproxy-close-connection.td",
        "toxiproxy-restore-connection.td",
        "delete-rows-t1.td",
        "delete-rows-t2.td",
        "alter-table.td",
        "alter-mz.td",
    )


def restart_pg_during_snapshot(c: Composition) -> None:
    restart_pg(c)

    c.run(
        "testdrive",
        "delete-rows-t1.td",
        "delete-rows-t2.td",
        "alter-table.td",
        "alter-mz.td",
    )


def restart_mz_during_snapshot(c: Composition) -> None:
    c.run("testdrive", "alter-mz.td")
    restart_mz(c)

    c.run("testdrive", "delete-rows-t1.td", "delete-rows-t2.td", "alter-table.td")


def disconnect_pg_during_replication(c: Composition) -> None:
    c.run(
        "testdrive",
        "wait-for-snapshot.td",
        "delete-rows-t1.td",
        "delete-rows-t2.td",
        "alter-table.td",
        "alter-mz.td",
        "toxiproxy-close-connection.td",
        "toxiproxy-restore-connection.td",
    )


def restart_pg_during_replication(c: Composition) -> None:
    c.run(
        "testdrive",
        "wait-for-snapshot.td",
        "delete-rows-t1.td",
        "alter-table.td",
        "alter-mz.td",
    )

    restart_pg(c)

    c.run("testdrive", "delete-rows-t2.td")


def restart_mz_during_replication(c: Composition) -> None:
    c.run(
        "testdrive",
        "wait-for-snapshot.td",
        "delete-rows-t1.td",
        "alter-table.td",
        "alter-mz.td",
    )

    restart_mz(c)

    c.run("testdrive", "delete-rows-t2.td")


def fix_pg_schema_while_mz_restarts(c: Composition) -> None:
    c.run(
        "testdrive",
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
    c.run("testdrive", "wait-for-snapshot.td", "postgres-disable-select-permission.td")

    restart_mz(c)

    c.run(
        "testdrive",
        "delete-rows-t1.td",
        "delete-rows-t2.td",
        "alter-table.td",
        "alter-mz.td",
    )


def pg_out_of_disk_space(c: Composition) -> None:
    c.run(
        "testdrive",
        "wait-for-snapshot.td",
        "delete-rows-t1.td",
    )

    fill_file = "/var/lib/postgresql/data/fill_file"
    c.exec(
        "postgres",
        "bash",
        "-c",
        f"dd if=/dev/zero of={fill_file} bs=1024 count=$[1024*512] || true",
    )
    print("Sleeping for 30 seconds ...")
    time.sleep(30)
    c.exec("postgres", "bash", "-c", f"rm {fill_file}")

    c.run("testdrive", "delete-rows-t2.td", "alter-table.td", "alter-mz.td")


def backup_restore_pg(c: Composition) -> None:
    c.exec(
        "postgres",
        "bash",
        "-c",
        "echo 'local replication all trust\n' >> /share/conf/pg_hba.conf",
    )
    # Tell postgres to reload config
    c.kill("postgres", signal="HUP", wait=False)

    # Backup postgres, wait for completion
    backup_dir = "/scratch/backup"
    c.exec(
        "postgres",
        "bash",
        "-c",
        f"mkdir {backup_dir} && chown -R postgres:postgres {backup_dir}",
    )
    c.exec(
        "postgres",
        "pg_basebackup",
        "-U",
        "postgres",
        "-X",
        "stream",
        "-c",
        "fast",
        "-D",
        backup_dir,
    )
    c.run("testdrive", "delete-rows-t1.td")

    # Stop postgres service
    c.stop("postgres")

    # Perform a point-in-time recovery from the backup to postgres
    c.run("alpine", "/bin/sh", "-c", "rm -rf /var/lib/postgresql/data/*")
    c.run("alpine", "/bin/sh", "-c", f"cp -r {backup_dir}/* /var/lib/postgresql/data/")
    c.run("alpine", "/bin/sh", "-c", "touch /var/lib/postgresql/data/recovery.signal")
    c.run(
        "alpine",
        "/bin/sh",
        "-c",
        "echo \"restore_command 'cp /var/lib/postgresql/data/pg_wal/%f %p'\n\" >> /var/lib/postgresql/data/postgresql.conf",
    )

    # Wait for postgres to become usable again
    c.up("postgres")
    c.run("testdrive", "verify-postgres-select.td")

    # Check state of the postgres source
    c.run("testdrive", "verify-source-failed.td")

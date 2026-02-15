# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Postgres source tests with interruptions, test that Materialize can recover.
"""

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.alpine import Alpine
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.toxiproxy import Toxiproxy
from materialize.pg_cdc_resumption import (
    backup_restore_pg,
    disconnect_pg_during_replication,
    disconnect_pg_during_snapshot,
    end,
    fix_pg_schema_while_mz_restarts,
    initialize,
    pg_out_of_disk_space,
    restart_mz_during_replication,
    restart_mz_during_snapshot,
    restart_pg_during_replication,
    restart_pg_during_snapshot,
    verify_no_snapshot_reingestion,
)

SERVICES = [
    Alpine(),
    Mz(app_password=""),
    Materialized(default_replication_factor=2),
    Postgres(),
    Toxiproxy(),
    Testdrive(no_reset=True, default_timeout="300s"),
]


def _kill_postgres(c: Composition) -> None:
    # clear to avoid issues
    c.kill("postgres")
    c.rm("postgres")


def workflow_default(c: Composition) -> None:
    c.run_all_workflows(between_workflows=_kill_postgres)


def workflow_disruptions(c: Composition) -> None:
    """Test Postgres direct replication's failure handling by
    disrupting replication at various stages using Toxiproxy or service restarts
    """

    # TODO: most of these should likely be converted to cluster tests

    c.shard_and_run_scenarios(
        [
            pg_out_of_disk_space,
            disconnect_pg_during_snapshot,
            disconnect_pg_during_replication,
            restart_pg_during_snapshot,
            restart_mz_during_snapshot,
            restart_pg_during_replication,
            restart_mz_during_replication,
            fix_pg_schema_while_mz_restarts,
            verify_no_snapshot_reingestion,
        ],
        init=initialize,
        end=end,
        get_overrides=lambda s: (
            [Postgres(volumes=["sourcedata_512Mb:/var/lib/postgresql/data"])]
            if s == pg_out_of_disk_space
            else []
        ),
        testcase_name_prefix="Scenario of workflow_disruptions",
    )


def workflow_backup_restore(c: Composition) -> None:
    with c.override(
        Materialized(sanity_restart=False, default_replication_factor=2),
        Alpine(volumes=["pgdata:/var/lib/postgresql/data", "tmp:/scratch"]),
        Postgres(volumes=["pgdata:/var/lib/postgresql/data", "tmp:/scratch"]),
    ):
        c.shard_and_run_scenarios(
            [backup_restore_pg],
            init=initialize,
        )

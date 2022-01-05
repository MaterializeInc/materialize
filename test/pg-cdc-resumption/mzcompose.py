# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition
from materialize.mzcompose.services import Materialized, Postgres, Testdrive, Toxiproxy

SERVICES = [
    Materialized(),
    Postgres(),
    Toxiproxy(),
    Testdrive(
        no_reset=True,
        default_timeout="60s",
        depends_on=["materialized", "postgres", "toxiproxy"],
    ),
]


def workflow_pg_cdc_resumption(c: Composition) -> None:
    """Test Postgres direct replication's failure handling by
    disrupting replication at various stages using Toxiproxy or service restarts
    """

    # We run configure-postgres.td only once for all workflows as
    # it contains CREATE USER that is not indempotent.
    c.down(volumes=True)
    c.run("testdrive-svc", "configure-postgres.td")

    for scenario in [
        disconnect_pg_during_snapshot,
        disconnect_pg_during_replication,
        restart_pg_during_snapshot,
        restart_mz_during_snapshot,
        restart_pg_during_replication,
        restart_mz_during_replication,
    ]:
        c.run(
            "testdrive-svc",
            "configure-toxiproxy.td",
            "populate-tables.td",
            "configure-materalize.td",
        )
        scenario(c)
        c.run("testdrive-svc", "verify-data.td", "toxiproxy-remove.td")


def disconnect_pg_during_snapshot(c: Composition) -> None:
    c.run(
        "testdrive-svc",
        "toxiproxy-close-connection.td",
        "toxiproxy-restore-connection.td",
        "delete-rows-t1.td",
        "delete-rows-t2.td",
    )


def restart_pg_during_snapshot(c: Composition) -> None:
    c.kill("postgres")
    c.run(
        "testdrive-svc",
        "delete-rows-t1.td",
        "delete-rows-t2.td",
    )


def restart_mz_during_snapshot(c: Composition) -> None:
    c.kill("materialized")
    c.run(
        "testdrive-svc",
        "delete-rows-t1.td",
        "delete-rows-t2.td",
    )


def disconnect_pg_during_replication(c: Composition) -> None:
    c.run(
        "testdrive-svc",
        "wait-for-snapshot.td",
        "delete-rows-t1.td",
        "delete-rows-t2.td",
        "toxiproxy-close-connection.td",
        "toxiproxy-restore-connection.td",
    )


def restart_pg_during_replication(c: Composition) -> None:
    c.run("testdrive-svc", "wait-for-snapshot.td", "delete-rows-t1.td")
    c.kill("postgres")
    c.run("testdrive-svc", "delete-rows-t2.td")


def restart_mz_during_replication(c: Composition) -> None:
    c.run("testdrive-svc", "wait-for-snapshot.td", "delete-rows-t1.td")
    c.kill("materialized")
    c.run("testdrive-svc", "delete-rows-t2.td")

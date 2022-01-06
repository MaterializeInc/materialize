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

services = [
    Materialized(),
    Postgres(),
    Toxiproxy(),
    Testdrive(no_reset=True, default_timeout="60s"),
]


def workflow_pg_cdc_resumption(c: Composition) -> None:
    """Test Postgres direct replication's failure handling by
    disrupting replication at various stages using Toxiproxy or service restarts
    """

    initialize(c)

    for scenario in [
        disconnect_pg_during_snapshot,
        disconnect_pg_during_replication,
        restart_pg_during_snapshot,
        restart_mz_during_snapshot,
        restart_pg_during_replication,
        restart_mz_during_replication,
    ]:
        begin(c)
        scenario(c)
        end(c)


def initialize(c: Composition) -> None:
    c.start_services(services=["materialized", "postgres", "toxiproxy"])

    c.wait_for_mz()
    c.wait_for_postgres()
    c.wait_for_tcp(host="toxiproxy", port=8474)

    # We run configure-postgres.td only once for all workflows as
    # it contains CREATE USER that is not indempotent

    c.run_service(service="testdrive-svc", command="configure-postgres.td")


def restart_pg(c: Composition) -> None:
    c.kill_services(services=["postgres"])
    c.start_services(services=["postgres"])
    c.wait_for_postgres()


def restart_mz(c: Composition) -> None:
    c.kill_services(services=["materialized"])
    c.start_services(services=["materialized"])
    c.wait_for_mz()


def begin(c: Composition) -> None:
    """Configure Toxiproxy and Mz and populate initial data"""

    c.run_service(
        service="testdrive-svc",
        command=" ".join(
            ["configure-toxiproxy.td", "populate-tables.td", "configure-materalize.td"]
        ),
    )


def end(c: Composition) -> None:
    """Validate the data at the end and reset Toxiproxy"""
    c.run_service(
        service="testdrive-svc",
        command=" ".join(["verify-data.td", "toxiproxy-remove.td"]),
    )


def disconnect_pg_during_snapshot(c: Composition) -> None:
    c.run_service(
        service="testdrive-svc",
        command=" ".join(
            [
                "toxiproxy-close-connection.td",
                "toxiproxy-restore-connection.td",
                "delete-rows-t1.td",
                "delete-rows-t2.td",
            ]
        ),
    )


def restart_pg_during_snapshot(c: Composition) -> None:
    restart_pg(c)

    c.run_service(
        service="testdrive-svc",
        command=" ".join(
            [
                "delete-rows-t1.td",
                "delete-rows-t2.td",
            ]
        ),
    )


def restart_mz_during_snapshot(c: Composition) -> None:
    restart_mz(c)

    c.run_service(
        service="testdrive-svc",
        command=" ".join(
            [
                "delete-rows-t1.td",
                "delete-rows-t2.td",
            ]
        ),
    )


def disconnect_pg_during_replication(c: Composition) -> None:
    c.run_service(
        service="testdrive-svc",
        command=" ".join(
            [
                "wait-for-snapshot.td",
                "delete-rows-t1.td",
                "delete-rows-t2.td",
                "toxiproxy-close-connection.td",
                "toxiproxy-restore-connection.td",
            ]
        ),
    )


def restart_pg_during_replication(c: Composition) -> None:
    c.run_service(
        service="testdrive-svc",
        command=" ".join(["wait-for-snapshot.td", "delete-rows-t1.td"]),
    )

    restart_pg(c)

    c.run_service(service="testdrive-svc", command="delete-rows-t2.td")


def restart_mz_during_replication(c: Composition) -> None:
    c.run_service(
        service="testdrive-svc",
        command=" ".join(["wait-for-snapshot.td", "delete-rows-t1.td"]),
    )

    restart_mz(c)

    c.run_service(service="testdrive-svc", command="delete-rows-t2.td")

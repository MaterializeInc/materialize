# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Workflow
from materialize.mzcompose.services import Materialized, Postgres, Testdrive, Toxiproxy

services = [
    Materialized(),
    Postgres(),
    Toxiproxy(),
    Testdrive(no_reset=True, default_timeout="60s"),
]


def workflow_pg_cdc_resumption(w: Workflow):
    """Test Postgres direct replication's failure handling by
    disrupting replication at various stages using Toxiproxy or service restarts
    """

    initialize(w)

    for scenario in [
        disconnect_pg_during_snapshot,
        disconnect_pg_during_replication,
        restart_pg_during_snapshot,
        restart_mz_during_snapshot,
        restart_pg_during_replication,
        restart_mz_during_replication,
    ]:
        begin(w)
        scenario(w)
        end(w)


def initialize(w: Workflow):
    w.start_services(services=["materialized", "postgres", "toxiproxy"])

    w.wait_for_mz()
    w.wait_for_postgres()
    w.wait_for_tcp(host="toxiproxy", port=8474)

    # We run configure-postgres.td only once for all workflows as
    # it contains CREATE USER that is not indempotent

    w.run_service(service="testdrive-svc", command="configure-postgres.td")


def restart_pg(w: Workflow):
    w.kill_services(services=["postgres"])
    w.start_services(services=["postgres"])
    w.wait_for_postgres()


def restart_mz(w: Workflow):
    w.kill_services(services=["materialized"])
    w.start_services(services=["materialized"])
    w.wait_for_mz()


def begin(w: Workflow):
    """Configure Toxiproxy and Mz and populate initial data"""

    w.run_service(
        service="testdrive-svc",
        command=" ".join(
            ["configure-toxiproxy.td", "populate-tables.td", "configure-materalize.td"]
        ),
    )


def end(w: Workflow):
    """Validate the data at the end and reset Toxiproxy"""
    w.run_service(
        service="testdrive-svc",
        command=" ".join(["verify-data.td", "toxiproxy-remove.td"]),
    )


def disconnect_pg_during_snapshot(w: Workflow):
    w.run_service(
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


def restart_pg_during_snapshot(w: Workflow):
    restart_pg(w)

    w.run_service(
        service="testdrive-svc",
        command=" ".join(
            [
                "delete-rows-t1.td",
                "delete-rows-t2.td",
            ]
        ),
    )


def restart_mz_during_snapshot(w: Workflow):
    restart_mz(w)

    w.run_service(
        service="testdrive-svc",
        command=" ".join(
            [
                "delete-rows-t1.td",
                "delete-rows-t2.td",
            ]
        ),
    )


def disconnect_pg_during_replication(w: Workflow):
    w.run_service(
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


def restart_pg_during_replication(w: Workflow):
    w.run_service(
        service="testdrive-svc",
        command=" ".join(["wait-for-snapshot.td", "delete-rows-t1.td"]),
    )

    restart_pg(w)

    w.run_service(service="testdrive-svc", command="delete-rows-t2.td")


def restart_mz_during_replication(w: Workflow):
    w.run_service(
        service="testdrive-svc",
        command=" ".join(["wait-for-snapshot.td", "delete-rows-t1.td"]),
    )

    restart_mz(w)

    w.run_service(service="testdrive-svc", command="delete-rows-t2.td")

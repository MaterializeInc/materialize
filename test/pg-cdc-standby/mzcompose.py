# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Postgres source against a physical *standby*.

Exercises "logical decoding on standby" (PostgreSQL 16+): a primary streams its
WAL to a physical replica via streaming replication, and Materialize creates a
logical replication slot on the *replica* and decodes changes there. Data
originates on the primary, flows to the standby physically, and is decoded
logically for Materialize -- the primary is never touched by Materialize.

The notable wrinkle is that creating a logical slot on a standby blocks until
the primary emits a standby-snapshot (RUNNING_XACTS) WAL record. Materialize
creates its slots synchronously, so without help the source would hang. We run
a background thread that calls pg_log_standby_snapshot() on the primary to
unblock slot creation; see nudge_standby_snapshots().
"""

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from textwrap import dedent

import psycopg

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.testdrive import Testdrive

# Standby's data directory is seeded by pg_basebackup before postgres starts, so
# it needs to survive between the `run` (basebackup) and `up` (serve) steps.
VOLUMES = {"standbydata": None}

SERVICES = [
    Mz(app_password=""),
    Materialized(
        additional_system_parameter_defaults={
            "log_filter": "mz_storage::source::postgres=trace,info",
            # Purification must record that the upstream server is in recovery,
            # so that WAL probes use pg_last_wal_replay_lsn() instead of
            # pg_current_wal_lsn(), which errors on a standby. Without this the
            # source dataflow restarts forever and never produces data.
            "enable_postgres_physical_replica_detection": "true",
        },
        default_replication_factor=1,
    ),
    # no_reset so the source created in one testdrive invocation survives into
    # the next; c.down at the start of the workflow gives us a clean slate.
    Testdrive(no_reset=True, default_timeout="120s"),
    Postgres(name="pg-primary"),
    Postgres(
        name="pg-standby",
        extra_command=["-c", "hot_standby_feedback=on"],
        volumes=["standbydata:/var/lib/postgresql/data"],
    ),
]


def _pg_connect(c: Composition, service: str) -> psycopg.Connection:
    return psycopg.connect(
        host="localhost",
        user="postgres",
        password="postgres",
        port=c.default_port(service),
        autocommit=True,
    )


def _allow_replication(c: Composition, service: str) -> None:
    """Permit replication connections (basebackup, streaming, logical decoding).

    The baked pg_hba.conf has no `replication` entry, and `host all` does not
    match replication connections in PostgreSQL.
    """
    c.exec(
        service,
        "bash",
        "-c",
        "echo 'host replication all all trust' >> /share/conf/pg_hba.conf",
    )
    c.exec(service, "psql", "-U", "postgres", "-c", "SELECT pg_reload_conf();")


def _wait_for_standby(c: Composition) -> None:
    """Block until the standby is accepting connections and is in recovery."""
    last_err: Exception | None = None
    for _ in range(60):
        try:
            conn = _pg_connect(c, "pg-standby")
            in_recovery = conn.execute("SELECT pg_is_in_recovery()").fetchone()[0]
            conn.close()
            if in_recovery:
                return
        except Exception as e:
            last_err = e
        threading.Event().wait(1)
    raise RuntimeError(f"standby did not reach recovery state: {last_err}")


@contextmanager
def nudge_standby_snapshots(c: Composition) -> Iterator[None]:
    """Periodically emit a standby-snapshot record on the primary.

    Creating a logical slot on a standby blocks until a RUNNING_XACTS record is
    replayed. Materialize creates slots synchronously (one persistent slot plus
    a temporary per-worker snapshot slot), so we keep nudging for the whole
    duration that the source is being set up and verified.
    """
    stop = threading.Event()

    def run() -> None:
        conn = _pg_connect(c, "pg-primary")
        try:
            while not stop.is_set():
                try:
                    conn.execute("SELECT pg_log_standby_snapshot()")
                except Exception:
                    pass
                stop.wait(1)
        finally:
            conn.close()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join()


def setup_standby(c: Composition) -> None:
    """Bring up the primary, seed it, then clone it into a physical standby."""
    c.down(destroy_volumes=True)

    # testdrive connects to materialized at startup, so it must be up before we
    # run any testdrive files -- even ones that only touch Postgres.
    c.up("materialized", "pg-primary")
    _allow_replication(c, "pg-primary")
    c.run_testdrive_files("configure-primary.td")

    # Clone the primary into the standby's data volume *before* postgres starts.
    # `-R` writes standby.signal + primary_conninfo so it comes up in recovery.
    # Runs as root to chown the fresh volume, then steps down to postgres since
    # the server refuses a data dir it does not own.
    c.run(
        "pg-standby",
        "-c",
        dedent("""
            set -e
            mkdir -p "$PGDATA"
            chown postgres:postgres "$PGDATA"
            chmod 700 "$PGDATA"
            rm -rf "$PGDATA"/* || true
            gosu postgres pg_basebackup -h pg-primary -U postgres -D "$PGDATA" -Fp -Xs -R -P
            echo basebackup-done
            """),
        entrypoint="bash",
    )

    c.up("pg-standby")
    _wait_for_standby(c)
    # The standby reads its own pg_hba.conf; allow Materialize's replication
    # connection (slot creation + START_REPLICATION) to it.
    _allow_replication(c, "pg-standby")


def workflow_default(c: Composition) -> None:
    setup_standby(c)

    with nudge_standby_snapshots(c):
        # Source creation + initial snapshot both create slots on the standby,
        # which block until a snapshot record arrives -- hence the nudger.
        c.run_testdrive_files("configure-materialize.td", "verify-snapshot.td")

        # Changes made on the primary must flow primary -> standby (physical) ->
        # Materialize (logical decoding on the standby).
        c.run_testdrive_files("insert-update-delete.td", "verify-cdc.td")

    _verify_reading_from_standby(c)


def _verify_reading_from_standby(c: Composition) -> None:
    """Prove Materialize really decoded from the standby, not the primary."""
    conn = _pg_connect(c, "pg-standby")
    try:
        in_recovery = conn.execute("SELECT pg_is_in_recovery()").fetchone()[0]
        assert in_recovery, "expected pg-standby to be a replica in recovery"

        logical_slots = conn.execute(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_type = 'logical'"
        ).fetchone()[0]
        assert (
            logical_slots >= 1
        ), f"expected a logical slot on the standby, found {logical_slots}"
    finally:
        conn.close()

    # The primary must NOT carry Materialize's logical slot.
    conn = _pg_connect(c, "pg-primary")
    try:
        in_recovery = conn.execute("SELECT pg_is_in_recovery()").fetchone()[0]
        assert not in_recovery, "expected pg-primary to be the primary"
    finally:
        conn.close()

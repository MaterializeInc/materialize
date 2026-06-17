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
from typing import Any, LiteralString

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


def _query_scalar(conn: psycopg.Connection, query: LiteralString) -> Any:
    row = conn.execute(query).fetchone()
    assert row is not None, f"query returned no rows: {query}"
    return row[0]


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
            in_recovery = _query_scalar(conn, "SELECT pg_is_in_recovery()")
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


def _basebackup_standby(c: Composition) -> None:
    """Clone the primary into the standby's data volume via pg_basebackup.

    Runs as a one-off container *before* the standby's postgres serves, so the
    data directory is fully seeded first. `-R` writes standby.signal +
    primary_conninfo so it comes up in recovery. Runs as root to chown the fresh
    volume, then steps down to postgres since the server refuses a data dir it
    does not own.

    A base backup never copies replication slots, so any logical slot that lived
    on the standby (including one Materialize created) is gone afterwards.
    """
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


def setup_standby(c: Composition) -> None:
    """Bring up the primary, seed it, then clone it into a physical standby."""
    c.down(destroy_volumes=True)

    # testdrive connects to materialize at startup, so it must be up before we
    # run any testdrive files -- even ones that only touch Postgres.
    c.up("materialized", "pg-primary")
    _allow_replication(c, "pg-primary")
    c.run_testdrive_files("configure-primary.td")

    _basebackup_standby(c)

    c.up("pg-standby")
    _wait_for_standby(c)
    # The standby reads its own pg_hba.conf; allow Materialize's replication
    # connection (slot creation + START_REPLICATION) to it.
    _allow_replication(c, "pg-standby")


def rebuild_standby(c: Composition) -> None:
    """Re-seed the standby from a now-advanced primary, as an operator would when
    rebuilding a replica with a fresh base backup.

    This is the destructive scenario: the base backup destroys the logical slot
    Materialize created on the standby. When the source resumes it recreates the
    slot at the rebuilt standby's *current* LSN -- which we deliberately push far
    ahead of the LSN Materialize has durably committed. The slot can no longer
    serve the data Materialize still needs, so the source stalls permanently.
    """
    # Stop the serving standby so we can overwrite its data directory with future
    # data.
    c.kill("pg-standby")
    # Advance the primary well past Materialize's committed LSN so the rebuilt
    # standby's fresh slot starts from a definitively later position.
    conn = _pg_connect(c, "pg-primary")
    try:
        conn.execute(
            "INSERT INTO t SELECT g, 'post-rebuild' FROM generate_series(2001, 5000) AS g"
        )
        # Force a WAL rotation so the base backup's start LSN is clearly ahead.
        conn.execute("SELECT pg_switch_wal()")
    finally:
        conn.close()

    _basebackup_standby(c)
    c.up("pg-standby")
    _wait_for_standby(c)
    _allow_replication(c, "pg-standby")


def promote_standby(c: Composition) -> None:
    """Promote the physical standby to a primary, as an operator would during a
    failover.

    Promotion flips pg_is_in_recovery() to false and switches the WAL timeline.
    The source was created against this node *as a standby*, so once it is no
    longer one, logical decoding can no longer safely continue: a periodic
    background check notices the changed physical-replica status while the stream
    is live and stalls the source permanently -- no restart required.
    """
    conn = _pg_connect(c, "pg-standby")
    try:
        # pg_promote() waits (default 60s) for promotion to complete and returns
        # whether it succeeded.
        promoted = _query_scalar(conn, "SELECT pg_promote()")
        assert promoted, "pg_promote() did not complete"
        in_recovery = _query_scalar(conn, "SELECT pg_is_in_recovery()")
        assert not in_recovery, "expected the promoted standby to leave recovery"
    finally:
        conn.close()


def workflow_promotion(c: Composition) -> None:
    """Promoting the standby to a primary stalls the source.

    Deliberately kept on its own clean slate -- setup_standby() tears everything
    down first -- so this scenario is isolated from workflow_default's CDC,
    rebuild, and drop assertions and cannot regress them. Promotion is also
    irreversible, so it must run against a freshly built standby.
    """
    setup_standby(c)

    with nudge_standby_snapshots(c):
        # Source creation + initial snapshot create slots on the standby, which
        # block until a standby-snapshot record arrives -- hence the nudger.
        c.run_testdrive_files("configure-materialize.td", "verify-snapshot.td")

    # Confirm we really decoded from the standby before promoting it; otherwise a
    # subsequent stall would not prove anything about promotion.
    _verify_reading_from_standby(c)

    # Promotion needs no nudger: the live physical-replica check stalls the source
    # without (re)creating a slot, so nothing blocks on a standby snapshot.
    promote_standby(c)
    c.run_testdrive_files("test-standby-promotion.td")


def workflow_default(c: Composition) -> None:
    setup_standby(c)

    with nudge_standby_snapshots(c):
        # Source creation + initial snapshot both create slots on the standby,
        # which block until a snapshot record arrives -- hence the nudger.
        c.run_testdrive_files("configure-materialize.td", "verify-snapshot.td")

        # Changes made on the primary must flow primary -> standby (physical) ->
        # Materialize (logical decoding on the standby).
        c.run_testdrive_files("insert-update-delete.td", "verify-cdc.td")

        # Validate real-time-recency queries work with CDC data from the standby.
        c.run_testdrive_files("verify-rtr.td")

    _verify_reading_from_standby(c)

    # Finally, demonstrate the hazard: rebuilding the standby from a later version
    # of the primary destroys the slot Materialize depends on and stalls the
    # source. This must run after _verify_reading_from_standby, which asserts the
    # (about to be destroyed) slot still exists.
    rebuild_standby(c)
    with nudge_standby_snapshots(c):
        # Recreating the logical slot on the rebuilt standby blocks on a standby
        # snapshot record, exactly like initial slot creation, so keep nudging.
        c.run_testdrive_files("verify-rebuild-stalls.td")

    # Dropping the source must clean up its logical slot on the standby, even
    # though the source stalled. Dropping a slot (unlike creating one) does not
    # block on a standby snapshot, so this runs outside the nudger.
    _verify_slot_cleaned_up_on_drop(c)

    # Finally, run the other workflows (currently just promotion). Each runs on
    # its own clean slate (see workflow_promotion's docstring) so it stays
    # isolated from everything above; we run them here so they are exercised by
    # the default CI invocation, which passes no workflow name.
    for name in c.workflows:
        if name == "default":
            continue
        with c.test_case(name):
            c.workflow(name)


def _verify_reading_from_standby(c: Composition) -> None:
    """Prove Materialize really decoded from the standby, not the primary."""
    conn = _pg_connect(c, "pg-standby")
    try:
        in_recovery = _query_scalar(conn, "SELECT pg_is_in_recovery()")
        assert in_recovery, "expected pg-standby to be a replica in recovery"

        logical_slots = _query_scalar(
            conn,
            "SELECT count(*) FROM pg_replication_slots WHERE slot_type = 'logical'",
        )
        assert (
            logical_slots >= 1
        ), f"expected a logical slot on the standby, found {logical_slots}"
    finally:
        conn.close()

    # The primary must NOT carry Materialize's logical slot.
    conn = _pg_connect(c, "pg-primary")
    try:
        logical_slots = _query_scalar(
            conn,
            "SELECT count(*) FROM pg_replication_slots WHERE slot_type = 'logical'",
        )
        assert logical_slots == 0, "expected no logical slots on primary"

        in_recovery = _query_scalar(conn, "SELECT pg_is_in_recovery()")
        assert not in_recovery, "expected pg-primary to be the primary"
    finally:
        conn.close()


def _count_logical_slots(c: Composition) -> int:
    conn = _pg_connect(c, "pg-standby")
    try:
        return _query_scalar(
            conn,
            "SELECT count(*) FROM pg_replication_slots WHERE slot_type = 'logical'",
        )
    finally:
        conn.close()


def _verify_slot_cleaned_up_on_drop(c: Composition) -> None:
    """Assert dropping the source removes its logical slot from the standby.

    Slot cleanup runs in a best-effort background task in the coordinator that
    retries for up to ~60s, so it is not synchronous with DROP SOURCE returning.
    Poll the standby until the slot is gone.
    """
    # Sanity check: a logical slot exists before the drop.
    assert (
        _count_logical_slots(c) >= 1
    ), "expected a logical slot on the standby before dropping the source"

    c.run_testdrive_files("drop-source.td")

    for _ in range(90):
        if _count_logical_slots(c) == 0:
            return
        threading.Event().wait(1)
    raise RuntimeError(
        "logical slot was not cleaned up on the standby after dropping the source"
    )

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Check multi-table transaction atomicity against a durable client ledger.

The coordinator accumulates Writes across statements in session.rs, then commits
their table batches through the shared txn-WAL. A single strict-serializable
UNION read must see every participating table's marker or none of them.
"""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Callable

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always_or_unreachable as always,
)

from materialize.antithesis import ddl_hold, quiet, sql, state, telemetry
from materialize.antithesis.atomic_commits_model import Plan, violations
from materialize.antithesis.drivers import configure, history, pod_restarts, rollouts
from materialize.antithesis.drivers.access_control import exclusive
from materialize.antithesis.environment import Environment
from materialize.antithesis.observations import sometimes
from materialize.antithesis.rng import rng

SCHEMA = "atomic_commits"
TABLES = 3
MAX_PLANS = 128
BUDGET_S = 90.0


def open_state() -> sqlite3.Connection:
    db = state.open_db(SCHEMA)
    with db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS plans (id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " shards TEXT NOT NULL, value INTEGER NOT NULL, outcome TEXT NOT NULL,"
            " retired INTEGER NOT NULL DEFAULT 0)"
        )
    return db


def load_plans(db: sqlite3.Connection) -> dict[int, Plan]:
    return {
        marker: Plan(tuple(json.loads(shards)), value, outcome, bool(retired))
        for marker, shards, value, outcome, retired in db.execute("SELECT * FROM plans")
    }


def _execute(conn: psycopg.Connection, deadline: float, query: str, params=None):
    remaining = int(1000 * (deadline - time.monotonic()))
    if remaining <= 0:
        raise TimeoutError("atomic transaction command deadline elapsed")
    conn.execute(f"SET statement_timeout = '{min(20_000, remaining)}ms'".encode())
    return conn.execute(query.encode(), params)


def _connect(host: str) -> psycopg.Connection:
    conn = sql.connect_with_retry(
        host,
        10,
        options={"cluster": configure.SHARED_CLUSTER},
        statement_timeout_ms=20_000,
    )
    try:
        conn.execute("SET transaction_isolation = 'strict serializable'")
    except BaseException:
        conn.close()
        raise
    return conn


def check(
    db: sqlite3.Connection,
    conn: psycopg.Connection,
    deadline: float,
    *,
    uncommitted: int | None = None,
    still_held: Callable[[], bool] | None = None,
) -> bool:
    # All branches belong to one statement, so a commit between separate
    # SELECTs cannot manufacture a torn transaction in the oracle.
    query = " UNION ALL ".join(
        f"SELECT {shard}::int, id, v FROM {SCHEMA}.t{shard}" for shard in range(TABLES)
    )
    # A concurrent COMMIT may finish after this snapshot is chosen. Its later
    # acknowledgement must not become an obligation on this earlier read.
    plans = load_plans(db)
    held = uncommitted if still_held is not None and still_held() else None
    rows = _execute(conn, deadline, query).fetchall()
    if still_held is not None and not still_held():
        held = None
    errors = violations(plans, rows, uncommitted=held)
    always(
        not errors,
        "atomic commits: one snapshot preserves complete transaction markers across tables",
        {"errors": errors[:10], "plans": len(plans), "rows": len(rows)},
    )
    telemetry.comparison("atomic_commits", "table", not errors, max(1, len(plans)))
    if errors:
        return False
    visible = {int(row[1]) for row in rows}
    with db:
        db.executemany(
            "UPDATE plans SET outcome = 'observed' WHERE id = ?"
            " AND outcome = 'indeterminate'",
            [(marker,) for marker in visible],
        )
    sometimes(
        any(
            len(plan.shards) > 1 and marker in visible and not plan.retired
            for marker, plan in plans.items()
        ),
        "atomic commits: a common snapshot checked a nonempty multi-table commit",
        {"visible_transactions": len(visible)},
    )
    sometimes(
        any(plan.outcome == "rejected" for plan in plans.values()),
        "atomic commits: a common snapshot excluded a rolled-back multi-table write",
        {},
    )
    return True


def _prune(db: sqlite3.Connection, conn: psycopg.Connection, deadline: float) -> bool:
    if db.execute("SELECT count(*) FROM plans").fetchone()[0] < MAX_PLANS:
        return True
    if db.execute(
        "SELECT 1 FROM plans WHERE outcome = 'indeterminate' LIMIT 1"
    ).fetchone():
        return False
    cutoff = db.execute("SELECT max(id) FROM plans").fetchone()[0]
    # DELETE is a single-statement transaction in Materialize. Retire checked
    # writes before cleanup, which may remove only some participating tables.
    with db:
        db.execute("UPDATE plans SET retired = 1")
    for shard in range(TABLES):
        # A timed-out DELETE can commit after this process disappears.
        # Its fixed cutoff must not erase the next invocation's markers.
        _execute(
            conn, deadline, f"DELETE FROM {SCHEMA}.t{shard} WHERE id <= %s", (cutoff,)
        )
    with db:
        db.execute("DELETE FROM plans WHERE id <= ?", (cutoff,))
    return True


def write(
    host: str,
    marker: int,
    deadline: float,
    rollback: bool,
    staged: Callable[[], None],
) -> None:
    db = open_state()
    try:
        plan = load_plans(db)[marker]
        try:
            with _connect(host) as conn, conn.transaction():
                for shard in plan.shards:
                    _execute(
                        conn,
                        deadline,
                        f"INSERT INTO {SCHEMA}.t{shard} VALUES (%s, %s)",
                        (marker, plan.value),
                    )
                staged()
                if rollback:
                    raise psycopg.Rollback()
            outcome = "rejected" if rollback else "ok"
        except (psycopg.Error, OSError) as error:
            classification = sql.classify(error)
            always(
                classification.outcome != sql.Outcome.VIOLATION,
                "atomic commits: multi-table writes return only classified SQL errors",
                {
                    "sqlstate": classification.sqlstate,
                    "template": classification.template,
                },
            )
            outcome = (
                "rejected"
                if classification.outcome == sql.Outcome.REJECTED
                else "indeterminate"
            )
        with db:
            db.execute("UPDATE plans SET outcome = ? WHERE id = ?", (outcome, marker))
    finally:
        db.close()


def run_round(
    host: str,
    db: sqlite3.Connection,
    deadline: float,
    *,
    disrupt: pod_restarts.InterleavingDisruption | None = None,
    rollback: bool = False,
    shards: tuple[int, ...] = (0, 1, 2),
    fault_acknowledged: Callable[[], bool] = lambda: False,
) -> None:
    with _connect(host) as reader:
        if not check(db, reader, deadline):
            return
        if not _prune(db, reader, deadline):
            return
        with db:
            marker = db.execute(
                "INSERT INTO plans (shards, value, outcome) VALUES (?, ?, 'indeterminate')",
                (json.dumps(shards), rng.randrange(-(2**30), 2**30)),
            ).lastrowid
        assert marker is not None

        def staged_write(staged: Callable[[], None]) -> None:
            write(host, marker, deadline, rollback, staged)

        def exposure(pending: Callable[[], bool], release: Callable[[], None]) -> None:
            if not check(
                db,
                reader,
                deadline,
                uncommitted=marker,
                still_held=lambda: pending() and time.monotonic() < deadline,
            ):
                return
            if disrupt is not None and pending():
                disrupt.run(pending, release)

        completed = history._overlap_write(
            staged_write,
            pod_restarts.InterleavingDisruption(exposure),
            race_commit=False,
            deadline=deadline,
        )
        if completed:
            passed = check(db, reader, deadline)
            sometimes(
                passed and fault_acknowledged(),
                "atomic commits: a checked multi-table transaction was held across a scoped replica delete",
                {"marker": marker, "rollback": rollback, "shards": shards},
            )


def _fault(
    env: Environment, host: str, deadline: float, acknowledged: list[bool]
) -> pod_restarts.InterleavingDisruption | None:
    cluster_id = telemetry.resolve_cluster_id(host, configure.SHARED_CLUSTER)
    if cluster_id is None:
        return None
    try:
        kube = rollouts.Kube(env)
    except pod_restarts.TRANSIENT_ERRORS:
        return None

    def delete(pending: Callable[[], bool], release: Callable[[], None]) -> None:
        if quiet.active_quiet_period() or not pending():
            return
        try:
            victim = pod_restarts.pick_victim(kube, "clusterd", cluster_id=cluster_id)
            if victim is None:
                return
            acknowledged[0] = pod_restarts.delete_recorded(
                kube,
                origin=SCHEMA,
                role="clusterd",
                namespace=victim.namespace,
                pod=victim.pod,
                uid=victim.uid,
                grace=1,
                reason="held multi-table write transaction",
                cluster_id=cluster_id,
                deadline=deadline,
                still_pending=pending,
                before_delete=release,
            )
        except pod_restarts.TRANSIENT_ERRORS:
            return

    return pod_restarts.InterleavingDisruption(delete)


def main() -> int:
    deadline = time.monotonic() + BUDGET_S
    history.start_watchdog(BUDGET_S + history.WATCHDOG_GRACE_S)
    env = Environment()
    if quiet.active_quiet_period() or ddl_hold.active():
        return 0
    with exclusive(str(env.endpoints.state_dir / f"{SCHEMA}.lock")) as acquired:
        if not acquired:
            return 0
        db = open_state()
        try:
            try:
                host = env.sql_host()
            except pod_restarts.TRANSIENT_ERRORS:
                telemetry.comparison("atomic_commits", "table", None)
                return 0
            with _connect(host) as conn:
                _execute(conn, deadline, f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
                for shard in range(TABLES):
                    _execute(
                        conn,
                        deadline,
                        f"CREATE TABLE IF NOT EXISTS {SCHEMA}.t{shard} (id bigint, v bigint)",
                    )
            for _ in range(rng.randint(2, 5)):
                if time.monotonic() >= deadline or quiet.active_quiet_period():
                    break
                acknowledged = [False]
                fault = (
                    _fault(env, host, deadline, acknowledged)
                    if rng.random() < 0.25
                    else None
                )
                run_round(
                    host,
                    db,
                    deadline,
                    shards=tuple(
                        sorted(rng.sample(range(TABLES), rng.randint(2, TABLES)))
                    ),
                    rollback=rng.random() < 0.25,
                    disrupt=fault,
                    fault_acknowledged=lambda: acknowledged[0],
                )
        except (psycopg.Error, OSError) as error:
            classification = sql.classify(error)
            always(
                classification.outcome != sql.Outcome.VIOLATION,
                "atomic commits: snapshot checks return only classified SQL errors",
                {
                    "sqlstate": classification.sqlstate,
                    "template": classification.template,
                },
            )
            telemetry.comparison("atomic_commits", "table", None)
        finally:
            db.close()
    return 0

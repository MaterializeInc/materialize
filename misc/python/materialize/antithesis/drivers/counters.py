# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Read-then-write statements on a few contended counter rows, with a bound check at every read.

Property: `read-then-write-updates-conserved`.

This is the conservation oracle of `ReadThenWriteCounter` in
`materialize.parallel_workload.database`, mirrored rather than imported because
that class lives next to mzcompose and keeps its tallies in process memory. Here
the tallies are rows in the `counters` state database, written by every
concurrent driver, and the invariant is checked per read rather than at the end
of a run. For a read of key `k` that was invoked at `i` and completed at `c`:

    increments of k acknowledged before i  <=  v  <=  increments of k invoked
                                                      before c, minus those
                                                      definitely rejected

An increment is recorded in the state database before it is sent and its
outcome after the response, so an operation whose process died stays pending
and counts toward the upper bound only.

Each generation of tables holds `K` counter rows, a marker table with the same
keys (for the subquery-gated UPDATE), and a log that `INSERT ... SELECT` appends
the value it read to. Generations rotate after `GEN_MAX_OPS` operations to bound
the size of log reads.
"""

from __future__ import annotations

import sqlite3
import time
import uuid
from collections.abc import Callable

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    sometimes,
    unreachable,
)

from materialize.antithesis import sql, state
from materialize.antithesis.drivers.history import (
    WATCHDOG_GRACE_S,
    current_generation,
    ensure_generation_tables,
    generation_params,
    rotate_generation,
    start_watchdog,
)
from materialize.antithesis.environment import Environment
from materialize.antithesis.rng import rng

STATE_DB = "counters"

# Calibration: the budgets below are first guesses and need a fault-free
# baseline on one simulated core before they are trusted.
DRIVER_BUDGET_S = (10.0, 90.0)
DRIVER_MAX_OPS = 300
GEN_MAX_OPS = 2000
CONNECT_DEADLINE_S = 30.0
STATEMENT_TIMEOUT_MS = 30_000
RECENT_ACTIVITY_WINDOW_S = 30.0

KEY_COUNTS = (1, 2, 3, 5)
"""Few keys maximize conflicts between concurrent read-then-write statements."""

UPDATE = "update"
UPDATE_MARKER = "update_marker"
INSERT_SELECT = "insert_select"
READ_COUNTERS = "read_counters"
READ_LOG = "read_log"
ACTIONS = (UPDATE, UPDATE_MARKER, INSERT_SELECT, READ_COUNTERS, READ_LOG)
INCREMENTS = (UPDATE, UPDATE_MARKER)


def log(message: str) -> None:
    print(f"counters: {message}", flush=True)


def _counters(gen: int) -> str:
    return f"counters_c_g{gen}"


def _markers(gen: int) -> str:
    return f"counters_m_g{gen}"


def _log(gen: int) -> str:
    return f"counters_log_g{gen}"


def open_state() -> sqlite3.Connection:
    db = state.open_db(STATE_DB)
    ensure_generation_tables(db)
    with db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS ops ("
            " op_id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " gen INTEGER NOT NULL,"
            " k INTEGER NOT NULL,"
            " family TEXT NOT NULL,"
            " op_uuid TEXT NOT NULL UNIQUE,"
            " invoke_rt REAL NOT NULL,"
            " complete_rt REAL,"
            # NULL while in flight, or forever if the process died: unknown.
            # Otherwise 'ok', 'rejected', or 'indeterminate'.
            " outcome TEXT)"
        )
        db.execute("CREATE INDEX IF NOT EXISTS ops_gen_k ON ops (gen, k, family)")
        db.execute(
            "CREATE TABLE IF NOT EXISTS timeline_params ("
            " slot INTEGER PRIMARY KEY CHECK (slot = 0), weights TEXT NOT NULL)"
        )
    return db


def recent_activity(window_s: float = RECENT_ACTIVITY_WINDOW_S) -> int:
    """Writes in flight or invoked within the last `window_s` seconds."""
    try:
        db = open_state()
        try:
            now = time.monotonic()
            row = db.execute(
                "SELECT count(*) FROM ops WHERE invoke_rt >= ?"
                " OR (outcome IS NULL AND invoke_rt >= ?)",
                (now - window_s, now - 10 * window_s),
            ).fetchone()
            return int(row[0])
        finally:
            db.close()
    except sqlite3.Error:
        return 0


def _timeline_weights(db: sqlite3.Connection) -> list[float]:
    """Per-timeline action weights. Each action may be left out entirely."""
    query = "SELECT weights FROM timeline_params"
    if db.execute(query).fetchone() is None:
        while True:
            weights = [rng.choice([0.0, 1.0, 1.0, 5.0]) for _ in ACTIONS]
            # At least one increment and one read, or nothing is checked.
            if any(weights[:2]) and any(weights[3:]):
                break
        with db:
            db.execute(
                "INSERT OR IGNORE INTO timeline_params VALUES (0, ?)",
                (",".join(str(w) for w in weights),),
            )
    row = db.execute(query).fetchone()
    return [float(w) for w in str(row[0]).split(",")]


def _create_generation(host: str, keys: int) -> Callable[[int], bool]:
    def create(gen: int) -> bool:
        try:
            with sql.connection(
                host, statement_timeout_ms=STATEMENT_TIMEOUT_MS
            ) as conn:
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {_counters(gen)} (k int NOT NULL, v bigint NOT NULL)".encode()
                )
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {_markers(gen)} (k int NOT NULL)".encode()
                )
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {_log(gen)}"
                    " (op text NOT NULL, k int NOT NULL, v bigint NOT NULL)".encode()
                )
                # The tables are fresh, so each seed must be acknowledged
                # exactly once. Any doubt abandons the generation.
                conn.execute(
                    f"INSERT INTO {_counters(gen)} SELECT g, 0 FROM generate_series(0, %s) AS g".encode(),
                    (keys - 1,),
                )
                conn.execute(
                    f"INSERT INTO {_markers(gen)} SELECT g FROM generate_series(0, %s) AS g".encode(),
                    (keys - 1,),
                )
            return True
        except (psycopg.Error, OSError) as e:
            log(f"creating generation {gen} failed: {e}")
            return False

    return create


def _drop_generation(host: str) -> Callable[[int], bool]:
    def drop(gen: int) -> bool:
        try:
            with sql.connection(
                host, statement_timeout_ms=STATEMENT_TIMEOUT_MS
            ) as conn:
                for table in (_counters(gen), _markers(gen), _log(gen)):
                    conn.execute(f"DROP TABLE IF EXISTS {table}".encode())
            return True
        except (psycopg.Error, OSError):
            return False

    return drop


def _generation(
    db: sqlite3.Connection, host: str, force_rotate: bool
) -> tuple[int, int] | None:
    """The current generation and its key count, rotating it if it is full or broken."""
    gen = current_generation(db)
    if gen is not None and not force_rotate:
        row = db.execute("SELECT count(*) FROM ops WHERE gen = ?", (gen,)).fetchone()
        if int(row[0]) < GEN_MAX_OPS:
            return gen, int(generation_params(db, gen))
    keys = rng.choice(KEY_COUNTS)
    gen = rotate_generation(
        db, gen, str(keys), _create_generation(host, keys), _drop_generation(host)
    )
    if gen is None:
        return None
    return gen, int(generation_params(db, gen))


def _connect(host: str) -> psycopg.Connection:
    conn = sql.connect_with_retry(
        host, CONNECT_DEADLINE_S, statement_timeout_ms=STATEMENT_TIMEOUT_MS
    )
    # The lower bounds assume every statement is linearized after every
    # previously acknowledged write.
    conn.execute("SET transaction_isolation = 'strict serializable'")
    return conn


def _report_violation(family: str, c: sql.Classified, details: dict) -> None:
    details = {**details, "sqlstate": c.sqlstate, "template": c.template}
    if family == UPDATE:
        unreachable(
            "counters: UPDATE increment returns only classified errors", details
        )
    elif family == UPDATE_MARKER:
        unreachable(
            "counters: UPDATE increment gated on a marker subquery returns only classified errors",
            details,
        )
    elif family == INSERT_SELECT:
        unreachable(
            "counters: INSERT ... SELECT of a counter value returns only classified errors",
            details,
        )
    elif family == READ_COUNTERS:
        unreachable("counters: counter read returns only classified errors", details)
    else:
        unreachable(
            "counters: INSERT ... SELECT log read returns only classified errors",
            details,
        )


def _write(
    db: sqlite3.Connection, conn: psycopg.Connection, gen: int, k: int, family: str
) -> tuple[str, bool]:
    """Run one write. Returns its outcome and whether the generation's tables are missing."""
    op_uuid = str(uuid.UUID(int=rng.getrandbits(128)))
    if family == UPDATE:
        statement = f"UPDATE {_counters(gen)} SET v = v + 1 WHERE k = %s"
        params: tuple = (k,)
    elif family == UPDATE_MARKER:
        statement = (
            f"UPDATE {_counters(gen)} SET v = v + 1"
            f" WHERE k = %s AND k IN (SELECT k FROM {_markers(gen)})"
        )
        params = (k,)
    else:
        statement = f"INSERT INTO {_log(gen)} SELECT %s, k, v FROM {_counters(gen)} WHERE k = %s"
        params = (op_uuid, k)

    with db:
        cur = db.execute(
            "INSERT INTO ops (gen, k, family, op_uuid, invoke_rt) VALUES (?, ?, ?, ?, ?)",
            (gen, k, family, op_uuid, time.monotonic()),
        )
    op_id = cur.lastrowid
    outcome = "ok"
    missing = False
    try:
        rowcount = conn.execute(statement.encode(), params).rowcount
        details = {"op_id": op_id, "gen": gen, "k": k, "rowcount": rowcount}
        if family == UPDATE:
            always(
                rowcount == 1,
                "counters: UPDATE increment affects exactly the one seeded row",
                details,
            )
        elif family == UPDATE_MARKER:
            always(
                rowcount == 1,
                "counters: marker-gated UPDATE increment affects exactly the one seeded row",
                details,
            )
        else:
            always(
                rowcount == 1,
                "counters: INSERT ... SELECT inserts exactly one log row",
                details,
            )
    except Exception as e:
        c = sql.classify(e)
        if c.outcome == sql.Outcome.REJECTED:
            outcome = "rejected"
            missing = c.race is sql.CatalogRace.MISSING
        else:
            if c.outcome == sql.Outcome.VIOLATION:
                _report_violation(family, c, {"op_id": op_id, "gen": gen, "k": k})
            outcome = "indeterminate"
    with db:
        db.execute(
            "UPDATE ops SET complete_rt = ?, outcome = ? WHERE op_id = ?",
            (time.monotonic(), outcome, op_id),
        )
    return outcome, missing


def _bounds(
    db: sqlite3.Connection, gen: int, k: int, invoke: float, complete: float
) -> tuple[int, int, int]:
    """Lower bound, upper bound, and final-indeterminate increments of key `k`."""
    placeholders = ",".join("?" for _ in INCREMENTS)
    row = db.execute(
        "SELECT"
        " sum(outcome = 'ok' AND complete_rt < ?),"
        " sum(invoke_rt < ? AND (outcome IS NULL OR outcome != 'rejected')),"
        " sum(invoke_rt < ? AND outcome = 'indeterminate')"
        f" FROM ops WHERE gen = ? AND k = ? AND family IN ({placeholders})",
        (invoke, complete, complete, gen, k, *INCREMENTS),
    ).fetchone()
    return int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)


def _read_counters(
    db: sqlite3.Connection, conn: psycopg.Connection, gen: int, keys: int
) -> bool:
    """Read every counter and check it against its bounds. Returns whether the tables are missing."""
    invoke = time.monotonic()
    try:
        rows = conn.execute(f"SELECT k, v FROM {_counters(gen)}".encode()).fetchall()
    except Exception as e:
        c = sql.classify(e)
        if c.outcome == sql.Outcome.VIOLATION:
            _report_violation(READ_COUNTERS, c, {"gen": gen})
        if c.outcome == sql.Outcome.INDETERMINATE:
            raise
        return c.race is sql.CatalogRace.MISSING
    complete = time.monotonic()
    by_key: dict[int, list[int]] = {}
    for k, v in rows:
        by_key.setdefault(int(k), []).append(int(v))
    always(
        sorted(by_key) == list(range(keys))
        and all(len(v) == 1 for v in by_key.values()),
        "counters: the counter table holds exactly one row per seeded key",
        {
            "gen": gen,
            "keys": keys,
            "rows": sorted((k, len(v)) for k, v in by_key.items()),
        },
    )
    for k, values in sorted(by_key.items()):
        if len(values) != 1:
            continue
        v = values[0]
        lower, upper, unknown = _bounds(db, gen, k, invoke, complete)
        details = {"gen": gen, "k": k, "v": v, "lower": lower, "upper": upper}
        always(
            v >= lower,
            "counters: counter value includes every increment acknowledged before the read",
            details,
        )
        always(
            v <= upper,
            "counters: counter value includes no increment beyond those attempted before the read completed",
            details,
        )
        sometimes(
            unknown > 0,
            "counters: counter checked while an increment's outcome was indeterminate",
            {"gen": gen, "k": k, "unknown": unknown},
        )
        sometimes(
            upper - lower >= 2,
            "counters: counter checked while several increments of one key were unresolved",
            details,
        )
    return False


def _read_log(db: sqlite3.Connection, conn: psycopg.Connection, gen: int) -> bool:
    """Check the INSERT ... SELECT log against the recorded operations."""
    invoke = time.monotonic()
    try:
        rows = conn.execute(f"SELECT op, k, v FROM {_log(gen)}".encode()).fetchall()
    except Exception as e:
        c = sql.classify(e)
        if c.outcome == sql.Outcome.VIOLATION:
            _report_violation(READ_LOG, c, {"gen": gen})
        if c.outcome == sql.Outcome.INDETERMINATE:
            raise
        return c.race is sql.CatalogRace.MISSING
    complete = time.monotonic()
    ops = {
        r[0]: r[1:]
        for r in db.execute(
            "SELECT op_uuid, k, invoke_rt, complete_rt, outcome FROM ops"
            " WHERE gen = ? AND family = ?",
            (gen, INSERT_SELECT),
        )
    }
    seen: dict[str, int] = {}
    for op_uuid, k, v in rows:
        seen[op_uuid] = seen.get(op_uuid, 0) + 1
        op = ops.get(op_uuid)
        always(
            op is not None and op[0] == k and op[1] < complete and op[3] != "rejected",
            "counters: every log row comes from an INSERT ... SELECT attempted before the read completed",
            {"gen": gen, "op": op_uuid, "k": k, "recorded": op},
        )
        if op is None:
            continue
        # The INSERT read the counter somewhere between its own invoke and
        # completion (or this read's completion, if its outcome is unknown).
        op_complete = op[2] if op[2] is not None and op[3] == "ok" else complete
        lower, _, _ = _bounds(db, gen, int(k), op[1], op[1])
        _, upper, _ = _bounds(db, gen, int(k), op_complete, op_complete)
        always(
            lower <= int(v) <= upper,
            "counters: INSERT ... SELECT logged a counter value within the bounds of its own execution",
            {"gen": gen, "op": op_uuid, "k": k, "v": v, "lower": lower, "upper": upper},
        )
    duplicated = sorted(u for u, n in seen.items() if n > 1)
    always(
        not duplicated,
        "counters: each INSERT ... SELECT is applied at most once",
        {"gen": gen, "ops": duplicated[:20]},
    )
    missing = sorted(
        u
        for u, (_, _, op_complete, outcome) in ops.items()
        if outcome == "ok"
        and op_complete is not None
        and op_complete < invoke
        and u not in seen
    )
    always(
        not missing,
        "counters: acknowledged INSERT ... SELECT rows are visible to later reads",
        {"gen": gen, "ops": missing[:20]},
    )
    return False


def read_then_write_driver() -> int:
    db = open_state()
    host = Environment().sql_host()
    weights = _timeline_weights(db)
    budget = rng.uniform(*DRIVER_BUDGET_S)
    start_watchdog(budget + WATCHDOG_GRACE_S)
    deadline = time.monotonic() + budget
    conn: psycopg.Connection | None = None
    force_rotate = False
    ops = 0
    while ops < DRIVER_MAX_OPS and time.monotonic() < deadline:
        try:
            current = _generation(db, host, force_rotate)
            force_rotate = False
            if current is None:
                time.sleep(1)
                continue
            gen, keys = current
            if conn is None or conn.closed:
                conn = _connect(host)
            action = rng.choices(ACTIONS, weights=weights)[0]
            if action == READ_COUNTERS:
                force_rotate = _read_counters(db, conn, gen, keys)
            elif action == READ_LOG:
                force_rotate = _read_log(db, conn, gen)
            else:
                outcome, force_rotate = _write(
                    db, conn, gen, rng.randrange(keys), action
                )
                if outcome == "indeterminate":
                    conn.close()
                    conn = None
            ops += 1
        except (psycopg.Error, OSError) as e:
            log(f"transient error, reconnecting: {e}")
            if conn is not None:
                try:
                    conn.close()
                except psycopg.Error:
                    pass
            conn = None
            time.sleep(rng.uniform(0.1, 2.0))
    if conn is not None:
        conn.close()
    log(f"finished {ops} operations")
    return 0

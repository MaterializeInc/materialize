# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Client-side history of writes and reads, and an incremental checker over it.

Properties: `strict-serializable-client-history` and the data-level claims of
`acked-writes-survive-generation-handoff` that hold through the public Service.

`client_history_driver` writes globally unique ids into an append-only table and
reads `mz_now()` together with the full set of ids, from sessions whose
`transaction_isolation` is drawn at random. Every operation is recorded in the
`history` state database before it is sent, with its invoke and completion
instants on `time.monotonic()` and its outcome. `client_history_check` evaluates
the completed reads that it has not checked yet against the whole history.

Real-time comparisons rely on `CLOCK_MONOTONIC` being shared by every process in
the workload container, so instants recorded by concurrent commands compare
directly.

The table rotates to a new epoch after `EPOCH_MAX_OPS` operations, which bounds
both the id set a read returns and the work the checker does per read. Checks
that compare contents stay within one epoch; timestamp checks span epochs.
"""

from __future__ import annotations

import bisect
import os
import sqlite3
import threading
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    always_or_unreachable,
    reachable,
    sometimes,
    unreachable,
)

from materialize.antithesis import sql, state
from materialize.antithesis.environment import Environment
from materialize.antithesis.rng import rng

STATE_DB = "history"
TABLE_PREFIX = "history_writes_e"

# Calibration: the budgets below are first guesses and need a fault-free
# baseline on one simulated core before they are trusted.
DRIVER_BUDGET_S = (10.0, 90.0)
"""Range of the wall-clock budget of one driver invocation."""
DRIVER_MAX_OPS = 400
EPOCH_MAX_OPS = 1500
"""Operations per table epoch. Keeps each read's id set to a few kilobytes."""
CONNECT_DEADLINE_S = 30.0
STATEMENT_TIMEOUT_MS = 30_000
ACK_TO_READ_GAP_S = 0.1
"""A strict serializable read that starts this soon after a write was
acknowledged is the hard case for the visibility check."""
RECENT_ACTIVITY_WINDOW_S = 30.0
WATCHDOG_GRACE_S = 180.0
"""Time past the driver budget before the watchdog ends the process."""

STRICT = "strict serializable"
SERIALIZABLE = "serializable"
STRONG_SESSION = "strong session serializable"
ISOLATIONS = (STRICT, SERIALIZABLE, STRONG_SESSION)


def log(message: str) -> None:
    print(f"client-history: {message}", flush=True)


def start_watchdog(seconds: float) -> None:
    """End the process with status 0 after `seconds`, even if a statement never returns.

    A wedged coordinator can hold a statement open past every server-side
    timeout. The canaries own that liveness failure, so a driver only needs to
    exit. Operations in flight stay pending in the state database, which every
    checker treats as indeterminate.
    """

    def fire() -> None:
        print(f"watchdog: exiting after {seconds:.0f}s", flush=True)
        os._exit(0)

    timer = threading.Timer(seconds, fire)
    timer.daemon = True
    timer.start()


def ensure_generation_tables(db: sqlite3.Connection) -> None:
    """Create the bookkeeping for rotating table generations, see `rotate_generation`."""
    with db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS generations ("
            " gen INTEGER PRIMARY KEY AUTOINCREMENT, params TEXT)"
        )
        db.execute(
            "CREATE TABLE IF NOT EXISTS current_generation ("
            " slot INTEGER PRIMARY KEY CHECK (slot = 0), gen INTEGER NOT NULL)"
        )
        db.execute(
            "CREATE TABLE IF NOT EXISTS dropped_generations (gen INTEGER PRIMARY KEY)"
        )


def current_generation(db: sqlite3.Connection) -> int | None:
    row = db.execute("SELECT gen FROM current_generation WHERE slot = 0").fetchone()
    return None if row is None else int(row[0])


def generation_params(db: sqlite3.Connection, gen: int) -> str:
    row = db.execute("SELECT params FROM generations WHERE gen = ?", (gen,)).fetchone()
    return "" if row is None or row[0] is None else str(row[0])


def allocate_generation(db: sqlite3.Connection, params: str = "") -> int:
    with db:
        cur = db.execute("INSERT INTO generations (params) VALUES (?)", (params,))
        assert cur.lastrowid is not None
        return int(cur.lastrowid)


def install_generation(db: sqlite3.Connection, expected: int | None, new: int) -> bool:
    """Make `new` current if the current generation is still `expected`."""
    with db:
        if expected is None:
            cur = db.execute(
                "INSERT OR IGNORE INTO current_generation (slot, gen) VALUES (0, ?)",
                (new,),
            )
        else:
            cur = db.execute(
                "UPDATE current_generation SET gen = ? WHERE slot = 0 AND gen = ?",
                (new, expected),
            )
        return cur.rowcount == 1


def rotate_generation(
    db: sqlite3.Connection,
    expected: int | None,
    params: str,
    create: Callable[[int], bool],
    drop: Callable[[int], bool],
) -> int | None:
    """Try to replace generation `expected` with a fresh one.

    Any process may propose a generation and create its SQL objects. The
    compare-and-set in `install_generation` picks the winner, so no process
    holds a SQLite lock across a network round trip. `create` builds the SQL
    objects of a generation and returns whether every statement was definitely
    acknowledged. `drop` removes them and returns whether it succeeded.
    Returns the current generation after the attempt, which is another
    process's when this one loses the race.

    The winner also drops every generation older than `expected`. `expected`
    itself stays, because drivers that read the current generation before
    this rotation may still be using it.
    """
    new = allocate_generation(db, params)
    if create(new):
        if not install_generation(db, expected, new):
            drop(new)
        elif expected is not None:
            _drop_older(db, expected, drop)
    else:
        drop(new)
    return current_generation(db)


def _drop_older(db: sqlite3.Connection, keep: int, drop: Callable[[int], bool]) -> None:
    """Best effort: a generation whose drop fails is retried at the next rotation."""
    stale = [
        int(r[0])
        for r in db.execute(
            "SELECT gen FROM generations WHERE gen < ?"
            " AND gen NOT IN (SELECT gen FROM dropped_generations)",
            (keep,),
        )
    ]
    for gen in stale:
        if drop(gen):
            with db:
                db.execute(
                    "INSERT OR IGNORE INTO dropped_generations VALUES (?)", (gen,)
                )


def open_state() -> sqlite3.Connection:
    db = state.open_db(STATE_DB)
    ensure_generation_tables(db)
    with db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS ops ("
            " op_id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " epoch INTEGER NOT NULL,"
            " kind TEXT NOT NULL,"  # 'w' or 'r'
            " session TEXT NOT NULL,"
            " isolation TEXT NOT NULL,"
            " invoke_rt REAL NOT NULL,"
            " complete_rt REAL,"
            # NULL while in flight, or forever if the process died: indeterminate.
            " outcome TEXT,"
            " ts INTEGER,"
            " uptime_s REAL,"
            " ids TEXT,"
            " checked INTEGER NOT NULL DEFAULT 0)"
        )
        db.execute(
            "CREATE INDEX IF NOT EXISTS ops_unchecked ON ops (checked, kind, outcome)"
        )
        db.execute("CREATE INDEX IF NOT EXISTS ops_epoch ON ops (epoch, kind)")
        db.execute(
            "CREATE TABLE IF NOT EXISTS timeline_params ("
            " slot INTEGER PRIMARY KEY CHECK (slot = 0), write_prob REAL NOT NULL,"
            " isolation_weights TEXT NOT NULL)"
        )
    return db


def recent_activity(window_s: float = RECENT_ACTIVITY_WINDOW_S) -> int:
    """Operations in flight or invoked within the last `window_s` seconds."""
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


def _timeline_params(db: sqlite3.Connection) -> tuple[float, list[float]]:
    """Per-timeline swarm parameters, drawn once by whichever invocation is first."""
    query = "SELECT write_prob, isolation_weights FROM timeline_params"
    if db.execute(query).fetchone() is None:
        write_prob = rng.choice([0.05, 0.3, 0.6, 0.95])
        while True:
            weights = [rng.choice([0.0, 1.0, 4.0]) for _ in ISOLATIONS]
            if any(weights):
                break
        with db:
            db.execute(
                "INSERT OR IGNORE INTO timeline_params VALUES (0, ?, ?)",
                (write_prob, ",".join(str(w) for w in weights)),
            )
    row = db.execute(query).fetchone()
    return float(row[0]), [float(w) for w in str(row[1]).split(",")]


def _table(epoch: int) -> str:
    return f"{TABLE_PREFIX}{epoch}"


class Session:
    """One SQL session at one isolation level. Reconnecting starts a new session."""

    def __init__(self, host: str, weights: list[float]) -> None:
        self.host = host
        self.weights = weights
        self.conn: psycopg.Connection | None = None
        self.name = ""
        self.isolation = STRICT

    def ensure(self) -> psycopg.Connection:
        if self.conn is not None and not self.conn.closed:
            return self.conn
        conn = sql.connect_with_retry(
            self.host,
            CONNECT_DEADLINE_S,
            statement_timeout_ms=STATEMENT_TIMEOUT_MS,
        )
        isolation = rng.choices(ISOLATIONS, weights=self.weights)[0]
        try:
            conn.execute(f"SET transaction_isolation = '{isolation}'")
        except psycopg.Error as e:
            if isolation != STRONG_SESSION:
                conn.close()
                raise
            # Strong session serializable is gated on `enable_session_timelines`.
            log(f"strong session serializable unavailable ({e}), using strict")
            isolation = STRICT
            conn.execute(f"SET transaction_isolation = '{isolation}'")
        self.conn = conn
        self.isolation = isolation
        self.name = f"{time.monotonic_ns()}-{rng.getrandbits(32):08x}"
        return conn

    def reset(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            except psycopg.Error:
                pass
        self.conn = None


def _create_epoch(host: str) -> Callable[[int], bool]:
    def create(epoch: int) -> bool:
        try:
            with sql.connection(
                host, statement_timeout_ms=STATEMENT_TIMEOUT_MS
            ) as conn:
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {_table(epoch)}"
                    " (id bigint NOT NULL, session text NOT NULL)".encode()
                )
            return True
        except (psycopg.Error, OSError) as e:
            log(f"creating epoch {epoch} failed: {e}")
            return False

    return create


def _drop_epoch(host: str) -> Callable[[int], bool]:
    def drop(epoch: int) -> bool:
        try:
            with sql.connection(
                host, statement_timeout_ms=STATEMENT_TIMEOUT_MS
            ) as conn:
                conn.execute(f"DROP TABLE IF EXISTS {_table(epoch)}".encode())
            return True
        except (psycopg.Error, OSError):
            return False

    return drop


def _epoch_for_op(db: sqlite3.Connection, host: str, force_rotate: bool) -> int | None:
    epoch = current_generation(db)
    if epoch is not None and not force_rotate:
        row = db.execute(
            "SELECT count(*) FROM ops WHERE epoch = ?", (epoch,)
        ).fetchone()
        if int(row[0]) < EPOCH_MAX_OPS:
            return epoch
    return rotate_generation(db, epoch, "", _create_epoch(host), _drop_epoch(host))


def _write(db: sqlite3.Connection, session: Session, epoch: int) -> bool:
    """Insert one fresh id. Returns whether the epoch's table is missing."""
    conn = session.ensure()
    invoke = time.monotonic()
    with db:
        cur = db.execute(
            "INSERT INTO ops (epoch, kind, session, isolation, invoke_rt)"
            " VALUES (?, 'w', ?, ?, ?)",
            (epoch, session.name, session.isolation, invoke),
        )
    op_id = cur.lastrowid
    outcome = "ok"
    missing = False
    try:
        conn.execute(
            f"INSERT INTO {_table(epoch)} (id, session) VALUES (%s, %s)".encode(),
            (op_id, session.name),
        )
    except Exception as e:
        c = sql.classify(e)
        if c.outcome == sql.Outcome.REJECTED:
            outcome = "rejected"
            missing = c.race is sql.CatalogRace.MISSING
        else:
            if c.outcome == sql.Outcome.VIOLATION:
                unreachable(
                    "client history: INSERT of a fresh id returns only classified errors",
                    {"sqlstate": c.sqlstate, "template": c.template, "op_id": op_id},
                )
            outcome = "indeterminate"
            session.reset()
    complete = time.monotonic()
    with db:
        db.execute(
            "UPDATE ops SET complete_rt = ?, outcome = ? WHERE op_id = ?",
            (complete, outcome, op_id),
        )
    return missing


def _read(db: sqlite3.Connection, session: Session, epoch: int) -> bool:
    """Read the epoch's full id set at one timestamp. Returns whether the table is missing."""
    conn = session.ensure()
    invoke = time.monotonic()
    with db:
        cur = db.execute(
            "INSERT INTO ops (epoch, kind, session, isolation, invoke_rt)"
            " VALUES (?, 'r', ?, ?, ?)",
            (epoch, session.name, session.isolation, invoke),
        )
    op_id = cur.lastrowid
    try:
        row = conn.execute(
            "SELECT mz_now()::text, extract(epoch FROM mz_uptime())::float8,"
            " c, d, m, ids FROM (SELECT count(*) AS c, count(DISTINCT id) AS d,"
            f" max(id) AS m, string_agg(id::text, ',') AS ids FROM {_table(epoch)})".encode()
        ).fetchone()
    except Exception as e:
        c = sql.classify(e)
        if c.outcome == sql.Outcome.VIOLATION:
            unreachable(
                "client history: mz_now() read of the id set returns only classified errors",
                {"sqlstate": c.sqlstate, "template": c.template, "op_id": op_id},
            )
        if c.outcome != sql.Outcome.REJECTED:
            session.reset()
        with db:
            db.execute(
                "UPDATE ops SET complete_rt = ?, outcome = 'error' WHERE op_id = ?",
                (time.monotonic(), op_id),
            )
        return c.race is sql.CatalogRace.MISSING
    complete = time.monotonic()
    assert row is not None
    ts, uptime_s, count, distinct, max_id, ids_text = row
    ids = [int(x) for x in ids_text.split(",")] if ids_text else []
    always(
        int(count) == len(ids) and (max(ids) if ids else None) == max_id,
        "client history: count(*), max(id), and the id list agree within one read",
        {"op_id": op_id, "count": count, "max": max_id, "listed": len(ids)},
    )
    # Every id is inserted by exactly one statement, so a duplicate means a
    # retried append whose first attempt had also committed
    # (indeterminate-cas-outcome-never-misreported).
    duplicated = (
        {i for i, n in Counter(ids).items() if n > 1}
        if int(count) != int(distinct)
        else set()
    )
    always(
        int(count) == int(distinct),
        "client history: no written id appears twice in the table",
        {
            "op_id": op_id,
            "epoch": epoch,
            "count": count,
            "distinct": distinct,
            "duplicated_ids": _sample(duplicated),
        },
    )
    with db:
        db.execute(
            "UPDATE ops SET complete_rt = ?, outcome = 'ok', ts = ?, uptime_s = ?,"
            " ids = ? WHERE op_id = ?",
            (complete, int(ts), float(uptime_s), ids_text or "", op_id),
        )
    if session.isolation == STRONG_SESSION:
        reachable(
            "client history: a strong session serializable read completed",
            {"op_id": op_id},
        )
    return False


def client_history_driver() -> int:
    db = open_state()
    host = Environment().sql_host()
    write_prob, weights = _timeline_params(db)
    budget = rng.uniform(*DRIVER_BUDGET_S)
    start_watchdog(budget + WATCHDOG_GRACE_S)
    deadline = time.monotonic() + budget
    session = Session(host, weights)
    force_rotate = False
    ops = 0
    while ops < DRIVER_MAX_OPS and time.monotonic() < deadline:
        try:
            epoch = _epoch_for_op(db, host, force_rotate)
            force_rotate = False
            if epoch is None:
                time.sleep(1)
                continue
            if rng.random() < write_prob:
                force_rotate = _write(db, session, epoch)
            else:
                force_rotate = _read(db, session, epoch)
            ops += 1
            # Occasionally start a new session so per-session isolation and
            # reconnects vary within one invocation.
            if rng.random() < 0.05:
                session.reset()
        except (psycopg.Error, OSError) as e:
            log(f"transient error, reconnecting: {e}")
            session.reset()
            time.sleep(rng.uniform(0.1, 2.0))
    session.reset()
    log(f"finished {ops} operations")
    return 0


@dataclass
class Read:
    op_id: int
    epoch: int
    session: str
    isolation: str
    invoke_rt: float
    complete_rt: float
    ts: int
    uptime_s: float
    ids: frozenset[int]
    checked: bool


@dataclass
class Write:
    op_id: int
    epoch: int
    session: str
    invoke_rt: float
    complete_rt: float | None
    outcome: str | None


def _load_reads(
    db: sqlite3.Connection, where: str, args: tuple, with_ids: bool = True
) -> list[Read]:
    ids_column = "ids" if with_ids else "''"
    rows = db.execute(
        "SELECT op_id, epoch, session, isolation, invoke_rt, complete_rt, ts,"
        f" uptime_s, {ids_column}, checked FROM ops"
        f" WHERE kind = 'r' AND outcome = 'ok' AND {where}",
        args,
    ).fetchall()
    return [
        Read(
            op_id=r[0],
            epoch=r[1],
            session=r[2],
            isolation=r[3],
            invoke_rt=r[4],
            complete_rt=r[5],
            ts=int(r[6]),
            uptime_s=float(r[7]),
            ids=frozenset(int(x) for x in r[8].split(",")) if r[8] else frozenset(),
            checked=bool(r[9]),
        )
        for r in rows
    ]


def _load_writes(db: sqlite3.Connection, epoch: int) -> dict[int, Write]:
    rows = db.execute(
        "SELECT op_id, epoch, session, invoke_rt, complete_rt, outcome FROM ops"
        " WHERE kind = 'w' AND epoch = ?",
        (epoch,),
    ).fetchall()
    return {r[0]: Write(*r) for r in rows}


def _sample(ids: set[int] | frozenset[int]) -> list[int]:
    return sorted(ids)[:20]


def _check_epoch(epoch: int, new: list[Read], db: sqlite3.Connection) -> None:
    writes = _load_writes(db, epoch)
    reads = sorted(_load_reads(db, "epoch = ?", (epoch,)), key=lambda r: r.ts)
    ts_keys = [r.ts for r in reads]
    acked = sorted(
        (w for w in writes.values() if w.outcome == "ok" and w.complete_rt is not None),
        key=lambda w: w.complete_rt or 0.0,
    )
    acked_completes = [w.complete_rt or 0.0 for w in acked]
    # In-flight writes (outcome still NULL) may be observed before their ack is
    # recorded, so they only count toward the stability check, not the reach claim.
    indeterminate = {
        w.op_id for w in writes.values() if w.outcome in (None, "indeterminate")
    }
    known_indeterminate = {
        w.op_id for w in writes.values() if w.outcome == "indeterminate"
    }

    for r in new:
        phantoms = {
            i for i in r.ids if i not in writes or writes[i].invoke_rt >= r.complete_rt
        }
        always(
            not phantoms,
            "client history: reads observe only writes attempted before the read completed",
            {"read": r.op_id, "epoch": epoch, "phantom_ids": _sample(phantoms)},
        )
        rejected_seen = {
            i for i in r.ids if i in writes and writes[i].outcome == "rejected"
        }
        always(
            not rejected_seen,
            "client history: writes rejected with a definite error are never observed",
            {"read": r.op_id, "epoch": epoch, "ids": _sample(rejected_seen)},
        )

        # Every acknowledged write that completed before the read began.
        before = acked[: bisect.bisect_left(acked_completes, r.invoke_rt)]
        if r.isolation == STRICT:
            missing = {w.op_id for w in before if w.op_id not in r.ids}
            always(
                not missing,
                "client history: strict serializable reads observe every write acknowledged before they began",
                {
                    "read": r.op_id,
                    "epoch": epoch,
                    "ts": r.ts,
                    "missing_ids": _sample(missing),
                },
            )
            if before:
                gap = r.invoke_rt - (before[-1].complete_rt or 0.0)
                sometimes(
                    gap < ACK_TO_READ_GAP_S,
                    "client history: strict serializable read began within 100 ms of a write acknowledgement",
                    {"read": r.op_id, "gap_s": gap},
                )
        elif r.isolation == STRONG_SESSION:
            missing = {
                w.op_id
                for w in before
                if w.session == r.session and w.op_id not in r.ids
            }
            always_or_unreachable(
                not missing,
                "client history: strong session serializable reads observe the session's own acknowledged writes",
                {"read": r.op_id, "epoch": epoch, "missing_ids": _sample(missing)},
            )

        # Snapshot inclusion by timestamp. Checking the nearest neighbors is
        # enough: the reads already checked form a chain under inclusion.
        lo = bisect.bisect_left(ts_keys, r.ts)
        hi = bisect.bisect_right(ts_keys, r.ts)
        equal = [o for o in reads[lo:hi] if o.op_id != r.op_id]
        prev = reads[lo - 1] if lo > 0 else None
        nxt = reads[hi] if hi < len(reads) else None
        for o in equal:
            always_or_unreachable(
                o.ids == r.ids,
                "client history: reads at the same timestamp observe the same writes",
                {
                    "read": r.op_id,
                    "other": o.op_id,
                    "ts": r.ts,
                    "only_in_read": _sample(r.ids - o.ids),
                    "only_in_other": _sample(o.ids - r.ids),
                },
            )
        for earlier, later in ((prev, r), (r, nxt)):
            if earlier is None or later is None:
                continue
            lost = earlier.ids - later.ids
            lost_indeterminate = lost & indeterminate
            always(
                not (lost - lost_indeterminate),
                "client history: a later timestamp observes every write an earlier timestamp observed",
                {
                    "earlier": earlier.op_id,
                    "later": later.op_id,
                    "earlier_ts": earlier.ts,
                    "later_ts": later.ts,
                    "lost_ids": _sample(lost - lost_indeterminate),
                },
            )
            always(
                not lost_indeterminate,
                "client history: an indeterminate write once observed stays observed at later timestamps",
                {
                    "earlier": earlier.op_id,
                    "later": later.op_id,
                    "lost_ids": _sample(lost_indeterminate),
                },
            )
            sometimes(
                bool(earlier.ids & known_indeterminate),
                "client history: a read observed a write whose outcome was indeterminate",
                {"read": earlier.op_id},
            )


def _check_real_time(db: sqlite3.Connection, new: list[Read]) -> None:
    """Strict serializable timestamps are monotone in real time, across sessions and epochs."""
    strict = _load_reads(db, "isolation = ?", (STRICT,), with_ids=False)
    strict.sort(key=lambda r: r.complete_rt)
    completes = [r.complete_rt for r in strict]
    prefix_max: list[Read] = []
    for r in strict:
        prefix_max.append(
            r if not prefix_max or r.ts >= prefix_max[-1].ts else prefix_max[-1]
        )
    for r in new:
        if r.isolation != STRICT:
            continue
        n = bisect.bisect_left(completes, r.invoke_rt)
        if n == 0:
            continue
        top = prefix_max[n - 1]
        always(
            top.ts <= r.ts,
            "client history: strict serializable read timestamps never go backwards in real time",
            {
                "read": r.op_id,
                "ts": r.ts,
                "earlier_read": top.op_id,
                "earlier_ts": top.ts,
                "same_session": top.session == r.session,
            },
        )
        latest = strict[n - 1]
        sometimes(
            r.uptime_s < r.invoke_rt - latest.complete_rt,
            "client history: strict serializable monotonicity checked across an environmentd restart",
            {"read": r.op_id, "earlier_read": latest.op_id, "uptime_s": r.uptime_s},
        )


def _check_sessions(db: sqlite3.Connection, new: list[Read]) -> None:
    sessions = {r.session for r in new if r.isolation == STRONG_SESSION}
    for s in sessions:
        reads = _load_reads(db, "session = ?", (s,), with_ids=False)
        reads.sort(key=lambda r: r.invoke_rt)
        for a, b in zip(reads, reads[1:]):
            if not b.checked:
                always_or_unreachable(
                    a.ts <= b.ts,
                    "client history: strong session serializable reads in one session never go backwards",
                    {"session": s, "earlier": a.op_id, "later": b.op_id},
                )


def client_history_check() -> int:
    db = open_state()
    new = _load_reads(db, "checked = 0", ())
    if not new:
        log("no new reads to check")
        return 0
    by_epoch: dict[int, list[Read]] = {}
    for r in new:
        by_epoch.setdefault(r.epoch, []).append(r)
    for epoch, reads in sorted(by_epoch.items()):
        _check_epoch(epoch, reads, db)
    _check_real_time(db, new)
    _check_sessions(db, new)
    with db:
        db.executemany(
            "UPDATE ops SET checked = 1 WHERE op_id = ?", [(r.op_id,) for r in new]
        )
    log(f"checked {len(new)} reads across {len(by_epoch)} epochs")
    return 0

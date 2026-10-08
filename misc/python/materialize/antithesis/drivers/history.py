# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Client-side history of appends and reads over several keys, and its checker.

Properties: `strict-serializable-client-history` and the data-level claims of
`acked-writes-survive-generation-handoff`.

Each epoch has `KEYS` append-only tables `client_history.k<key>_e<epoch>
(v, rn)`, one per key, so that a read spanning keys spans separate
collections whose timestamps the coordinator selects together. Three derived
read paths are maintained on `configure.SHARED_CLUSTER`: an index on key
`INDEXED_KEY`, a materialized view over key `MV_KEY`, and a materialized view
that full-outer-joins the two `JOIN_KEYS`. Values are globally unique, so the
join matches nothing and returns both keys' values.

`client_history_driver` runs sessions that

* append their op id to one key (`INSERT ... VALUES`),
* append their op id and the count of one key to another key, or the same
  one, in one `INSERT ... SELECT count(*)`: a read-modify-write whose read is
  recovered from the row it wrote,
* read a random subset of keys with `mz_now()`, through the tables and the
  derived paths, in one statement or in one read-only transaction.

Materialize rejects write transactions that touch two tables (`wrong set of
locks acquired`), so every write transaction appends to exactly one key.

A session connects through the public Service, or directly to an
environmentd pod of the active generation or of the other one during a
rollout. Whatever a pod answers joins the history, so a stale or read-only
generation must reject or answer consistently.

Every operation is recorded in the `history` state database before it is
sent, with invoke and completion instants on `time.monotonic()` and its
outcome. Real-time comparisons rely on `CLOCK_MONOTONIC` being shared by every
process in the workload container. `client_history_check` (anytime) runs
`serializability.check` over the epochs that gained completed operations since
the last check. `client_history_final` (finally) reads every key of the live
epochs once more and checks the whole history.

The tables rotate to a new epoch after `EPOCH_MAX_OPS` operations, which
bounds what one read returns and the size of each dependency graph. Graphs
are per epoch: an operation touches the tables of one epoch only, so a cycle
across epochs would need real-time edges in both directions between them and
is not searched for. Timestamp monotonicity checks span epochs.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    always_or_unreachable,
    reachable,
    sometimes,
    unreachable,
)

from materialize.antithesis import serializability as sz
from materialize.antithesis import sql, state
from materialize.antithesis.drivers import configure
from materialize.antithesis.environment import Environment
from materialize.antithesis.rng import rng

STATE_DB = "history"
SCHEMA = "client_history"
KEYS = 8
INDEXED_KEY = 0
MV_KEY = 1
JOIN_KEYS = (2, 3)
DERIVED_CLUSTER = configure.SHARED_CLUSTER

# Calibration: the budgets below are first guesses and need a fault-free
# baseline on one simulated core before they are trusted.
DRIVER_BUDGET_S = (10.0, 90.0)
"""Range of the wall-clock budget of one driver invocation."""
DRIVER_MAX_OPS = 400
EPOCH_MAX_OPS = 1500
"""Operations per epoch. Keeps each read to a few kilobytes and each
dependency graph to a few thousand nodes."""
CONNECT_DEADLINE_S = 30.0
DIRECT_CONNECT_DEADLINE_S = 5.0
"""A pod may be gone or not serving. The session falls back to the Service."""
STATEMENT_TIMEOUT_MS = 30_000
POD_REFRESH_S = 5.0
ACK_TO_READ_GAP_S = 0.1
"""A strict serializable read that starts this soon after a write was
acknowledged is the hard case for the visibility check."""
RECENT_ACTIVITY_WINDOW_S = 30.0
WATCHDOG_GRACE_S = 180.0
"""Time past the driver budget before the watchdog ends the process."""
ANYTIME_EPOCHS = 3
"""Most epochs one anytime check analyzes, newest first."""
CHECK_WATCHDOG_S = 600.0
FINAL_READ_DEADLINE_S = 300.0
LARGE_HISTORY_OPS = 500

STRICT = sz.STRICT
SERIALIZABLE = sz.SERIALIZABLE
STRONG_SESSION = sz.STRONG_SESSION
ISOLATIONS = (STRICT, SERIALIZABLE, STRONG_SESSION)

CYCLE_MESSAGES = {
    "G0": "client history: no G0 anomaly (cycle of ww edges)",
    "G1c": "client history: no G1c anomaly (cycle of ww and wr edges)",
    "G-single": "client history: no G-single anomaly (cycle with exactly one rw edge)",
    "G2": "client history: no G2 anomaly (cycle with rw edges)",
    "G0-realtime": "client history: no G0-realtime anomaly (cycle of ww and real-time edges)",
    "G1c-realtime": "client history: no G1c-realtime anomaly (cycle of ww, wr and real-time edges)",
    "G-single-realtime": "client history: no G-single-realtime anomaly (cycle with real-time edges and exactly one rw edge)",
    "G2-realtime": "client history: no G2-realtime anomaly (cycle with real-time and rw edges)",
}
assert set(CYCLE_MESSAGES) == {a.name for a in sz.ANOMALIES}


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
            # 'w' (append), 'rmw' (append of a count), or 'r'.
            " kind TEXT NOT NULL,"
            " session TEXT NOT NULL,"
            " isolation TEXT NOT NULL,"
            # 'service', or '<active|other>:<generation>' for a pod.
            " endpoint TEXT NOT NULL,"
            " invoke_rt REAL NOT NULL,"
            " complete_rt REAL,"
            # NULL while in flight, or forever if the process died: unknown.
            # Otherwise 'ok', 'rejected', or 'indeterminate'.
            " outcome TEXT,"
            " wkey INTEGER,"
            " rkey INTEGER,"
            " ts INTEGER,"
            " uptime_s REAL,"
            # Reads: JSON list of [key, path, values, [[value, count], ...]].
            " observed TEXT,"
            " checked INTEGER NOT NULL DEFAULT 0)"
        )
        db.execute("CREATE INDEX IF NOT EXISTS ops_epoch ON ops (epoch)")
        db.execute("CREATE INDEX IF NOT EXISTS ops_checked ON ops (checked, epoch)")
        db.execute(
            "CREATE TABLE IF NOT EXISTS timeline_params ("
            " slot INTEGER PRIMARY KEY CHECK (slot = 0), params TEXT NOT NULL)"
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


@dataclass(frozen=True)
class TimelineParams:
    """Swarm parameters, drawn once per timeline so timelines skew differently."""

    write_prob: float
    rmw_share: float
    """Fraction of writes that are read-modify-writes, in strict sessions."""
    isolation_weights: list[float]
    key_weights: list[float]
    direct_prob: float
    """Chance that a new session connects to a pod instead of the Service."""
    txn_prob: float
    """Chance that a read of several paths is a transaction, not one statement."""


def _timeline_params(db: sqlite3.Connection) -> TimelineParams:
    query = "SELECT params FROM timeline_params"
    if db.execute(query).fetchone() is None:
        params = {
            "write_prob": rng.choice([0.05, 0.3, 0.6, 0.9]),
            "rmw_share": rng.choice([0.0, 0.2, 0.5]),
            # Strict serializable is never left out: the real-time edges and
            # the acknowledged-write checks only apply to it.
            "isolation_weights": [
                rng.choice([1.0, 4.0]),
                rng.choice([0.0, 1.0]),
                rng.choice([0.0, 1.0]),
            ],
            "key_weights": [rng.choice([1.0, 1.0, 4.0]) for _ in range(KEYS)],
            "direct_prob": rng.choice([0.0, 0.1, 0.3]),
            "txn_prob": rng.choice([0.2, 0.5]),
        }
        with db:
            db.execute(
                "INSERT OR IGNORE INTO timeline_params VALUES (0, ?)",
                (json.dumps(params),),
            )
    row = db.execute(query).fetchone()
    return TimelineParams(**json.loads(row[0]))


def _table(epoch: int, key: int) -> str:
    return f"{SCHEMA}.k{key}_e{epoch}"


def _mv(epoch: int) -> str:
    return f"{SCHEMA}.mv{MV_KEY}_e{epoch}"


def _join_mv(epoch: int) -> str:
    return f"{SCHEMA}.mvj_e{epoch}"


@dataclass(frozen=True)
class ReadPath:
    name: str
    """`table`, `index`, `mv` or `join_mv`."""
    key: int
    relation: Callable[[int], str]
    value: str = "v"
    count: str = "rn"

    def aggregate(self, epoch: int) -> str:
        """One row: the key's values, and `value:count` for read-modify-write rows."""
        where = f" WHERE {self.value} IS NOT NULL" if self.name == "join_mv" else ""
        return (
            f"(SELECT string_agg({self.value}::text, ',') AS vs,"
            f" string_agg(CASE WHEN {self.count} IS NOT NULL THEN"
            f" {self.value}::text || ':' || {self.count}::text END, ',') AS cs"
            f" FROM {self.relation(epoch)}{where})"
        )


def _paths(key: int, on_derived_cluster: bool) -> list[ReadPath]:
    """Paths that read `key`. The index serves reads only on its own cluster."""
    table = ReadPath("table", key, lambda e: _table(e, key))
    if key == INDEXED_KEY:
        return (
            [ReadPath("index", key, table.relation)] if on_derived_cluster else [table]
        )
    if key == MV_KEY:
        return [table, ReadPath("mv", key, _mv)]
    if key in JOIN_KEYS:
        side = "a" if key == JOIN_KEYS[0] else "b"
        return [table, ReadPath("join_mv", key, _join_mv, f"{side}v", f"{side}rn")]
    return [table]


def _create_epoch(host: str) -> Callable[[int], bool]:
    def create(epoch: int) -> bool:
        a, b = (_table(epoch, k) for k in JOIN_KEYS)
        statements = [
            f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
            *(
                f"CREATE TABLE IF NOT EXISTS {_table(epoch, k)}"
                " (v bigint NOT NULL, rn bigint)"
                for k in range(KEYS)
            ),
            f"CREATE INDEX IF NOT EXISTS k{INDEXED_KEY}_e{epoch}_idx"
            f" IN CLUSTER {DERIVED_CLUSTER} ON {_table(epoch, INDEXED_KEY)} (v)",
            f"CREATE MATERIALIZED VIEW IF NOT EXISTS {_mv(epoch)}"
            f" IN CLUSTER {DERIVED_CLUSTER}"
            f" AS SELECT v, rn FROM {_table(epoch, MV_KEY)}",
            f"CREATE MATERIALIZED VIEW IF NOT EXISTS {_join_mv(epoch)}"
            f" IN CLUSTER {DERIVED_CLUSTER}"
            " AS SELECT a.v AS av, a.rn AS arn, b.v AS bv, b.rn AS brn"
            f" FROM {a} AS a FULL OUTER JOIN {b} AS b ON a.v = b.v",
        ]
        try:
            with sql.connection(
                host, statement_timeout_ms=STATEMENT_TIMEOUT_MS
            ) as conn:
                for statement in statements:
                    conn.execute(statement.encode())
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
                for k in range(KEYS):
                    conn.execute(
                        f"DROP TABLE IF EXISTS {_table(epoch, k)} CASCADE".encode()
                    )
            return True
        except (psycopg.Error, OSError):
            return False

    return drop


@dataclass(frozen=True)
class Target:
    host: str
    endpoint: str
    """`service`, or `<active|other>:<generation>`."""

    @property
    def is_other_generation(self) -> bool:
        return self.endpoint.startswith("other:")


class Targets:
    """Picks where a new session connects."""

    def __init__(self, service_host: str, direct_prob: float) -> None:
        self.service = Target(service_host, "service")
        self.direct_prob = direct_prob
        self._kube: Any = None
        self._pods: list[Target] = []
        self._pods_at = float("-inf")

    def choose(self) -> Target:
        if rng.random() >= self.direct_prob:
            return self.service
        pods = self._pod_targets()
        others = [t for t in pods if t.is_other_generation]
        actives = [t for t in pods if not t.is_other_generation]
        if others and (not actives or rng.random() < 0.5):
            return rng.choice(others)
        return rng.choice(actives) if actives else self.service

    def _pod_targets(self) -> list[Target]:
        if time.monotonic() - self._pods_at < POD_REFRESH_S:
            return self._pods
        # NOTE: imported here because `rollouts` imports this module, and
        # `counters` (which `rollouts` also imports) imports names from it.
        from materialize.antithesis.drivers import rollouts

        self._pods_at = time.monotonic()
        try:
            if self._kube is None:
                self._kube = rollouts.Kube(Environment())
            active = self._kube.snapshot().active
            pods = self._kube.envd_pods()
        except rollouts.TRANSIENT_ERRORS as e:
            log(f"listing environmentd pods failed: {e}")
            self._pods = []
            return self._pods
        # The role is the CR's view when the pod was listed. A promotion can
        # flip it before the session's statements run, which the checker
        # tolerates: every endpoint must answer consistently.
        self._pods = (
            []
            if active is None
            else [
                Target(
                    p.ip,
                    f"{'active' if p.generation == active else 'other'}:{p.generation}",
                )
                for p in pods
                if p.ip
            ]
        )
        return self._pods


class Session:
    """One SQL session at one isolation level. Reconnecting starts a new session."""

    def __init__(self, targets: Targets, weights: list[float]) -> None:
        self.targets = targets
        self.weights = weights
        self.conn: psycopg.Connection | None = None
        self.name = ""
        self.isolation = STRICT
        self.target = targets.service

    def ensure(self) -> psycopg.Connection:
        if self.conn is not None and not self.conn.closed:
            return self.conn
        target = self.targets.choose()
        try:
            conn = sql.connect_with_retry(
                target.host,
                (
                    CONNECT_DEADLINE_S
                    if target == self.targets.service
                    else DIRECT_CONNECT_DEADLINE_S
                ),
                interval=1.0,
                statement_timeout_ms=STATEMENT_TIMEOUT_MS,
            )
        except (psycopg.Error, OSError) as e:
            if target == self.targets.service:
                raise
            log(f"connecting to {target.endpoint} failed ({e}), using the Service")
            target = self.targets.service
            conn = sql.connect_with_retry(
                target.host,
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
        self.target = target
        self.name = f"{time.monotonic_ns()}-{rng.getrandbits(32):08x}"
        return conn

    def reset(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            except psycopg.Error:
                pass
        self.conn = None


def _epoch_for_op(db: sqlite3.Connection, host: str, force_rotate: bool) -> int | None:
    epoch = current_generation(db)
    if epoch is not None and not force_rotate:
        row = db.execute(
            "SELECT count(*) FROM ops WHERE epoch = ?", (epoch,)
        ).fetchone()
        if int(row[0]) < EPOCH_MAX_OPS:
            return epoch
    return rotate_generation(db, epoch, "", _create_epoch(host), _drop_epoch(host))


def _begin(
    db: sqlite3.Connection,
    session: Session,
    epoch: int,
    kind: str,
    wkey: int | None = None,
    rkey: int | None = None,
) -> tuple[int, float]:
    invoke = time.monotonic()
    with db:
        cur = db.execute(
            "INSERT INTO ops (epoch, kind, session, isolation, endpoint, invoke_rt,"
            " wkey, rkey) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                epoch,
                kind,
                session.name,
                session.isolation,
                session.target.endpoint,
                invoke,
                wkey,
                rkey,
            ),
        )
    assert cur.lastrowid is not None
    return int(cur.lastrowid), invoke


def _write(
    db: sqlite3.Connection,
    session: Session,
    epoch: int,
    key: int,
    read_key: int | None,
) -> bool:
    """Append one fresh value to `key`, counting `read_key` if given.

    Returns whether the epoch's tables are missing on the active generation.
    """
    conn = session.ensure()
    kind = "w" if read_key is None else "rmw"
    op_id, _ = _begin(db, session, epoch, kind, key, read_key)
    if read_key is None:
        statement = f"INSERT INTO {_table(epoch, key)} (v) VALUES ({op_id})"
        message = "client history: INSERT of a fresh id returns only classified errors"
    else:
        statement = (
            f"INSERT INTO {_table(epoch, key)} (v, rn)"
            f" SELECT {op_id}, count(*) FROM {_table(epoch, read_key)}"
        )
        message = "client history: read-modify-write INSERT ... SELECT returns only classified errors"
    outcome = "ok"
    missing = False
    try:
        conn.execute(statement.encode())
    except Exception as e:
        c = sql.classify(e)
        if c.outcome == sql.Outcome.REJECTED:
            outcome = "rejected"
            missing = (
                c.race is sql.CatalogRace.MISSING
                and not session.target.is_other_generation
            )
        else:
            if c.outcome == sql.Outcome.VIOLATION:
                unreachable(
                    message,
                    {
                        "sqlstate": c.sqlstate,
                        "template": c.template,
                        "op_id": op_id,
                        "endpoint": session.target.endpoint,
                    },
                )
            outcome = "indeterminate"
            session.reset()
    complete = time.monotonic()
    with db:
        db.execute(
            "UPDATE ops SET complete_rt = ?, outcome = ? WHERE op_id = ?",
            (complete, outcome, op_id),
        )
    if session.target.is_other_generation:
        sometimes(
            outcome == "rejected",
            "client history: a non-active environmentd generation rejected a write",
            {"op_id": op_id, "endpoint": session.target.endpoint},
        )
    return missing


def _parse_values(text: str | None) -> list[int]:
    return [int(x) for x in text.split(",")] if text else []


def _parse_counts(text: str | None) -> list[list[int]]:
    if not text:
        return []
    return [[int(v), int(n)] for v, n in (pair.split(":") for pair in text.split(","))]


def _choose_paths(on_derived_cluster: bool) -> list[ReadPath]:
    n = rng.choices([1, 2, 3, KEYS], weights=[3, 3, 2, 1])[0]
    chosen: list[ReadPath] = []
    for key in sorted(rng.sample(range(KEYS), n)):
        options = _paths(key, on_derived_cluster)
        first = rng.choice(options)
        chosen.append(first)
        # Reading one key on two paths in one transaction checks that the
        # paths agree at one timestamp.
        rest = [p for p in options if p != first]
        if rest and rng.random() < 0.2:
            chosen.append(rng.choice(rest))
    return chosen


def _read(
    db: sqlite3.Connection, session: Session, epoch: int, txn_prob: float
) -> bool:
    """Read a subset of keys with `mz_now()`. Returns whether the epoch's
    tables are missing on the active generation."""
    conn = session.ensure()
    on_derived_cluster = rng.random() < 0.5
    paths = _choose_paths(on_derived_cluster)
    as_txn = len(paths) > 1 and rng.random() < txn_prob
    if on_derived_cluster:
        conn.execute(f"SET cluster = {DERIVED_CLUSTER}".encode())
    else:
        conn.execute("RESET cluster")
    op_id, _ = _begin(db, session, epoch, "r")
    header = "SELECT mz_now()::text, extract(epoch FROM mz_uptime())::float8"
    rows: list[tuple[Any, ...]] = []
    in_txn = False
    try:
        if as_txn:
            conn.execute("BEGIN READ ONLY")
            in_txn = True
            for path in paths:
                row = conn.execute(
                    f"{header}, p.vs, p.cs FROM {path.aggregate(epoch)} AS p".encode()
                ).fetchone()
                assert row is not None
                rows.append(row)
            conn.execute("COMMIT")
            in_txn = False
        else:
            columns = ", ".join(f"p{i}.vs, p{i}.cs" for i in range(len(paths)))
            sources = ", ".join(
                f"{p.aggregate(epoch)} AS p{i}" for i, p in enumerate(paths)
            )
            row = conn.execute(
                f"{header}, {columns} FROM {sources}".encode()
            ).fetchone()
            assert row is not None
            rows = [
                (row[0], row[1], row[2 + 2 * i], row[3 + 2 * i])
                for i in range(len(paths))
            ]
    except Exception as e:
        c = sql.classify(e)
        if c.outcome == sql.Outcome.VIOLATION:
            unreachable(
                "client history: mz_now() read of the id set returns only classified errors",
                {
                    "sqlstate": c.sqlstate,
                    "template": c.template,
                    "op_id": op_id,
                    "endpoint": session.target.endpoint,
                    "paths": [p.name for p in paths],
                    "transaction": as_txn,
                },
            )
        if c.outcome == sql.Outcome.REJECTED and in_txn:
            try:
                conn.execute("ROLLBACK")
            except psycopg.Error:
                session.reset()
        elif c.outcome != sql.Outcome.REJECTED:
            session.reset()
        with db:
            db.execute(
                "UPDATE ops SET complete_rt = ?, outcome = ? WHERE op_id = ?",
                (
                    time.monotonic(),
                    (
                        "rejected"
                        if c.outcome == sql.Outcome.REJECTED
                        else "indeterminate"
                    ),
                    op_id,
                ),
            )
        return (
            c.race is sql.CatalogRace.MISSING and not session.target.is_other_generation
        )
    complete = time.monotonic()
    timestamps = sorted({int(r[0]) for r in rows})
    if as_txn:
        always_or_unreachable(
            len(timestamps) == 1,
            "client history: every statement of one read transaction observes one mz_now()",
            {
                "op_id": op_id,
                "timestamps": timestamps,
                "endpoint": session.target.endpoint,
            },
        )
    observed = [
        [path.key, path.name, _parse_values(r[2]), _parse_counts(r[3])]
        for path, r in zip(paths, rows)
    ]
    with db:
        db.execute(
            "UPDATE ops SET complete_rt = ?, outcome = 'ok', ts = ?, uptime_s = ?,"
            " observed = ? WHERE op_id = ?",
            (
                complete,
                timestamps[0],
                float(rows[0][1]),
                json.dumps(observed),
                op_id,
            ),
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
    params = _timeline_params(db)
    budget = rng.uniform(*DRIVER_BUDGET_S)
    start_watchdog(budget + WATCHDOG_GRACE_S)
    deadline = time.monotonic() + budget
    session = Session(Targets(host, params.direct_prob), params.isolation_weights)
    force_rotate = False
    ops = 0
    while ops < DRIVER_MAX_OPS and time.monotonic() < deadline:
        try:
            epoch = _epoch_for_op(db, host, force_rotate)
            force_rotate = False
            if epoch is None:
                time.sleep(1)
                continue
            session.ensure()
            # Serializable sessions only read: their writes would carry no
            # real-time or session guarantee the checker could use.
            if session.isolation != SERIALIZABLE and rng.random() < params.write_prob:
                key = rng.choices(range(KEYS), weights=params.key_weights)[0]
                read_key = None
                if session.isolation == STRICT and rng.random() < params.rmw_share:
                    read_key = rng.choices(range(KEYS), weights=params.key_weights)[0]
                force_rotate = _write(db, session, epoch, key, read_key)
            else:
                force_rotate = _read(db, session, epoch, params.txn_prob)
            ops += 1
            # Occasionally start a new session so per-session isolation,
            # endpoint, and reconnects vary within one invocation.
            if rng.random() < 0.05:
                session.reset()
        except (psycopg.Error, OSError) as e:
            log(f"transient error, reconnecting: {e}")
            session.reset()
            time.sleep(rng.uniform(0.1, 2.0))
    session.reset()
    log(f"finished {ops} operations")
    return 0


_KINDS = {"w": sz.Kind.APPEND, "rmw": sz.Kind.RMW, "r": sz.Kind.READ}
_OUTCOMES = {"ok": sz.Outcome.OK, "rejected": sz.Outcome.FAIL}

_OP_COLUMNS = (
    "op_id, kind, outcome, invoke_rt, complete_rt, session, isolation, wkey, rkey,"
    " observed, ts, uptime_s, endpoint"
)


def _to_op(row: tuple[Any, ...]) -> sz.Op:
    (
        op_id,
        kind,
        outcome,
        invoke,
        complete,
        session,
        isolation,
        wkey,
        rkey,
        observed,
        ts,
        uptime_s,
        endpoint,
    ) = row
    observations = tuple(
        sz.Observation(
            int(key),
            str(path),
            tuple(int(v) for v in values),
            tuple((int(v), int(n)) for v, n in counts),
        )
        for key, path, values, counts in (json.loads(observed) if observed else [])
    )
    return sz.Op(
        op_id=int(op_id),
        kind=_KINDS[kind],
        # In flight, indeterminate, or abandoned by a dead process.
        outcome=_OUTCOMES.get(outcome, sz.Outcome.INFO),
        invoke=float(invoke),
        complete=None if complete is None else float(complete),
        session=str(session),
        isolation=str(isolation),
        write_key=None if wkey is None else int(wkey),
        read_key=None if rkey is None else int(rkey),
        observations=observations,
        ts=None if ts is None else int(ts),
        uptime_s=None if uptime_s is None else float(uptime_s),
        endpoint=str(endpoint),
    )


def _load_epoch(db: sqlite3.Connection, epoch: int) -> list[sz.Op]:
    rows = db.execute(
        f"SELECT {_OP_COLUMNS} FROM ops WHERE epoch = ? ORDER BY op_id", (epoch,)
    ).fetchall()
    return [_to_op(r) for r in rows]


def _load_timed_reads(db: sqlite3.Connection) -> list[sz.Op]:
    rows = db.execute(
        "SELECT op_id, kind, outcome, invoke_rt, complete_rt, session, isolation,"
        " wkey, rkey, NULL, ts, uptime_s, endpoint FROM ops"
        " WHERE kind = 'r' AND outcome = 'ok' AND ts IS NOT NULL"
    ).fetchall()
    return [_to_op(r) for r in rows]


def _first(items: list[dict[str, Any]]) -> dict[str, Any] | None:
    return items[0] if items else None


def _assert_report(report: sz.Report, epoch: int, scope: str) -> None:
    base = {"epoch": epoch, "scope": scope}

    def details(items: list[dict[str, Any]]) -> dict[str, Any]:
        return {**base, "count": len(items), "first": _first(items)}

    always(
        not report.phantom_reads,
        "client history: reads observe only writes attempted before the read completed",
        details(report.phantom_reads),
    )
    always(
        not report.aborted_reads,
        "client history: writes rejected with a definite error are never observed",
        details(report.aborted_reads),
    )
    always(
        not report.duplicates,
        "client history: no written id appears twice in the table",
        details(report.duplicates),
    )
    always(
        not report.internal,
        "client history: one read observes one set per key on every read path",
        details(report.internal),
    )
    always(
        not report.incompatible_orders,
        "client history: the sets observed for one key are ordered by inclusion",
        details(report.incompatible_orders),
    )
    always(
        not report.rmw_self_reads,
        "client history: a read-modify-write never counts its own append",
        details(report.rmw_self_reads),
    )
    always(
        not report.stale_reads,
        "client history: strict serializable reads observe every write acknowledged before they began",
        details(report.stale_reads),
    )
    always(
        not report.lost_writes,
        "client history: no write is acknowledged and then lost",
        details(report.lost_writes),
    )
    always(
        not report.timestamp_regressions,
        "client history: a later timestamp observes every write an earlier timestamp observed",
        details(report.timestamp_regressions),
    )
    always(
        not report.indeterminate_regressions,
        "client history: an indeterminate write once observed stays observed at later timestamps",
        details(report.indeterminate_regressions),
    )
    always_or_unreachable(
        not report.same_timestamp_mismatches,
        "client history: reads at the same timestamp observe the same writes",
        details(report.same_timestamp_mismatches),
    )
    for anomaly in sz.ANOMALIES:
        cycles = report.cycles[anomaly.name]
        always(
            not cycles,
            CYCLE_MESSAGES[anomaly.name],
            {
                **base,
                "count": report.anomaly_counts[anomaly.name],
                "cycle": _first(cycles),
            },
        )

    stats = report.stats
    summary = {
        **base,
        "ops": stats.ops,
        "graph_nodes": stats.graph_nodes,
        "graph_edges": stats.graph_edges,
    }
    sometimes(
        stats.multi_key_strict_reads > 0,
        "client history: a multi-key strict serializable read was checked",
        {**summary, "reads": stats.multi_key_strict_reads},
    )
    sometimes(
        "index" in stats.paths,
        "client history: a read through the index was checked",
        summary,
    )
    sometimes(
        "mv" in stats.paths,
        "client history: a read through the materialized view was checked",
        summary,
    )
    sometimes(
        "join_mv" in stats.paths,
        "client history: a read through the join materialized view was checked",
        summary,
    )
    sometimes(
        stats.non_active_ops > 0,
        "client history: an operation on a non-active environmentd generation was checked",
        {**summary, "non_active_ops": stats.non_active_ops},
    )
    sometimes(
        stats.graph_nodes >= LARGE_HISTORY_OPS,
        "client history: a cycle search covered at least 500 operations",
        summary,
    )
    sometimes(
        stats.rmw_reads_placed > 0,
        "client history: a read-modify-write's read was placed in the version order",
        {**summary, "placed": stats.rmw_reads_placed},
    )
    sometimes(
        stats.strict_reads_after_ack > 0,
        "client history: a strict serializable read was checked against an earlier acknowledged write",
        summary,
    )
    sometimes(
        stats.min_ack_gap_s is not None and stats.min_ack_gap_s < ACK_TO_READ_GAP_S,
        "client history: strict serializable read began within 100 ms of a write acknowledgement",
        {**summary, "gap_s": stats.min_ack_gap_s},
    )
    sometimes(
        stats.indeterminate_observed > 0,
        "client history: a read observed a write whose outcome was indeterminate",
        summary,
    )
    sometimes(
        stats.inclusion_checks_nonempty > 0,
        "client history: snapshot inclusion was checked on a read that observed writes",
        summary,
    )


def _assert_timestamp_order(db: sqlite3.Connection, only: set[int] | None) -> None:
    reads = _load_timed_reads(db)
    realtime = sz.realtime_timestamp_order(reads, only)
    always(
        not realtime.regressions,
        "client history: strict serializable read timestamps never go backwards in real time",
        {"count": len(realtime.regressions), "first": _first(realtime.regressions)},
    )
    sometimes(
        realtime.across_restart > 0,
        "client history: strict serializable monotonicity checked across an environmentd restart",
        {"checked": realtime.checked},
    )
    session = sz.session_timestamp_order(reads, only)
    always_or_unreachable(
        not session.regressions,
        "client history: strong session serializable reads in one session never go backwards",
        {"count": len(session.regressions), "first": _first(session.regressions)},
    )


def _check_epoch(db: sqlite3.Connection, epoch: int, scope: str) -> list[int]:
    """Check one epoch. Returns the ids of the completed operations it covered."""
    ops = _load_epoch(db, epoch)
    started = time.monotonic()
    report = sz.check(ops)
    log(
        f"{scope}: epoch {epoch}: {len(ops)} ops, {report.stats.graph_nodes} nodes,"
        f" {report.stats.graph_edges} edges in {time.monotonic() - started:.1f}s"
    )
    _assert_report(report, epoch, scope)
    return [op.op_id for op in ops if op.complete is not None]


def _mark_checked(db: sqlite3.Connection, op_ids: list[int]) -> None:
    with db:
        db.executemany(
            "UPDATE ops SET checked = 1 WHERE op_id = ?", [(i,) for i in op_ids]
        )


def client_history_check() -> int:
    """Check the newest epochs that gained completed operations since the last check."""
    start_watchdog(CHECK_WATCHDOG_S)
    db = open_state()
    epochs = [
        int(r[0])
        for r in db.execute(
            "SELECT DISTINCT epoch FROM ops"
            " WHERE checked = 0 AND complete_rt IS NOT NULL"
            " ORDER BY epoch DESC LIMIT ?",
            (ANYTIME_EPOCHS,),
        )
    ]
    if not epochs:
        log("no new operations to check")
        return 0
    new_reads = {
        int(r[0])
        for r in db.execute(
            "SELECT op_id FROM ops WHERE checked = 0 AND kind = 'r' AND outcome = 'ok'"
        )
    }
    covered: list[int] = []
    for epoch in sorted(epochs):
        covered.extend(_check_epoch(db, epoch, "anytime"))
    _assert_timestamp_order(db, new_reads)
    _mark_checked(db, covered)
    log(f"checked epochs {sorted(epochs)}")
    return 0


def _final_read(db: sqlite3.Connection, host: str, epoch: int, deadline: float) -> bool:
    """Read every key of `epoch` through the Service, recorded like any read.

    Returns whether the read succeeded before `deadline`.
    """
    paths = [ReadPath("table", k, lambda e, k=k: _table(e, k)) for k in range(KEYS)]
    columns = ", ".join(f"p{i}.vs, p{i}.cs" for i in range(KEYS))
    sources = ", ".join(f"{p.aggregate(epoch)} AS p{i}" for i, p in enumerate(paths))
    statement = (
        "SELECT mz_now()::text, extract(epoch FROM mz_uptime())::float8,"
        f" {columns} FROM {sources}"
    )
    while time.monotonic() < deadline:
        op_id: int | None = None
        try:
            with sql.connection(
                host, statement_timeout_ms=STATEMENT_TIMEOUT_MS
            ) as conn:
                conn.execute(f"SET transaction_isolation = '{STRICT}'")
                invoke = time.monotonic()
                with db:
                    cur = db.execute(
                        "INSERT INTO ops (epoch, kind, session, isolation, endpoint,"
                        " invoke_rt) VALUES (?, 'r', 'final', ?, 'service', ?)",
                        (epoch, STRICT, invoke),
                    )
                op_id = cur.lastrowid
                row = conn.execute(statement.encode()).fetchone()
                complete = time.monotonic()
        except (psycopg.Error, OSError) as e:
            c = sql.classify(e)
            if op_id is not None:
                with db:
                    db.execute(
                        "UPDATE ops SET complete_rt = ?, outcome = ? WHERE op_id = ?",
                        (time.monotonic(), c.outcome.value, op_id),
                    )
            if c.outcome == sql.Outcome.VIOLATION:
                unreachable(
                    "client history: mz_now() read of the id set returns only classified errors",
                    {
                        "sqlstate": c.sqlstate,
                        "template": c.template,
                        "op_id": op_id,
                        "endpoint": "service",
                        "final": True,
                    },
                )
            if c.race is sql.CatalogRace.MISSING:
                log(f"final read: epoch {epoch} is gone: {c.template}")
                return False
            log(f"final read of epoch {epoch} failed, retrying: {c.template}")
            time.sleep(2)
            continue
        assert row is not None
        observed = [
            [k, "table", _parse_values(row[2 + 2 * k]), _parse_counts(row[3 + 2 * k])]
            for k in range(KEYS)
        ]
        with db:
            db.execute(
                "UPDATE ops SET complete_rt = ?, outcome = 'ok', ts = ?, uptime_s = ?,"
                " observed = ? WHERE op_id = ?",
                (complete, int(row[0]), float(row[1]), json.dumps(observed), op_id),
            )
        return True
    return False


def client_history_final() -> int:
    """Read every key of the live epochs once more, then check the whole history."""
    start_watchdog(FINAL_READ_DEADLINE_S + CHECK_WATCHDOG_S)
    db = open_state()
    epochs = [
        int(r[0]) for r in db.execute("SELECT DISTINCT epoch FROM ops ORDER BY epoch")
    ]
    if not epochs:
        log("no history to check")
        return 0
    dropped = {int(r[0]) for r in db.execute("SELECT gen FROM dropped_generations")}
    try:
        host = Environment().sql_host()
    except Exception as e:
        log(f"cannot locate the environment, checking without final reads: {e}")
        host = None
    deadline = time.monotonic() + FINAL_READ_DEADLINE_S
    for epoch in epochs:
        if epoch in dropped or host is None:
            continue
        done = _final_read(db, host, epoch, deadline)
        sometimes(
            done,
            "client history: a final read covered every key of a live epoch",
            {"epoch": epoch},
        )
    covered: list[int] = []
    for epoch in epochs:
        covered.extend(_check_epoch(db, epoch, "finally"))
    _assert_timestamp_order(db, None)
    _mark_checked(db, covered)
    return 0

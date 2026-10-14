# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Incremental view maintenance checked continuously through `SUBSCRIBE`.

Property: `ivm-subscribe-matches-reference`.

Objects, all in schema `ivm` and maintained on `ivm_c` (replication factor 1):
the base tables and view kinds in `ivm_model` (filter and map, inner, left and
full outer join, grouped and global reduce, distinct, TopK, a temporal filter,
and a `WITH MUTUALLY RECURSIVE` closure), each kind both as an indexed view and
as a materialized view. Tables, indexes and MVs retain history, so a subscribe
can start in the past.

`load_main` inserts, updates and deletes rows of every base table. Each write
is recorded in the `writes` ledger before it is sent, and rows of `t` and `d`
carry the ledger id of the write that produced them, so the checker can tie a
row to its write and learn the write's commit timestamp from the stream.

`check_main` runs windows. Each window subscribes, `AS OF` a drawn timestamp,
to `ivm_model.subscribe_query()`: one union of the base tables and every view,
tagged by relation, so the one stream is a consistent cut of inputs and
outputs. At every closed timestamp it compares each view with the Python
reference over the base tables, and it checks frontier discipline and
multiplicities. A window first peeks the same query `AS OF` its start, and a
window may start `AS OF` the last timestamp an earlier window closed, which
must reproduce the state that window saw there. A dropped connection ends the
window; the next one reconnects.
"""

from __future__ import annotations

import json
import sqlite3
import time
from collections import Counter
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

from materialize.antithesis import ivm_model as model
from materialize.antithesis import quiet, sql, state
from materialize.antithesis.drivers import configure, history, pod_restarts
from materialize.antithesis.drivers.recovery import observe_pods
from materialize.antithesis.drivers.rollouts import (
    GRACE_MENU,
    K8S_TIMEOUT_SECONDS,
    Kube,
)
from materialize.antithesis.drivers.session_mix import classify_read, quote_literal
from materialize.antithesis.drivers.sources_common import (
    ensure_retain_history,
    is_since_race,
    mz_now_ms,
    pick_as_of,
    timeline_choice,
)
from materialize.antithesis.environment import Environment
from materialize.antithesis.rng import rng

STATE_DB = "ivm"
SCHEMA = model.SCHEMA
CLUSTER = "ivm_c"
CLUSTER_SIZE = "antithesis-1"
META_CLUSTER = configure.SHARED_CLUSTER
"""Catalog and frontier reads run here, so they answer while `ivm_c` rehydrates."""
RETAIN_MENU = ("2m", "10m")

KEYSPACE = 48
GROUPS = 6
DIM_GROUPS = GROUPS + 2
"""`d` also holds groups `t` never uses, so the full outer join has unmatched
rows on both sides."""
NODES = 6
MAX_ROWS = {"t": 120, "d": 12, "ev": 30, "e": 20}
EXPIRY_OFFSET_MS = (-5_000, 45_000)
"""Offsets of `ev.expires` from envd's clock. With `TEMPORAL_LEAD_MS`, rows
appear and disappear on their own within a window."""

# Calibration: first guesses for one simulated core, to be replaced by a
# fault-free baseline.
CHECK_BUDGET_S = 150.0
WINDOW_MENU_S = (20.0, 45.0, 90.0)
MIN_WINDOW_S = 10.0
KILL_MIN_WINDOW_S = 45.0
"""A window that deletes the replica pod must outlast its rehydration."""
KILL_P_MENU = (0.0, 0.15, 0.4)
"""Per-window chance, drawn per timeline, that the checker deletes the `ivm_c` pod."""
RESUME_P = 0.5
FETCH_TIMEOUT = "1s"
STATEMENT_TIMEOUT_MS = 45_000
CONNECT_DEADLINE_S = 30.0
LOAD_BUDGET_S = (10.0, 60.0)
LOAD_OPS_MENU = (10, 50, 200)
SETUP_OPS = 40
WATCHDOG_GRACE_S = 120.0

KIND_EQUAL = {
    "filter_map": "ivm: the filter_map view equals the reference at every closed timestamp",
    "inner_join": "ivm: the inner_join view equals the reference at every closed timestamp",
    "left_join": "ivm: the left_join view equals the reference at every closed timestamp",
    "full_join": "ivm: the full_join view equals the reference at every closed timestamp",
    "reduce": "ivm: the reduce view equals the reference at every closed timestamp",
    "global_reduce": "ivm: the global_reduce view equals the reference at every closed timestamp",
    "distinct": "ivm: the distinct view equals the reference at every closed timestamp",
    "topk": "ivm: the topk view equals the reference at every closed timestamp",
    "temporal": "ivm: the temporal view equals the reference at every closed timestamp",
    "recursive": "ivm: the recursive view equals the reference at every closed timestamp",
}
KIND_NONEMPTY = {
    "filter_map": "ivm: compared a non-empty filter_map view at a closed timestamp",
    "inner_join": "ivm: compared a non-empty inner_join view at a closed timestamp",
    "left_join": "ivm: compared a non-empty left_join view at a closed timestamp",
    "full_join": "ivm: compared a non-empty full_join view at a closed timestamp",
    "reduce": "ivm: compared a non-empty reduce view at a closed timestamp",
    "global_reduce": "ivm: compared a non-empty global_reduce view at a closed timestamp",
    "distinct": "ivm: compared a non-empty distinct view at a closed timestamp",
    "topk": "ivm: compared a non-empty topk view at a closed timestamp",
    "temporal": "ivm: compared a non-empty temporal view at a closed timestamp",
    "recursive": "ivm: compared a non-empty recursive view at a closed timestamp",
}
assert set(KIND_EQUAL) == set(KIND_NONEMPTY) == set(model.VIEWS_BY_KIND)


def log(message: str) -> None:
    print(f"ivm: {message}", flush=True)


def open_state() -> sqlite3.Connection:
    db = state.open_db(STATE_DB)
    with db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS writes ("
            " op_id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " kind TEXT NOT NULL,"
            " invoke_rt REAL NOT NULL,"
            " complete_rt REAL,"
            " outcome TEXT NOT NULL,"
            " commit_ts INTEGER)"
        )
        db.execute(
            "CREATE TABLE IF NOT EXISTS resume ("
            " slot INTEGER PRIMARY KEY CHECK (slot = 0),"
            " t INTEGER NOT NULL, gen INTEGER, state TEXT NOT NULL)"
        )
        db.execute(
            "CREATE TABLE IF NOT EXISTS generations_seen (gen INTEGER NOT NULL, ts INTEGER NOT NULL)"
        )
    return db


def _connect(host: str, cluster: str, **kwargs: Any) -> psycopg.Connection:
    kwargs.setdefault("statement_timeout_ms", STATEMENT_TIMEOUT_MS)
    options = dict(kwargs.pop("options", None) or {})
    options.setdefault("cluster", cluster)
    return sql.connect_with_retry(host, CONNECT_DEADLINE_S, options=options, **kwargs)


def _ddl(conn: psycopg.Connection, statement: str) -> None:
    try:
        conn.execute(statement.encode())
    except psycopg.Error as e:
        if sql.classify(e).race is not sql.CatalogRace.EXISTS:
            raise


def _object_names() -> list[str]:
    names = [t.name for t in model.TABLES]
    for v in model.VIEWS:
        names += [f"{v.kind}_v", f"{v.kind}_v_idx", f"{v.kind}_mv"]
    return names


def _objects_present(conn: psycopg.Connection) -> bool:
    names = _object_names()
    row = conn.execute(
        "SELECT"
        " (SELECT count(*) FROM mz_catalog.mz_clusters WHERE name = %s),"
        " (SELECT count(DISTINCT o.name) FROM mz_catalog.mz_objects o"
        "  JOIN mz_catalog.mz_schemas s ON o.schema_id = s.id"
        "  JOIN mz_catalog.mz_databases d ON s.database_id = d.id"
        "  WHERE d.name = 'materialize' AND s.name = %s AND o.name = ANY(%s::text[]))",
        (CLUSTER, SCHEMA, names),
    ).fetchone()
    return row is not None and int(row[0]) == 1 and int(row[1]) == len(names)


def ensure_objects(host: str, db: sqlite3.Connection) -> bool:
    """Create the cluster, tables, views, indexes and MVs if any is missing. Idempotent."""
    if not ensure_retain_history(host):
        return False
    retain = timeline_choice(db, "retain_history", RETAIN_MENU)
    assert retain in RETAIN_MENU
    history_opt = f"RETAIN HISTORY = FOR '{retain}'"
    try:
        with _connect(host, META_CLUSTER, statement_timeout_ms=60_000) as conn:
            if _objects_present(conn):
                return True
            _ddl(conn, f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
            if (
                conn.execute(
                    "SELECT 1 FROM mz_clusters WHERE name = %s", (CLUSTER,)
                ).fetchone()
                is None
            ):
                _ddl(
                    conn,
                    f"CREATE CLUSTER {CLUSTER} (SIZE {quote_literal(CLUSTER_SIZE)},"
                    " REPLICATION FACTOR 1)",
                )
            for t in model.TABLES:
                _ddl(
                    conn,
                    f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{t.name} ({t.ddl})"
                    f" WITH ({history_opt})",
                )
            for v in model.VIEWS:
                _ddl(conn, f"CREATE VIEW IF NOT EXISTS {SCHEMA}.{v.kind}_v AS {v.sql}")
                _ddl(
                    conn,
                    f"CREATE INDEX IF NOT EXISTS {v.kind}_v_idx IN CLUSTER {CLUSTER}"
                    f" ON {SCHEMA}.{v.kind}_v ({v.columns[0]}) WITH ({history_opt})",
                )
                _ddl(
                    conn,
                    f"CREATE MATERIALIZED VIEW IF NOT EXISTS {SCHEMA}.{v.kind}_mv"
                    f" IN CLUSTER {CLUSTER} WITH ({history_opt}) AS {v.sql}",
                )
        return True
    except (psycopg.Error, OSError) as e:
        c = sql.classify(e)
        if c.outcome is sql.Outcome.VIOLATION:
            unreachable(
                "ivm: setup DDL returns only classified errors",
                {"sqlstate": c.sqlstate, "template": c.template},
            )
        log(f"setup failed: {c.template}")
        return False


@dataclass
class Writer:
    """Issues random DML against the base tables, recording each write first."""

    db: sqlite3.Connection
    conn: psycopg.Connection
    counts: dict[str, int]
    now_ms: int

    def refresh(self) -> None:
        row = self.conn.execute(
            f"SELECT (SELECT count(*) FROM {SCHEMA}.t), (SELECT count(*) FROM {SCHEMA}.d),"
            f" (SELECT count(*) FROM {SCHEMA}.ev), (SELECT count(*) FROM {SCHEMA}.e)".encode()
        ).fetchone()
        assert row is not None
        self.counts = dict(zip(("t", "d", "ev", "e"), (int(x) for x in row)))
        self.now_ms = mz_now_ms(self.conn)

    def pick(self) -> str:
        full = [name for name, n in self.counts.items() if n >= MAX_ROWS[name]]
        if full:
            return {
                "t": "t_delete_range",
                "d": "d_delete",
                "ev": "ev_delete",
                "e": "e_delete",
            }[rng.choice(full)]
        return rng.choice(
            (
                "t_insert",
                "t_insert",
                "t_insert_many",
                "t_update",
                "t_move",
                "t_delete",
                "t_delete_range",
                "d_insert",
                "d_update",
                "d_delete",
                "ev_insert",
                "ev_insert",
                "ev_delete",
                "e_insert",
                "e_insert",
                "e_delete",
            )
        )

    def statement(self, kind: str, op: int) -> tuple[str, tuple[Any, ...]]:
        s = SCHEMA
        if kind == "t_insert":
            return f"INSERT INTO {s}.t VALUES (%s, %s, %s, %s)", (
                rng.randrange(KEYSPACE),
                rng.randrange(GROUPS),
                _value(),
                op,
            )
        if kind == "t_insert_many":
            n = rng.randint(2, 5)
            params: list[Any] = []
            for _ in range(n):
                params += [rng.randrange(KEYSPACE), rng.randrange(GROUPS), _value(), op]
            values = ", ".join(["(%s, %s, %s, %s)"] * n)
            return f"INSERT INTO {s}.t VALUES {values}", tuple(params)
        if kind == "t_update":
            return f"UPDATE {s}.t SET v = %s, w = %s WHERE k = %s", (
                _value(),
                op,
                rng.randrange(KEYSPACE),
            )
        if kind == "t_move":
            return f"UPDATE {s}.t SET g = %s, w = %s WHERE k = %s", (
                rng.randrange(GROUPS),
                op,
                rng.randrange(KEYSPACE),
            )
        if kind == "t_delete":
            return f"DELETE FROM {s}.t WHERE k = %s", (rng.randrange(KEYSPACE),)
        if kind == "t_delete_range":
            lo = rng.randrange(KEYSPACE)
            return f"DELETE FROM {s}.t WHERE k BETWEEN %s AND %s", (
                lo,
                lo + rng.choice((2, 8, 16)),
            )
        if kind == "d_insert":
            return f"INSERT INTO {s}.d VALUES (%s, %s)", (rng.randrange(DIM_GROUPS), op)
        if kind == "d_update":
            return f"UPDATE {s}.d SET label = %s WHERE g = %s", (
                op,
                rng.randrange(DIM_GROUPS),
            )
        if kind == "d_delete":
            return f"DELETE FROM {s}.d WHERE g = %s", (rng.randrange(DIM_GROUPS),)
        if kind == "ev_insert":
            expires = self.now_ms + rng.randint(*EXPIRY_OFFSET_MS)
            return f"INSERT INTO {s}.ev VALUES (%s, %s)", (
                rng.randrange(KEYSPACE),
                expires,
            )
        if kind == "ev_delete":
            return f"DELETE FROM {s}.ev WHERE k = %s", (rng.randrange(KEYSPACE),)
        if kind == "e_insert":
            return f"INSERT INTO {s}.e VALUES (%s, %s)", (
                rng.randrange(NODES),
                rng.randrange(NODES),
            )
        assert kind == "e_delete"
        return f"DELETE FROM {s}.e WHERE src = %s", (rng.randrange(NODES),)

    def write(self) -> bool:
        """Run one recorded write. Returns whether the connection is still usable."""
        kind = self.pick()
        with self.db:
            op = self.db.execute(
                "INSERT INTO writes (kind, invoke_rt, outcome) VALUES (?, ?, 'pending')",
                (kind, time.monotonic()),
            ).lastrowid
        assert op is not None
        text, params = self.statement(kind, op)
        usable = True
        try:
            self.conn.execute(text.encode(), params)
            outcome = "ok"
        except Exception as e:
            c = classify_read(e)
            if c.outcome is sql.Outcome.VIOLATION:
                unreachable(
                    "ivm writes: DML returns only classified errors",
                    {"kind": kind, "sqlstate": c.sqlstate, "template": c.template},
                )
            outcome = (
                "rejected" if c.outcome is sql.Outcome.REJECTED else "indeterminate"
            )
            usable = not self.conn.closed and outcome == "rejected"
        with self.db:
            self.db.execute(
                "UPDATE writes SET complete_rt = ?, outcome = ? WHERE op_id = ?",
                (time.monotonic(), outcome, op),
            )
        return usable


def _value() -> int:
    return rng.randrange(-1000, 1001)


def run_writes(host: str, db: sqlite3.Connection, ops: int, budget_s: float) -> int:
    """Issue up to `ops` writes within `budget_s`. Returns how many were sent."""
    end = time.monotonic() + budget_s
    writer: Writer | None = None
    sent = 0
    for i in range(ops):
        if time.monotonic() >= end:
            break
        try:
            if writer is None:
                writer = Writer(db, _connect(host, CLUSTER), {}, 0)
                writer.refresh()
            elif i % 20 == 0:
                writer.refresh()
        except Exception as e:
            c = classify_read(e)
            if c.outcome is sql.Outcome.VIOLATION:
                unreachable(
                    "ivm writes: DML returns only classified errors",
                    {"kind": "refresh", "sqlstate": c.sqlstate, "template": c.template},
                )
            if writer is not None:
                writer.conn.close()
            writer = None
            continue
        sent += 1
        if not writer.write():
            writer.conn.close()
            writer = None
    if writer is not None:
        writer.conn.close()
    return sent


def setup_main() -> int:
    """Create the objects and seed the tables. Run from `first_configure`; the
    driver and the check repair setup lazily if this did not finish."""
    try:
        host = Environment().sql_host()
    except Exception as e:
        log(f"no environmentd host: {e}")
        return 0
    db = open_state()
    try:
        if ensure_objects(host, db):
            run_writes(host, db, SETUP_OPS, 60.0)
    finally:
        db.close()
    return 0


def load_main() -> int:
    budget = rng.uniform(*LOAD_BUDGET_S)
    history.start_watchdog(budget + STATEMENT_TIMEOUT_MS / 1000 + WATCHDOG_GRACE_S)
    try:
        host = Environment().sql_host()
    except Exception as e:
        log(f"no environmentd host: {e}")
        return 0
    db = open_state()
    if not ensure_objects(host, db):
        return 0
    sent = run_writes(host, db, rng.choice(LOAD_OPS_MENU), budget)
    log(f"sent {sent} writes")
    return 0


def _active_generation(env: Environment) -> int | None:
    try:
        snap = Kube(env).try_snapshot()
    except Exception as e:
        log(f"reading the CR failed: {e}")
        return None
    return None if snap is None else snap.active


def _cluster_id(meta: psycopg.Connection) -> str | None:
    row = meta.execute(
        "SELECT id FROM mz_clusters WHERE name = %s", (CLUSTER,)
    ).fetchone()
    return None if row is None else str(row[0])


def _replica_signature(
    env: Environment, meta: psycopg.Connection, cid: str
) -> str | None:
    """A value that changes when any `ivm_c` replica process restarts, or None if unknown."""
    rows = meta.execute(
        "SELECT r.id, max(h.occurred_at)::text FROM mz_catalog.mz_cluster_replicas r"
        " LEFT JOIN mz_internal.mz_cluster_replica_status_history h"
        "  ON h.replica_id = r.id AND h.status = 'offline'"
        " WHERE r.cluster_id = %s GROUP BY r.id ORDER BY r.id",
        (cid,),
    ).fetchall()
    try:
        pods = observe_pods(env)
    except Exception as e:
        log(f"pod observation failed: {e}")
        return None
    marker = f"cluster-{cid}-replica-"
    procs = sorted(
        (p.pod_uid, p.container, p.restart_count) for p in pods if marker in p.pod
    )
    return json.dumps({"offline": [list(r) for r in rows], "pods": procs})


def _kill_replica_pod(env: Environment, cid: str) -> bool:
    if quiet.active_quiet_period():
        return False
    kube = Kube(env)
    marker = f"cluster-{cid}-replica-"
    pods = [
        p
        for p in kube.core.list_namespaced_pod(
            kube.namespace,
            _request_timeout=K8S_TIMEOUT_SECONDS,  # pyright: ignore[reportCallIssue]
        ).items
        if p.metadata is not None
        and p.metadata.name
        and marker in p.metadata.name
        and p.metadata.deletion_timestamp is None
    ]
    if not pods:
        return False
    victim = rng.choice(pods)
    assert victim.metadata is not None and victim.metadata.name is not None
    acked = pod_restarts.delete_recorded(
        kube,
        origin="ivm_check",
        role="clusterd",
        namespace=kube.namespace,
        pod=victim.metadata.name,
        uid=victim.metadata.uid,
        grace=rng.choice(GRACE_MENU),
        reason="ivm subscribe window",
    )
    if acked:
        reachable(
            "ivm: the checker deleted the ivm_c replica pod mid-window",
            {"pod": victim.metadata.name},
        )
    return acked


def _frontiers(meta: psycopg.Connection) -> tuple[list[int | None], list[int | None]]:
    rows = meta.execute(
        "SELECT f.read_frontier::text, f.write_frontier::text"
        " FROM mz_internal.mz_frontiers f"
        " JOIN mz_catalog.mz_objects o ON o.id = f.object_id"
        " JOIN mz_catalog.mz_schemas s ON o.schema_id = s.id"
        " JOIN mz_catalog.mz_databases d ON s.database_id = d.id"
        " WHERE d.name = 'materialize' AND s.name = %s",
        (SCHEMA,),
    ).fetchall()
    sinces = [None if r is None else int(r) for r, _ in rows]
    uppers = [None if w is None else int(w) for _, w in rows]
    return sinces, uppers


def _row(r: tuple[Any, ...]) -> tuple[Any, ...]:
    return tuple(None if x is None else int(x) for x in r)


def _peek(conn: psycopg.Connection, as_of: int) -> dict[str, model.Bag]:
    rows = conn.execute(
        f"SELECT tag, c1, c2, c3, c4, c5 FROM ({model.subscribe_query()}) AS q"
        f" AS OF {as_of}".encode()
    ).fetchall()
    out: dict[str, model.Bag] = {}
    for r in rows:
        out.setdefault(str(r[0]), Counter())[_row(r[1:])] += 1
    return out


@dataclass
class Resume:
    t: int
    gen: int | None
    state: dict[str, model.Bag]


def _load_resume(db: sqlite3.Connection) -> Resume | None:
    row = db.execute("SELECT t, gen, state FROM resume WHERE slot = 0").fetchone()
    if row is None:
        return None
    return Resume(
        int(row[0]),
        None if row[1] is None else int(row[1]),
        model.decode_state(json.loads(row[2])),
    )


def _save_resume(
    db: sqlite3.Connection, t: int, gen: int | None, st: dict[str, model.Bag]
) -> None:
    with db:
        db.execute(
            "INSERT OR REPLACE INTO resume (slot, t, gen, state) VALUES (0, ?, ?, ?)",
            (t, gen, json.dumps(model.encode_state(st))),
        )


class Window:
    """One subscribe from `as_of` until its deadline or a connection loss."""

    def __init__(
        self,
        env: Environment,
        host: str,
        db: sqlite3.Connection,
        budget: float,
        kill_p: float,
        gen: int | None,
    ) -> None:
        self.env = env
        self.host = host
        self.db = db
        self.budget = budget
        self.gen = gen
        """The active environmentd generation when the window started, if known."""
        self.phase = "setup"
        self.kill_at: float | None = None
        if budget >= KILL_MIN_WINDOW_S and rng.random() < kill_p:
            self.kill_at = rng.uniform(0.2, 0.5) * budget
        self.steps_at_kill: int | None = None
        self.checker = model.ViewChecker()
        self.stream: model.Stream | None = None
        self.seen_ids: dict[int, set[int | None]] = {}
        self.snapshot: dict[str, model.Bag] | None = None
        """Accumulated state at `as_of`, once the stream closed it."""
        self.peek: dict[str, model.Bag] | None = None
        self.resume: Resume | None = None
        self.newest: int | None = None
        self.clean = False

    def on_step(self, step: model.Step) -> None:
        assert self.stream is not None
        self.checker.check(self.stream.state, step)
        if self.snapshot is None and step.time == self.stream.as_of:
            self.snapshot = {t: Counter(b) for t, b in self.stream.state.items() if b}

    def feed(self, rows: list[tuple[Any, ...]]) -> None:
        assert self.stream is not None
        for r in rows:
            ts = int(r[0])
            if r[1]:
                for step in self.stream.advance(ts):
                    self.on_step(step)
                continue
            tag_name = str(r[3])
            row = _row(r[4:9])
            diff = int(r[2])
            self.stream.update(ts, tag_name, row, diff)
            if diff > 0:
                op = model.written_ids(tag_name, row)
                if op is not None:
                    self.seen_ids.setdefault(op, set()).add(
                        ts if ts > self.stream.as_of else None
                    )

    def run(self) -> None:
        with _connect(self.host, META_CLUSTER) as meta:
            cid = _cluster_id(meta)
            if cid is None:
                log("ivm_c does not exist")
                return
            sinces, uppers = _frontiers(meta)
            if (
                not sinces
                or any(s is None for s in sinces)
                or any(u is None for u in uppers)
            ):
                log("frontiers unavailable")
                return
            since = max(s for s in sinces if s is not None)
            newest = min(u for u in uppers if u is not None) - 1
            self.newest = newest
            if self.gen is not None:
                with self.db:
                    self.db.execute(
                        "INSERT INTO generations_seen VALUES (?, ?)", (self.gen, newest)
                    )
            resume = _load_resume(self.db)
            if resume is not None and (
                resume.t < since or resume.t > newest or rng.random() >= RESUME_P
            ):
                resume = None
            as_of = resume.t if resume is not None else pick_as_of(sinces, uppers)
            if as_of is None:
                log("no readable timestamp")
                return
            self.resume = resume
            sig_start = _replica_signature(self.env, meta, cid)

        self.stream = model.Stream(as_of)
        start = time.monotonic()
        with _connect(
            self.host, CLUSTER, options={"idle_in_transaction_session_timeout": "0"}
        ) as conn:
            conn.execute("SET transaction_isolation = 'serializable'")
            self.phase = "peek"
            try:
                self.peek = _peek(conn, as_of)
            except Exception as e:
                _note_read_error(e, "peek", as_of)
                if conn.closed:
                    return
            self.phase = "subscribe"
            with conn.transaction():
                conn.execute(
                    f"DECLARE c CURSOR FOR SUBSCRIBE ({model.subscribe_query()})"
                    f" WITH (PROGRESS) AS OF {as_of}".encode()
                )
                while time.monotonic() - start < self.budget:
                    rows = conn.execute(
                        f"FETCH ALL c WITH (timeout = '{FETCH_TIMEOUT}')"
                    ).fetchall()
                    self.feed(rows)
                    if (
                        self.kill_at is not None
                        and time.monotonic() - start >= self.kill_at
                    ):
                        self.kill_at = None
                        try:
                            if _kill_replica_pod(self.env, cid):
                                self.steps_at_kill = self.checker.steps
                        except Exception as e:
                            log(f"replica pod delete failed: {e}")
                conn.execute("CLOSE c")
        self.clean = True
        self.phase = "teardown"
        try:
            with _connect(self.host, META_CLUSTER) as meta:
                sig_end = _replica_signature(self.env, meta, cid)
        except (psycopg.Error, OSError) as e:
            log(f"reading the end signature failed: {e}")
            sig_end = None
        restarted = (
            sig_start is not None and sig_end is not None and sig_start != sig_end
        )
        sometimes(
            restarted and self.checker.steps > (self.steps_at_kill or 0),
            "ivm: a SUBSCRIBE window stayed open and kept comparing across an ivm_c replica restart",
            {
                "as_of": as_of,
                "steps": self.checker.steps,
                "steps_at_kill": self.steps_at_kill,
            },
        )

    def snapshot_checks(self) -> None:
        """Compare the state at `as_of` with the peek and the resumed state."""
        if self.snapshot is None or self.stream is None:
            return
        as_of, newest, peek, resume, gen = (
            self.stream.as_of,
            self.newest,
            self.peek,
            self.resume,
            self.gen,
        )
        if peek is not None and newest is not None:
            diff = model.state_diff(peek, self.snapshot)
            always(
                not diff,
                "ivm: SUBSCRIBE AS OF a timestamp starts from the snapshot a peek AS OF that timestamp returns",
                {"as_of": as_of, "newest": newest, "diff": diff},
            )
            sometimes(
                as_of < newest and sum(sum(b.values()) for b in peek.values()) > 0,
                "ivm: compared a SUBSCRIBE snapshot with a peek at a past timestamp",
                {"as_of": as_of, "newest": newest},
            )
        if resume is not None:
            diff = model.state_diff(resume.state, self.snapshot)
            always_or_unreachable(
                not diff,
                "ivm: a SUBSCRIBE resumed AS OF a timestamp an earlier SUBSCRIBE closed reproduces the state it closed there",
                {"t": resume.t, "saved_gen": resume.gen, "gen": gen, "diff": diff},
            )
            sometimes(
                resume.gen is not None and gen is not None and resume.gen < gen,
                "ivm: a resumed SUBSCRIBE was compared with state closed under an earlier environmentd generation",
                {"t": resume.t, "saved_gen": resume.gen, "gen": gen},
            )


def _note_read_error(e: BaseException, what: str, as_of: int | None) -> None:
    if is_since_race(e):
        log(f"{what}: since race at {as_of}")
        return
    c = classify_read(e)
    if c.outcome is sql.Outcome.VIOLATION:
        unreachable(
            "ivm: SUBSCRIBE and AS OF peeks return only classified errors",
            {
                "what": what,
                "as_of": as_of,
                "sqlstate": c.sqlstate,
                "template": c.template,
            },
        )
    log(f"{what} failed: {c.outcome.value} {c.sqlstate} {c.template}")


def _report(w: Window, db: sqlite3.Connection) -> None:
    """Assertions over everything one window observed, connected to the end or not."""
    stream = w.stream
    checker = w.checker
    if stream is None or checker.steps == 0:
        return
    w.snapshot_checks()
    details = {
        "as_of": stream.as_of,
        "frontier": stream.frontier,
        "steps": checker.steps,
        "clean": w.clean,
    }
    always(
        not stream.late,
        "ivm: SUBSCRIBE never emits an update below its as_of or a closed progress timestamp",
        {**details, "late": stream.late[:10]},
    )
    always(
        not stream.regressions,
        "ivm: SUBSCRIBE progress timestamps never go backwards",
        {**details, "regressions": stream.regressions[:10]},
    )
    negative = {
        t: s.negative for t, s in checker.stats.items() if s.negative is not None
    }
    always(
        not negative,
        "ivm: consolidated SUBSCRIBE state never has a negative multiplicity",
        {**details, "negative": negative},
    )
    for view in model.VIEWS:
        tags = [model.tag(view.kind, v) for v in model.VARIANTS]
        stats = {t: checker.stats[t] for t in tags}
        if not any(s.compared for s in stats.values()):
            continue
        mismatches = {t: s.mismatch for t, s in stats.items() if s.mismatch is not None}
        always(
            not mismatches,
            KIND_EQUAL[view.kind],
            {
                **details,
                "kind": view.kind,
                "mismatches": mismatches,
                "first_mismatch_time": checker.first_mismatch_time,
                "replica_kill_step": w.steps_at_kill,
            },
        )
        sometimes(
            any(s.nonempty for s in stats.values()),
            KIND_NONEMPTY[view.kind],
            {"kind": view.kind, "compared": {t: s.compared for t, s in stats.items()}},
        )
    temporal = [checker.stats[model.tag("temporal", v)] for v in model.VARIANTS]
    sometimes(
        any(s.time_driven_changes for s in temporal),
        "ivm: the temporal view changed at a timestamp with no base table update",
        details,
    )
    outer = [
        checker.stats[model.tag(k, v)]
        for k in ("left_join", "full_join")
        for v in model.VARIANTS
    ]
    sometimes(
        any(s.null_extended for s in outer),
        "ivm: an outer join view held a NULL-extended row at a compared timestamp",
        details,
    )
    gen = w.gen
    if gen is not None:
        row = db.execute(
            "SELECT max(ts) FROM generations_seen WHERE gen < ?", (gen,)
        ).fetchone()
        earlier = None if row is None or row[0] is None else int(row[0])
        sometimes(
            earlier is not None and earlier >= stream.as_of,
            "ivm: a SUBSCRIBE window replayed history from before an environmentd generation change",
            {**details, "gen": gen, "earlier_gen_served_through": earlier},
        )
    _check_writes(w, db, details)


def _check_writes(w: Window, db: sqlite3.Connection, details: dict[str, Any]) -> None:
    ids = sorted(w.seen_ids)
    ledger: dict[int, model.WriteOp] = {}
    for chunk in range(0, len(ids), 500):
        part = ids[chunk : chunk + 500]
        marks = ", ".join("?" * len(part))
        for r in db.execute(
            "SELECT op_id, outcome, invoke_rt, complete_rt, commit_ts FROM writes"
            f" WHERE op_id IN ({marks})",
            part,
        ):
            ledger[int(r[0])] = model.WriteOp(
                int(r[0]), str(r[1]), float(r[2]), r[3], r[4]
            )
    violations = model.write_id_violations(w.seen_ids, ledger)
    always(
        not violations["rejected"] and not violations["unknown"],
        "ivm: every base row carries the id of a write that was attempted and not definitely rejected",
        {**details, **violations},
    )
    always(
        not violations["commit_ts_conflicts"],
        "ivm: every SUBSCRIBE sees a write's rows inserted at one commit timestamp",
        {**details, "conflicts": violations["commit_ts_conflicts"]},
    )
    with db:
        for op_id, times in w.seen_ids.items():
            exact = sorted(t for t in times if t is not None)
            if exact and op_id in ledger:
                db.execute(
                    "UPDATE writes SET commit_ts = ? WHERE op_id = ? AND commit_ts IS NULL",
                    (exact[0], op_id),
                )
    ops = [
        model.WriteOp(int(r[0]), str(r[1]), float(r[2]), r[3], int(r[4]))
        for r in db.execute(
            "SELECT op_id, outcome, invoke_rt, complete_rt, commit_ts FROM writes"
            " WHERE commit_ts IS NOT NULL"
        )
    ]
    order = model.realtime_order_violations(ops)
    always(
        not order,
        "ivm: a write acknowledged before another began never commits at a later timestamp",
        {"violations": order, "writes_with_commit_ts": len(ops)},
    )


def _maybe_save_resume(w: Window, db: sqlite3.Connection) -> None:
    """Store the state at the window's last closed timestamp for a later window
    to resume from, unless the window already found a violation there."""
    s = w.stream
    if s is None or s.frontier is None or s.frontier - 1 < s.as_of:
        return
    stats = w.checker.stats.values()
    if (
        s.late
        or s.regressions
        or w.checker.first_mismatch_time is not None
        or any(st.negative is not None for st in stats)
    ):
        return
    _save_resume(db, s.frontier - 1, w.gen, dict(s.state))


def check_main() -> int:
    history.start_watchdog(CHECK_BUDGET_S + WATCHDOG_GRACE_S)
    env = Environment()
    try:
        host = env.sql_host()
    except Exception as e:
        log(f"no environmentd host: {e}")
        return 0
    db = open_state()
    if not ensure_objects(host, db):
        return 0
    kill_p = timeline_choice(db, "kill_p", KILL_P_MENU)
    end = time.monotonic() + CHECK_BUDGET_S
    while end - time.monotonic() > MIN_WINDOW_S:
        budget = min(rng.choice(WINDOW_MENU_S), end - time.monotonic())
        w = Window(env, host, db, budget, kill_p, _active_generation(env))
        try:
            w.run()
        except (psycopg.Error, OSError) as e:
            _note_read_error(e, w.phase, None if w.stream is None else w.stream.as_of)
        finally:
            _report(w, db)
            _maybe_save_resume(w, db)
        if not w.clean:
            time.sleep(1.0)
    return 0

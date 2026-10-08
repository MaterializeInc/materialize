# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Statements racing `ALTER TABLE ... ADD COLUMN`, checked against an insert ledger.

Property: `schema-evolution-preserves-table-contents`.

`ADD COLUMN` gives the table a new `GlobalId` (a relation version) on the same
persist shard and evolves the shard's schema. Objects planned afterwards
resolve to the new version, existing dependents keep the version they were
planned against, and every version reads the same shard. Each replica learns
the evolved schema through persist PubSub or a state fetch, so a dataflow
created on a replica right after the `ALTER` can open the shard before its
cached state has the new schema.

`driver_main` (`parallel_driver_schema_evolution`) runs races on the tables
of schema `schema_evo` (row model in `schema_model`). One race:

1. opens sessions pinned to drawn replicas of `schema_evo_c` and
   `antithesis_shared`, and starts a writer that keeps inserting with the
   column set read before the `ALTER`, by name or positionally;
2. runs `ADD COLUMN`, and fires the followers either together with it or
   after a drawn delay (zero included): `CREATE INDEX` on the table, MVs and
   indexed views over the latest version or over an older version named with
   `[<id> AS <name> VERSION <v>]`, replica-pinned peeks (plain and through a
   dataflow), a `SUBSCRIBE` snapshot, and INSERTs naming the new column;
3. sometimes drops a dependent and recreates its definition under a new name.

Before a race the oldest dependents are dropped down to `MAX_DEPENDENTS`, and
after its check a table with too many columns or inserts is dropped and
replaced.

Every read of a table is checked against the ledger (`schema_model.check_table`).
After its races, and in `check_main` (`anytime_schema_evolution_check`), a
read transaction on `schema_evo_c` holds a timestamp T, and the table and every
live dependent are read `AS OF T`: the table through persist and through each
replica of every cluster that indexes it, MVs through persist, indexed views on
each replica of their index's cluster. Each dependent must equal the projection
of the table read, and must expose the columns of the version it was defined on.

Ledger (`schema_evo` state database): `inserts` holds one row per INSERT with
its keys, the columns it named, and its outcome, recorded before the statement
is sent; `dependents` holds each dependent's definition, recorded before its
CREATE; `alter_inflight` marks `ALTER` statements in progress for the pod
restart driver's `schema_evo_alter` trigger.

NOTE: `INSERT INTO [<id> AS <name> VERSION <v>]` with `v` below the latest
version is never issued. `plan_insert_query` zips the pinned version's desc
with the latest version's column defaults (`zip_eq`), which by reading the
code panics on the length mismatch. Not confirmed by a run; issuing it on
every timeline would turn one environmentd panic into the run's only finding.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    always_or_unreachable,
    sometimes,
    unreachable,
)

from materialize.antithesis import schema_model, sql, state
from materialize.antithesis.drivers import configure, history
from materialize.antithesis.drivers.session_mix import classify_read, quote_literal
from materialize.antithesis.drivers.sources_common import (
    bag_diff,
    is_since_race,
    timeline_choice,
)
from materialize.antithesis.environment import Environment
from materialize.antithesis.rng import rng
from materialize.antithesis.schema_model import KEY, Insert

STATE_DB = "schema_evo"
SCHEMA = "schema_evo"
CLUSTER = "schema_evo_c"
CLUSTER_SIZE = "antithesis-1"
SHARED = configure.SHARED_CLUSTER
RF_MENU = (1, 1, 2)

# Object and data caps keep every dataflow tiny on one simulated core.
MAX_TABLES = 2
MAX_ADDED_COLUMNS = 6
MAX_LEDGER_INSERTS = 120
MAX_DEPENDENTS = 4
ROWS_PER_INSERT = (1, 1, 2, 3)

# Calibration: first guesses for one simulated core.
DRIVER_BUDGET_S = 150.0
CHECK_BUDGET_S = 120.0
CONNECT_DEADLINE_S = 30.0
STATEMENT_TIMEOUT_MS = 30_000
SUBSCRIBE_DEADLINE_S = 20.0
FETCH_TIMEOUT = "1s"
JOIN_GRACE_S = 10.0
LEDGER_RETENTION_S = 2 * (CHECK_BUDGET_S + history.WATCHDOG_GRACE_S)
RACES_MENU = (1, 2, 3)
FOLLOWERS_MENU = (1, 3, 6)
# Seconds between ADD COLUMN returning and the followers starting. None starts
# them together with the ALTER, so they plan against either version.
DELAY_MENU_S: tuple[float | None, ...] = (None, None, 0.0, 0.0, 0.005, 0.02, 0.1, 1.0)
OLD_WRITER_OPS_MENU = (0, 2, 6)
OLD_WRITER_GAP_S = (0.0, 0.0, 0.01, 0.05)
CHURN_P = 0.3
FRESH_S = 0.1
"""A follower that starts this soon after the ALTER returned is 'immediate'."""
RECENT_S = 1.0
ALTER_TRIGGER_WINDOW_S = 2.0
"""The pod restart trigger stays armed this long after an ALTER returns."""

FOLLOWERS = (
    "index_table",
    "mv",
    "mv_old_version",
    "view_index",
    "peek",
    "peek_dataflow",
    "subscribe",
    "insert_new",
    "insert_old",
)
DATAFLOW_FOLLOWERS = {
    "index_table",
    "mv",
    "mv_old_version",
    "view_index",
    "peek",
    "peek_dataflow",
    "subscribe",
}
NEEDS_NEW_COLUMN = {"insert_new"}

DDL_FAMILY = "dependent DDL"
WRITE_FAMILY = "INSERT"
READ_FAMILY = "read"


def log(message: str) -> None:
    print(f"schema-evo[{os.getpid()}]: {message}", flush=True)


def open_state() -> sqlite3.Connection:
    db = state.open_db(STATE_DB)
    with db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS ids (name TEXT PRIMARY KEY, value INTEGER)"
        )
        db.execute(
            "CREATE TABLE IF NOT EXISTS inserts ("
            " op_id INTEGER PRIMARY KEY AUTOINCREMENT, tbl TEXT NOT NULL,"
            " keys TEXT NOT NULL, written TEXT NOT NULL, outcome TEXT NOT NULL,"
            " invoke_rt REAL NOT NULL, complete_rt REAL)"
        )
        db.execute("CREATE INDEX IF NOT EXISTS inserts_tbl ON inserts (tbl)")
        db.execute(
            "CREATE TABLE IF NOT EXISTS dependents ("
            " name TEXT PRIMARY KEY, tbl TEXT NOT NULL, kind TEXT NOT NULL,"
            " cluster TEXT NOT NULL, cols TEXT, not_null TEXT, version INTEGER,"
            " created_rt REAL NOT NULL)"
        )
        db.execute(
            "CREATE TABLE IF NOT EXISTS observed_shapes"
            " (name TEXT PRIMARY KEY, cols TEXT NOT NULL)"
        )
        db.execute(
            "CREATE TABLE IF NOT EXISTS dropped (tbl TEXT PRIMARY KEY, at REAL NOT NULL)"
        )
        db.execute(
            "CREATE TABLE IF NOT EXISTS alter_inflight ("
            " token INTEGER PRIMARY KEY AUTOINCREMENT, pid INTEGER NOT NULL,"
            " tbl TEXT NOT NULL, started_rt REAL NOT NULL, done_rt REAL)"
        )
    return db


def next_id(db: sqlite3.Connection, name: str) -> int:
    with db:
        db.execute(
            "INSERT INTO ids VALUES (?, 0) ON CONFLICT(name) DO NOTHING", (name,)
        )
        db.execute("UPDATE ids SET value = value + 1 WHERE name = ?", (name,))
        return int(
            db.execute("SELECT value FROM ids WHERE name = ?", (name,)).fetchone()[0]
        )


ALTER_TRIGGER_QUERY = (
    "SELECT pid, done_rt FROM alter_inflight"
    f" WHERE done_rt IS NULL OR done_rt > ? - {ALTER_TRIGGER_WINDOW_S}"
)
"""Rows for `alter_recent`: ALTERs in flight or returned within the window.
Takes the current `time.monotonic()` as its parameter."""


def alter_recent(conn: sqlite3.Connection, pid_alive: Callable[[int], bool]) -> bool:
    """Whether an `ADD COLUMN` is in flight in a live process or just returned."""
    rows = conn.execute(ALTER_TRIGGER_QUERY, (time.monotonic(),)).fetchall()
    return any(done is not None or pid_alive(int(pid)) for pid, done in rows)


def _report(family: str, c: sql.Classified, details: dict[str, Any]) -> None:
    details = {**details, "sqlstate": c.sqlstate, "template": c.template}
    if family == "ADD COLUMN":
        unreachable(
            "schema evolution: ADD COLUMN returns only classified errors", details
        )
    elif family == DDL_FAMILY:
        unreachable(
            "schema evolution: CREATE and DROP of dependents around ADD COLUMN return only classified errors",
            details,
        )
    elif family == WRITE_FAMILY:
        unreachable(
            "schema evolution: INSERTs around ADD COLUMN return only classified errors",
            details,
        )
    else:
        unreachable(
            "schema evolution: reads around ADD COLUMN return only classified errors",
            details,
        )


def connect(
    host: str, cluster: str = SHARED, replica: str | None = None
) -> psycopg.Connection:
    conn = sql.connect_with_retry(
        host, CONNECT_DEADLINE_S, statement_timeout_ms=STATEMENT_TIMEOUT_MS
    )
    # Ledger bounds assume every statement is linearized after every
    # previously acknowledged write.
    conn.execute("SET transaction_isolation = 'strict serializable'")
    conn.execute(f"SET cluster = {quote_literal(cluster)}".encode())
    if replica is not None:
        conn.execute(f"SET cluster_replica = {quote_literal(replica)}".encode())
    return conn


def close(conn: psycopg.Connection | None) -> None:
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass


@dataclass(frozen=True)
class Table:
    name: str
    id: str
    create_sql: str

    @property
    def qualified(self) -> str:
        return f"{SCHEMA}.{self.name}"

    def columns(self) -> list[str]:
        return schema_model.version_columns(
            self.create_sql, schema_model.latest_version(self.create_sql)
        )

    def version_ref(self, version: int) -> str:
        return (
            f'[{self.id} AS "materialize"."{SCHEMA}"."{self.name}" VERSION {version}]'
        )


def live_tables(conn: psycopg.Connection) -> dict[str, Table]:
    rows = conn.execute(
        "SELECT t.name, t.id, t.create_sql FROM mz_catalog.mz_tables t"
        " JOIN mz_catalog.mz_schemas s ON t.schema_id = s.id"
        " JOIN mz_catalog.mz_databases d ON s.database_id = d.id"
        " WHERE d.name = 'materialize' AND s.name = %s",
        (SCHEMA,),
    ).fetchall()
    return {str(n): Table(str(n), str(i), str(c)) for n, i, c in rows}


def live_names(conn: psycopg.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT o.name FROM mz_catalog.mz_objects o"
        " JOIN mz_catalog.mz_schemas s ON o.schema_id = s.id"
        " JOIN mz_catalog.mz_databases d ON s.database_id = d.id"
        " WHERE d.name = 'materialize' AND s.name = %s",
        (SCHEMA,),
    ).fetchall()
    return {str(r[0]) for r in rows}


def replicas(conn: psycopg.Connection) -> list[tuple[str, str]]:
    """`(cluster, replica)` for every replica of `schema_evo_c` and the shared cluster."""
    rows = conn.execute(
        "SELECT c.name, r.name FROM mz_catalog.mz_cluster_replicas r"
        " JOIN mz_catalog.mz_clusters c ON r.cluster_id = c.id"
        " WHERE c.name = ANY(%s::text[]) ORDER BY c.name, r.name",
        ([CLUSTER, SHARED],),
    ).fetchall()
    return [(str(c), str(r)) for c, r in rows]


def ensure_setup(host: str, db: sqlite3.Connection) -> bool:
    rf = timeline_choice(db, "replication_factor", RF_MENU)
    try:
        with sql.connection(host, statement_timeout_ms=60_000) as conn:
            if (
                conn.execute(
                    "SELECT 1 FROM mz_schemas s JOIN mz_databases d"
                    " ON s.database_id = d.id"
                    " WHERE d.name = 'materialize' AND s.name = %s",
                    (SCHEMA,),
                ).fetchone()
                is None
            ):
                conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}".encode())
            if (
                conn.execute(
                    "SELECT 1 FROM mz_clusters WHERE name = %s", (CLUSTER,)
                ).fetchone()
                is None
            ):
                try:
                    conn.execute(
                        f"CREATE CLUSTER {CLUSTER} (SIZE {quote_literal(CLUSTER_SIZE)},"
                        f" REPLICATION FACTOR {rf})".encode()
                    )
                except psycopg.Error as e:
                    if sql.classify(e).race is not sql.CatalogRace.EXISTS:
                        raise
            if not live_tables(conn):
                create_table(conn, db)
        return True
    except (psycopg.Error, OSError) as e:
        c = sql.classify(e)
        if c.outcome is sql.Outcome.VIOLATION:
            _report(DDL_FAMILY, c, {"step": "setup"})
        log(f"setup failed: {c.template}")
        return False


def create_table(conn: psycopg.Connection, db: sqlite3.Connection) -> None:
    name = f"t{next_id(db, 'table')}"
    run_ddl(conn, f"CREATE TABLE {SCHEMA}.{name} {schema_model.BASE_DDL}", DDL_FAMILY)


def load_inserts(db: sqlite3.Connection, tbl: str) -> list[Insert]:
    rows = db.execute(
        "SELECT keys, written, outcome, invoke_rt, complete_rt FROM inserts"
        " WHERE tbl = ?",
        (tbl,),
    ).fetchall()
    return [
        Insert(
            tuple(json.loads(keys)),
            tuple(json.loads(written)),
            outcome,
            float(invoke),
            None if complete is None else float(complete),
        )
        for keys, written, outcome, invoke, complete in rows
    ]


@dataclass
class InsertResult:
    outcome: str
    sqlstate: str | None
    invoke: float
    complete: float
    written: list[str]
    positional: bool


def do_insert(
    conn: psycopg.Connection,
    db: sqlite3.Connection,
    table: Table,
    shape: list[str],
    positional: bool,
    must_name: str | None = None,
) -> InsertResult:
    """Insert fresh keys with column set `shape`, recorded in the ledger first.

    Positional inserts supply a prefix of `shape` and leave the rest to
    defaults. Named inserts supply `KEY`, `must_name` if given, and a random
    subset of the other columns.
    """
    keys = [next_id(db, "key") for _ in range(rng.choice(ROWS_PER_INSERT))]
    if positional:
        written = shape[: rng.randint(1, len(shape))]
        target = ""
    else:
        written = [KEY] + [
            c for c in shape if c != KEY and (c == must_name or rng.random() < 0.6)
        ]
        target = f" ({', '.join(written)})"
    values = ", ".join(
        "(" + ", ".join(schema_model.sql_literal(k, c) for c in written) + ")"
        for k in keys
    )
    statement = f"INSERT INTO {table.qualified}{target} VALUES {values}"
    invoke = time.monotonic()
    with db:
        op_id = db.execute(
            "INSERT INTO inserts (tbl, keys, written, outcome, invoke_rt)"
            " VALUES (?, ?, ?, 'pending', ?)",
            (table.name, json.dumps(keys), json.dumps(written), invoke),
        ).lastrowid
    outcome, sqlstate = "ok", None
    try:
        conn.execute(statement.encode())
    except Exception as e:
        c = sql.classify(e)
        sqlstate = c.sqlstate
        if c.outcome is sql.Outcome.REJECTED:
            outcome = "rejected"
        else:
            if c.outcome is sql.Outcome.VIOLATION:
                _report(
                    WRITE_FAMILY,
                    c,
                    {"table": table.name, "written": written, "positional": positional},
                )
            outcome = "indeterminate"
    complete = time.monotonic()
    with db:
        db.execute(
            "UPDATE inserts SET outcome = ?, complete_rt = ? WHERE op_id = ?",
            (outcome, complete, op_id),
        )
    return InsertResult(outcome, sqlstate, invoke, complete, written, positional)


def check_read(
    path: str,
    table: Table,
    columns: list[str],
    rows: list[tuple],
    start: float,
    end: float,
    db: sqlite3.Connection,
    extra: dict[str, Any] | None = None,
) -> schema_model.TableVerdict:
    """Assert one full read of `table` against the ledger.

    Pass `start = -inf` for a read that is not linearized after every
    acknowledged write (a `SUBSCRIBE` snapshot), which drops the completeness
    half of the check.
    """
    v = schema_model.check_table(
        columns, rows, load_inserts(db, table.name), start, end
    )
    details = {
        "path": path,
        "table": table.name,
        "columns": columns,
        "rows": v.rows,
        **(extra or {}),
    }
    if start != float("-inf"):
        always(
            v.complete,
            "schema evolution: a read of an evolved table includes every row acknowledged before it began",
            {**details, "missing": v.missing},
        )
    always(
        v.sound,
        "schema evolution: a read of an evolved table holds each row at most once and only rows that were attempted",
        {**details, "unexpected": v.unexpected, "duplicated": v.duplicated},
    )
    always(
        v.values_ok,
        "schema evolution: every row of an evolved table holds its written values and NULL or the default elsewhere",
        {**details, "wrong": v.wrong, "unmodeled_columns": v.unmodeled_columns},
    )
    return v


def still_live(host: str, name: str) -> bool | None:
    try:
        with sql.connection(host) as conn:
            return name in live_names(conn)
    except (psycopg.Error, OSError):
        return None


def read_failed(
    host: str,
    path: str,
    table: Table,
    read: str,
    e: BaseException,
    extra: dict[str, Any],
) -> None:
    """Classify a failed read of object `read`, a table or one of its dependents."""
    c = classify_read(e)
    if c.outcome is sql.Outcome.VIOLATION:
        _report(READ_FAMILY, c, {"path": path, "table": table.name, **extra})
    elif c.sqlstate == "P0002" and read == table.name:
        # `CollectionUnreadable` is a designed outcome only when the table was
        # dropped while the read was sequenced.
        live = still_live(host, read)
        always_or_unreachable(
            live is not True,
            "schema evolution: a live evolved table is never reported unreadable at every timestamp",
            {"path": path, "table": table.name, "template": c.template, **extra},
        )
    log(f"{path} read of {read} failed ({c.outcome.value}): {c.template}")


def subscribe_snapshot(
    conn: psycopg.Connection, query: str
) -> tuple[list[str], list[tuple], list[tuple]] | None:
    """Columns, rows, and negative rows of a `SUBSCRIBE` snapshot: the updates
    at its `AS OF`, which the first progress message names, complete once any
    later timestamp arrives. None if that did not happen in time."""
    acc: Counter[tuple] = Counter()
    columns: list[str] = []
    as_of: int | None = None
    deadline = time.monotonic() + SUBSCRIBE_DEADLINE_S
    with conn.transaction():
        conn.execute(
            f"DECLARE c CURSOR FOR SUBSCRIBE ({query}) WITH (PROGRESS)".encode()
        )
        while time.monotonic() < deadline:
            cur = conn.execute(f"FETCH ALL c WITH (timeout = '{FETCH_TIMEOUT}')")
            if not columns and cur.description is not None:
                columns = [d.name for d in cur.description][3:]
            for r in cur.fetchall():
                ts, progressed = int(r[0]), bool(r[1])
                if as_of is None:
                    as_of = ts
                if ts > as_of:
                    conn.execute("CLOSE c")
                    negatives = [row for row, n in acc.items() if n < 0]
                    return columns, list((+acc).elements()), negatives
                if not progressed:
                    acc[schema_model.normalize(r[3:])] += int(r[2])
        conn.execute("CLOSE c")
    return None


@dataclass
class FollowerResult:
    kind: str
    cluster: str
    replica: str | None
    start: float
    outcome: str = "skipped"
    """`ok`, `rejected`, `indeterminate`, `violation`, or `skipped`."""
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class AlterState:
    done: threading.Event = field(default_factory=threading.Event)
    outcome: str = "pending"
    acked: float | None = None
    column: str = ""
    shape_after: list[str] = field(default_factory=list)
    table_after: Table | None = None


class Race:
    """One ADD COLUMN with its old-shape writer and its followers."""

    def __init__(self, host: str, table: Table, targets: list[tuple[str, str]]):
        self.host = host
        self.table = table
        self.targets = targets
        self.shape = table.columns()
        self.latest = schema_model.latest_version(table.create_sql)
        self.alter = AlterState()
        self.results: list[FollowerResult] = []
        self.old_writes: list[InsertResult] = []
        self.lock = threading.Lock()

    def target(self) -> tuple[str, str | None]:
        if not self.targets or rng.random() < 0.1:
            return (rng.choice([CLUSTER, SHARED]), None)
        return rng.choice(self.targets)

    def run_alter(self, db: sqlite3.Connection) -> None:
        code = rng.choice(list(schema_model.COLUMN_TYPES))
        column = schema_model.added_column(next_id(db, "column"), code)
        self.alter.column = column
        if_not_exists = " IF NOT EXISTS" if rng.random() < 0.3 else ""
        statement = (
            f"ALTER TABLE {self.table.qualified} ADD COLUMN{if_not_exists}"
            f" {column} {schema_model.COLUMN_TYPES[code]}"
        )
        with db:
            token = db.execute(
                "INSERT INTO alter_inflight (pid, tbl, started_rt) VALUES (?, ?, ?)",
                (os.getpid(), self.table.name, time.monotonic()),
            ).lastrowid
        conn = None
        try:
            conn = connect(self.host)
            conn.execute(statement.encode())
            self.alter.outcome = "ok"
            self.alter.acked = time.monotonic()
            log(f"ok: {statement}")
            after = live_tables(conn).get(self.table.name)
            if after is not None:
                self.alter.table_after = after
                self.alter.shape_after = after.columns()
        except Exception as e:
            c = sql.classify(e)
            if c.outcome is sql.Outcome.VIOLATION:
                _report("ADD COLUMN", c, {"table": self.table.name, "column": column})
            self.alter.outcome = c.outcome.value
            log(f"{c.outcome.value} ({c.sqlstate}): {statement}: {c.template}")
        finally:
            close(conn)
            with db:
                db.execute(
                    "UPDATE alter_inflight SET done_rt = ? WHERE token = ?",
                    (time.monotonic(), token),
                )
                db.execute(
                    "DELETE FROM alter_inflight WHERE done_rt < ?",
                    (time.monotonic() - 10 * ALTER_TRIGGER_WINDOW_S,),
                )
            self.alter.done.set()

    def old_writer(self, ops: int) -> None:
        db = open_state()
        conn = None
        try:
            for _ in range(ops):
                if conn is None or conn.closed:
                    conn = connect(self.host)
                r = do_insert(conn, db, self.table, self.shape, rng.random() < 0.5)
                with self.lock:
                    self.old_writes.append(r)
                time.sleep(rng.choice(OLD_WRITER_GAP_S))
        except (psycopg.Error, OSError) as e:
            log(f"old-shape writer stopped: {e}")
        finally:
            close(conn)
            db.close()

    def follower(
        self, kind: str, conn: psycopg.Connection, res: FollowerResult
    ) -> None:
        db = open_state()
        try:
            if kind in NEEDS_NEW_COLUMN:
                self.alter.done.wait(STATEMENT_TIMEOUT_MS / 1000)
                if self.alter.outcome != "ok":
                    return
            res.start = time.monotonic()
            self.run_follower(kind, conn, db, res)
        finally:
            db.close()

    def run_follower(
        self,
        kind: str,
        conn: psycopg.Connection,
        db: sqlite3.Connection,
        res: FollowerResult,
    ) -> None:
        if kind == "insert_new":
            shape = self.alter.shape_after or self.shape + [self.alter.column]
            r = do_insert(conn, db, self.table, shape, False, self.alter.column)
            res.outcome = r.outcome
            res.details = {"written": r.written, "sqlstate": r.sqlstate}
            return
        if kind == "insert_old":
            r = do_insert(conn, db, self.table, self.shape, rng.random() < 0.5)
            res.outcome = r.outcome
            res.details = {"written": r.written, "positional": r.positional}
            with self.lock:
                self.old_writes.append(r)
            return
        if kind in ("peek", "peek_dataflow", "subscribe"):
            self.read(kind, conn, db, res)
            return
        self.create_dependent(kind, conn, db, res)

    def read(
        self,
        kind: str,
        conn: psycopg.Connection,
        db: sqlite3.Connection,
        res: FollowerResult,
    ) -> None:
        distinct = "DISTINCT " if kind == "peek_dataflow" else ""
        query = f"SELECT {distinct}* FROM {self.table.qualified}"
        path = f"{kind}:{res.cluster}/{res.replica}"
        try:
            start = time.monotonic()
            if kind == "subscribe":
                snap = subscribe_snapshot(conn, query)
                if snap is None:
                    res.outcome = "indeterminate"
                    return
                columns, rows, negatives = snap
                always(
                    not negatives,
                    "schema evolution: accumulated SUBSCRIBE state has no negative multiplicity",
                    {
                        "table": self.table.name,
                        "path": path,
                        "negatives": negatives[:10],
                    },
                )
                start = float("-inf")
            else:
                cur = conn.execute(query.encode())
                rows = cur.fetchall()
                columns = [d.name for d in cur.description or []]
            end = time.monotonic()
        except Exception as e:
            c = classify_read(e)
            res.outcome = c.outcome.value
            read_failed(
                self.host, path, self.table, self.table.name, e, {"follower": kind}
            )
            return
        v = check_read(path, self.table, columns, rows, start, end, db)
        res.outcome = "ok"
        res.details = {
            "rows": v.rows,
            "columns": columns,
            "matched": v.complete and v.sound and v.values_ok,
            "new_column_visible": self.alter.column in columns,
        }

    def create_dependent(
        self,
        kind: str,
        conn: psycopg.Connection,
        db: sqlite3.Connection,
        res: FollowerResult,
    ) -> None:
        n = next_id(db, "dependent")
        tbl = self.table
        cluster = res.cluster
        if kind == "index_table":
            name = f"ix{n}_{tbl.name}"
            spec = Dependent(name, tbl.name, "table_index", cluster, None, None, None)
            statements = [
                f"CREATE INDEX {name} IN CLUSTER {cluster} ON {tbl.qualified} ({KEY})"
            ]
        else:
            # After the ALTER returned the latest version is known; while it is
            # in flight the follower plans against whichever version it sees.
            known = self.alter.shape_after if self.alter.done.is_set() else []
            old_version = kind == "mv_old_version" or (
                kind == "view_index" and rng.random() < 0.3
            )
            spec = dependent_spec(
                n, kind, tbl, cluster, known, self.latest if old_version else None
            )
            name = spec.name
            statements = spec.create_statements(tbl)
        record_dependent(db, spec)
        res.details = {"name": name, "version": spec.version, "cols": spec.cols}
        for i, statement in enumerate(statements):
            try:
                conn.execute(statement.encode())
                if i == 0:
                    res.outcome = "ok"
            except Exception as e:
                c = sql.classify(e)
                res.outcome = c.outcome.value
                if c.outcome is sql.Outcome.VIOLATION:
                    _report(DDL_FAMILY, c, {"sql": statement, "follower": kind})
                log(f"{c.outcome.value} ({c.sqlstate}): {statement}: {c.template}")
                return
        if spec.kind != "table_index" and spec.cols is None and spec.version is None:
            record_observed_shape(conn, db, name)

    def run(self, db: sqlite3.Connection) -> None:
        kinds = [rng.choice(FOLLOWERS) for _ in range(rng.choice(FOLLOWERS_MENU))]
        delay = rng.choice(DELAY_MENU_S)
        # Sessions are opened and targeted before the ALTER, so each follower's
        # first statement goes out as soon as it starts.
        prepared: list[tuple[str, psycopg.Connection, FollowerResult]] = []
        for kind in kinds:
            cluster, replica = self.target()
            try:
                conn = connect(self.host, cluster, replica)
            except (psycopg.Error, OSError) as e:
                log(f"follower session for {kind} failed: {e}")
                continue
            res = FollowerResult(kind, cluster, replica, 0.0)
            prepared.append((kind, conn, res))
            self.results.append(res)
        writer = threading.Thread(
            target=self.old_writer, args=(rng.choice(OLD_WRITER_OPS_MENU),), daemon=True
        )
        threads = [
            threading.Thread(target=self.follower, args=p, daemon=True)
            for p in prepared
        ]
        writer.start()
        if delay is None:
            for t in threads:
                t.start()
            self.run_alter(db)
        else:
            self.run_alter(db)
            time.sleep(delay)
            for t in threads:
                t.start()
        deadline = time.monotonic() + STATEMENT_TIMEOUT_MS / 1000 * 2 + JOIN_GRACE_S
        for t in [*threads, writer]:
            t.join(max(0.0, deadline - time.monotonic()))
        for _, conn, _ in prepared:
            close(conn)
        self.report(delay)

    def report(self, delay: float | None) -> None:
        acked = self.alter.acked
        details = {
            "table": self.table.name,
            "column": self.alter.column,
            "delay": delay,
        }
        fresh_dataflows: set[tuple[str, str | None]] = set()
        for r in self.results:
            if r.outcome != "ok" or r.start == 0.0:
                continue
            offset = None if acked is None else r.start - acked
            d = {
                **details,
                "kind": r.kind,
                "cluster": r.cluster,
                "replica": r.replica,
                "offset_s": offset,
                **r.details,
            }
            if r.kind in DATAFLOW_FOLLOWERS and offset is not None:
                sometimes(
                    0 <= offset <= FRESH_S,
                    "schema evolution: a dataflow over a table started within 100 ms after its ADD COLUMN returned",
                    d,
                )
                sometimes(
                    offset < 0,
                    "schema evolution: a dataflow over a table started while its ADD COLUMN was in flight",
                    d,
                )
                if offset <= FRESH_S:
                    fresh_dataflows.add((r.cluster, r.replica))
            if r.kind == "insert_new" and offset is not None:
                sometimes(
                    offset <= FRESH_S,
                    "schema evolution: an INSERT naming the new column was acknowledged within 100 ms of ADD COLUMN",
                    d,
                )
            if r.kind in ("peek", "peek_dataflow") and offset is not None:
                sometimes(
                    r.replica is not None
                    and offset <= RECENT_S
                    and bool(r.details.get("new_column_visible"))
                    and bool(r.details.get("matched")),
                    "schema evolution: a replica-pinned read of the new table version matched the ledger within 1 s of ADD COLUMN",
                    d,
                )
            if r.kind == "subscribe" and offset is not None:
                sometimes(
                    offset <= RECENT_S
                    and bool(r.details.get("new_column_visible"))
                    and bool(r.details.get("matched")),
                    "schema evolution: a SUBSCRIBE snapshot of the new table version matched the ledger within 1 s of ADD COLUMN",
                    d,
                )
        if acked is not None:
            sometimes(
                len(fresh_dataflows) >= 2,
                "schema evolution: dataflows on two different replicas or clusters started within 100 ms of one ADD COLUMN",
                {**details, "targets": sorted(map(str, fresh_dataflows))},
            )
            for w in self.old_writes:
                d = {**details, "written": w.written, "positional": w.positional}
                if w.invoke > acked:
                    sometimes(
                        w.outcome == "ok",
                        "schema evolution: an INSERT with the pre-ALTER column set was acknowledged after ADD COLUMN returned",
                        d,
                    )
                if w.invoke < acked:
                    sometimes(
                        w.sqlstate == "40001",
                        "schema evolution: an INSERT with the pre-ALTER column set was rejected as racing ADD COLUMN",
                        d,
                    )
        for w in self.old_writes:
            sometimes(
                w.outcome == "ok" and w.positional and len(w.written) < len(self.shape),
                "schema evolution: a positional INSERT with fewer values than the table has columns was acknowledged",
                {**details, "written": w.written},
            )


@dataclass(frozen=True)
class Dependent:
    name: str
    tbl: str
    kind: str
    """`table_index`, `mv`, or `view_index`."""
    cluster: str
    cols: list[str] | None
    """Projected columns, or None for `SELECT *` over the version planned."""
    not_null: str | None
    version: int | None
    """The table version named explicitly, or None for the latest at planning."""

    @property
    def index_name(self) -> str:
        return f"{self.name}_idx"

    def create_statements(self, table: Table) -> list[str]:
        if self.kind == "table_index":
            return [
                f"CREATE INDEX {self.name} IN CLUSTER {self.cluster}"
                f" ON {table.qualified} ({KEY})"
            ]
        source = (
            table.qualified if self.version is None else table.version_ref(self.version)
        )
        select = "*" if self.cols is None else ", ".join(self.cols)
        where = "" if self.not_null is None else f" WHERE {self.not_null} IS NOT NULL"
        body = f"SELECT {select} FROM {source}{where}"
        if self.kind == "mv":
            return [
                f"CREATE MATERIALIZED VIEW {SCHEMA}.{self.name}"
                f" IN CLUSTER {self.cluster} AS {body}"
            ]
        return [
            f"CREATE VIEW {SCHEMA}.{self.name} AS {body}",
            f"CREATE INDEX {self.index_name} IN CLUSTER {self.cluster}"
            f" ON {SCHEMA}.{self.name} ({KEY})",
        ]

    def drop_statement(self) -> str:
        if self.kind == "table_index":
            return f"DROP INDEX {SCHEMA}.{self.name}"
        if self.kind == "mv":
            return f"DROP MATERIALIZED VIEW {SCHEMA}.{self.name}"
        return f"DROP VIEW {SCHEMA}.{self.name} CASCADE"


def dependent_spec(
    n: int,
    kind: str,
    table: Table,
    cluster: str,
    known_shape: list[str],
    old_latest: int | None,
) -> Dependent:
    """A random MV or indexed view definition over `table`.

    `known_shape` is the latest version's columns if the caller knows them,
    else empty, in which case the definition selects `*`. With `old_latest`
    set, the definition names a version at or below it explicitly.
    """
    prefix = "mv" if kind in ("mv", "mv_old_version") else "vw"
    dkind = "mv" if prefix == "mv" else "view_index"
    name = f"{prefix}{n}_{table.name}"
    if old_latest is not None:
        version = rng.randint(0, old_latest)
        shape = schema_model.version_columns(table.create_sql, version)
    else:
        version = None
        shape = known_shape
    cols: list[str] | None = None
    if shape and rng.random() < 0.7:
        cols = [KEY] + [c for c in shape if c != KEY and rng.random() < 0.6]
    added = [c for c in (cols or shape) if c not in schema_model.BASE_COLUMNS]
    not_null = rng.choice(added) if added and rng.random() < 0.3 else None
    return Dependent(name, table.name, dkind, cluster, cols, not_null, version)


def record_dependent(db: sqlite3.Connection, d: Dependent) -> None:
    with db:
        db.execute(
            "INSERT OR REPLACE INTO dependents VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                d.name,
                d.tbl,
                d.kind,
                d.cluster,
                None if d.cols is None else json.dumps(d.cols),
                d.not_null,
                d.version,
                time.monotonic(),
            ),
        )


def record_observed_shape(
    conn: psycopg.Connection, db: sqlite3.Connection, name: str
) -> None:
    """Record the columns a `SELECT *` dependent got when it was planned.

    Planning pins the table version, so the shape must survive later ADD
    COLUMNs and restarts, which re-plan the dependent from its `create_sql`.
    """
    try:
        rows = conn.execute(
            "SELECT c.name FROM mz_catalog.mz_columns c"
            " JOIN mz_catalog.mz_objects o ON c.id = o.id"
            " JOIN mz_catalog.mz_schemas s ON o.schema_id = s.id"
            " WHERE s.name = %s AND o.name = %s ORDER BY c.position",
            (SCHEMA, name),
        ).fetchall()
    except (psycopg.Error, OSError) as e:
        log(f"reading the columns of {name} failed: {e}")
        return
    if rows:
        with db:
            db.execute(
                "INSERT OR REPLACE INTO observed_shapes VALUES (?, ?)",
                (name, json.dumps([str(r[0]) for r in rows])),
            )


def observed_shapes(db: sqlite3.Connection) -> dict[str, list[str]]:
    return {
        str(n): list(json.loads(c))
        for n, c in db.execute("SELECT name, cols FROM observed_shapes")
    }


def load_dependents(db: sqlite3.Connection, tbl: str) -> list[Dependent]:
    rows = db.execute(
        "SELECT name, tbl, kind, cluster, cols, not_null, version FROM dependents"
        " WHERE tbl = ? ORDER BY created_rt",
        (tbl,),
    ).fetchall()
    return [
        Dependent(n, t, k, c, None if cols is None else list(json.loads(cols)), nn, v)
        for n, t, k, c, cols, nn, v in rows
    ]


def run_ddl(conn: psycopg.Connection, statement: str, family: str) -> str:
    try:
        conn.execute(statement.encode())
        log(f"ok: {statement}")
        return "ok"
    except Exception as e:
        c = sql.classify(e)
        if c.outcome is sql.Outcome.VIOLATION:
            _report(family, c, {"sql": statement})
        log(f"{c.outcome.value} ({c.sqlstate}): {statement}: {c.template}")
        return c.outcome.value


def prune(host: str, db: sqlite3.Connection, table: Table) -> None:
    """Drop the oldest dependents of `table` until a race has room for more."""
    with sql.connection(host, statement_timeout_ms=STATEMENT_TIMEOUT_MS) as conn:
        names = live_names(conn)
        deps = [d for d in load_dependents(db, table.name) if d.name in names]
        for d in deps[: max(0, len(deps) - MAX_DEPENDENTS + 1)]:
            if run_ddl(conn, d.drop_statement(), DDL_FAMILY) == "ok":
                with db:
                    db.execute("DELETE FROM dependents WHERE name = ?", (d.name,))


def churn(host: str, db: sqlite3.Connection, table: Table) -> None:
    """Drop one live dependent of `table` and recreate its definition under a
    new name, or just drop one when the table is at its dependent cap."""
    with sql.connection(host, statement_timeout_ms=STATEMENT_TIMEOUT_MS) as conn:
        names = live_names(conn)
        deps = [d for d in load_dependents(db, table.name) if d.name in names]
        if not deps:
            return
        victim = rng.choice(deps)
        if run_ddl(conn, victim.drop_statement(), DDL_FAMILY) != "ok":
            return
        with db:
            db.execute("DELETE FROM dependents WHERE name = ?", (victim.name,))
        if len(deps) > MAX_DEPENDENTS:
            return
        prefix = victim.name.split("_", 1)[0].rstrip("0123456789")
        name = f"{prefix}{next_id(db, 'dependent')}_{table.name}"
        again = Dependent(
            name,
            table.name,
            victim.kind,
            victim.cluster,
            victim.cols,
            victim.not_null,
            victim.version,
        )
        record_dependent(db, again)
        outcomes = [
            run_ddl(conn, s, DDL_FAMILY) for s in again.create_statements(table)
        ]
        if outcomes[0] == "ok" and again.kind != "table_index":
            if again.cols is None and again.version is None:
                record_observed_shape(conn, db, name)
        sometimes(
            all(o == "ok" for o in outcomes)
            and schema_model.latest_version(table.create_sql) > 0,
            "schema evolution: dropped and recreated a dependent of an evolved table",
            {"dropped": victim.name, "created": name, "kind": victim.kind},
        )


def maybe_rotate(host: str, db: sqlite3.Connection, table: Table) -> None:
    """Drop `table` and start a new one once it has too many columns or inserts."""
    added = len(table.columns()) - len(schema_model.BASE_COLUMNS)
    inserts = db.execute(
        "SELECT count(*) FROM inserts WHERE tbl = ?", (table.name,)
    ).fetchone()[0]
    if added < MAX_ADDED_COLUMNS and inserts < MAX_LEDGER_INSERTS:
        return
    with sql.connection(host, statement_timeout_ms=60_000) as conn:
        if run_ddl(conn, f"DROP TABLE {table.qualified} CASCADE", DDL_FAMILY) != "ok":
            return
        now = time.monotonic()
        with db:
            db.execute("INSERT OR IGNORE INTO dropped VALUES (?, ?)", (table.name, now))
            # A check that read a table before its drop loads the ledger after
            # the read, so ledger rows outlive the drop by more than a check.
            stale = [
                r[0]
                for r in db.execute(
                    "SELECT tbl FROM dropped WHERE at < ?", (now - LEDGER_RETENTION_S,)
                )
            ]
            for tbl in stale:
                db.execute("DELETE FROM inserts WHERE tbl = ?", (tbl,))
                db.execute(
                    "DELETE FROM observed_shapes WHERE name IN"
                    " (SELECT name FROM dependents WHERE tbl = ?)",
                    (tbl,),
                )
                db.execute("DELETE FROM dependents WHERE tbl = ?", (tbl,))
                db.execute("DELETE FROM dropped WHERE tbl = ?", (tbl,))
        if len(live_tables(conn)) < MAX_TABLES:
            create_table(conn, db)


def read_as_of(
    host: str, query: str, t: int, cluster: str, replica: str | None
) -> tuple[list[str], list[tuple]]:
    with sql.connection(host, statement_timeout_ms=STATEMENT_TIMEOUT_MS) as conn:
        conn.execute(f"SET cluster = {quote_literal(cluster)}".encode())
        if replica is not None:
            conn.execute(f"SET cluster_replica = {quote_literal(replica)}".encode())
        cur = conn.execute(f"{query} AS OF {t}".encode())
        rows = cur.fetchall()
        return [d.name for d in cur.description or []], rows


def check_at_timestamp(host: str, db: sqlite3.Connection, table: Table) -> bool:
    """Compare the table and its live dependents at one held timestamp.

    Returns whether the table read succeeded, so the caller can retry a round
    lost to a since race.
    """
    with sql.connection(
        host,
        statement_timeout_ms=STATEMENT_TIMEOUT_MS,
        options={"idle_in_transaction_session_timeout": "0"},
    ) as hold:
        hold.execute(f"SET cluster = {quote_literal(CLUSTER)}".encode())
        hold.execute("SET transaction_isolation = 'strict serializable'")
        names = live_names(hold)
        reps = replicas(hold)
        current = live_tables(hold).get(table.name)
        if current is None:
            return True
        deps = [d for d in load_dependents(db, table.name) if d.name in names]
        with hold.transaction():
            start = time.monotonic()
            row = hold.execute(
                f"SELECT mz_now()::text FROM (SELECT count(*) FROM {current.qualified}) AS c".encode()
            ).fetchone()
            end = time.monotonic()
            assert row is not None
            t = int(row[0])
            return compare_at(host, db, current, deps, reps, t, start, end)


def guarded_read(
    host: str,
    table: Table,
    read: str,
    path: str,
    t: int,
    cluster: str,
    replica: str | None,
) -> tuple[list[str], list[tuple]] | None:
    """`SELECT * FROM <read> AS OF t`, or None if it failed with a classified error."""
    try:
        return read_as_of(host, f"SELECT * FROM {SCHEMA}.{read}", t, cluster, replica)
    except Exception as e:
        if is_since_race(e):
            log(f"{path}: since race at {t}")
            return None
        read_failed(host, path, table, read, e, {"t": t})
        return None


def compare_at(
    host: str,
    db: sqlite3.Connection,
    table: Table,
    deps: list[Dependent],
    reps: list[tuple[str, str]],
    t: int,
    start: float,
    end: float,
) -> bool:
    ref = guarded_read(host, table, table.name, "persist", t, SHARED, None)
    if ref is None:
        return False
    columns, rows = ref
    v = check_read("persist", table, columns, rows, start, end, db, {"t": t})
    sometimes(
        v.values_ok and v.with_added_column > 0 and v.rows > v.with_added_column,
        "schema evolution: the timestamp check saw rows written both with and without an added column",
        {"table": table.name, "rows": v.rows, "with_added": v.with_added_column},
    )

    for cluster in sorted({d.cluster for d in deps if d.kind == "table_index"}):
        for c, replica in reps:
            if c != cluster:
                continue
            path = f"table:{cluster}/{replica}"
            got = guarded_read(host, table, table.name, path, t, cluster, replica)
            if got is None:
                continue
            g_cols, g_rows = got
            check_read(path, table, g_cols, g_rows, start, end, db, {"t": t})
            # The two reads plan separately, so an ADD COLUMN between them
            # gives them different versions. Compare the shared columns.
            common = [c for c in columns if c in g_cols]
            agree(
                table,
                path,
                "table_index",
                None,
                t,
                common,
                schema_model.project(columns, rows, common),
                schema_model.project(g_cols, g_rows, common),
            )

    shapes = observed_shapes(db)
    for d in deps:
        if d.kind == "table_index":
            continue
        if d.kind == "mv":
            targets: list[tuple[str, str | None]] = [(SHARED, None)]
        else:
            targets = [(c, r) for c, r in reps if c == d.cluster] or [(d.cluster, None)]
        for cluster, replica in targets:
            path = f"{d.kind}:{d.name}@{cluster}/{replica}"
            got = guarded_read(host, table, d.name, path, t, cluster, replica)
            if got is None:
                continue
            d_cols, d_rows = got
            expected_cols = d.cols or shapes.get(d.name)
            if d.version is not None and d.cols is None:
                expected_cols = schema_model.version_columns(
                    table.create_sql, d.version
                )
            if expected_cols is not None:
                always_or_unreachable(
                    d_cols == expected_cols,
                    "schema evolution: a dependent exposes exactly the columns it was defined over",
                    {
                        "dependent": d.name,
                        "version": d.version,
                        "expected": expected_cols,
                        "actual": d_cols,
                    },
                )
            if not set(d_cols) <= set(columns) or (
                d.not_null is not None and d.not_null not in columns
            ):
                continue
            expected = schema_model.project(columns, rows, d_cols, d.not_null)
            observed = Counter(schema_model.normalize(r) for r in d_rows)
            agree(table, path, d.kind, d, t, d_cols, expected, observed)
    return True


def agree(
    table: Table,
    path: str,
    kind: str,
    d: Dependent | None,
    t: int,
    cols: list[str],
    expected: Counter[tuple],
    observed: Counter[tuple],
) -> None:
    details = {
        "table": table.name,
        "path": path,
        "kind": kind,
        "t": t,
        "columns": cols,
        "version": None if d is None else d.version,
        "not_null": None if d is None else d.not_null,
        "rows": sum(expected.values()),
        "diff": bag_diff(expected.elements(), observed.elements()),
    }
    always(
        expected == observed,
        "schema evolution: every dependent of an evolved table agrees with the table at one timestamp",
        details,
    )
    if d is not None and d.version is not None:
        sometimes(
            expected == observed
            and d.version < schema_model.latest_version(table.create_sql)
            and sum(expected.values()) > 0,
            "schema evolution: a dependent pinned to an older table version agreed with the table at one timestamp",
            details,
        )


def check_table(host: str, db: sqlite3.Connection, table: Table) -> None:
    for _ in range(2):
        try:
            if check_at_timestamp(host, db, table):
                return
        except (psycopg.Error, OSError) as e:
            c = sql.classify(e)
            if c.outcome is sql.Outcome.VIOLATION and not is_since_race(e):
                _report(READ_FAMILY, c, {"path": "hold", "table": table.name})
            log(f"timestamp check of {table.name} failed: {c.template}")
            return


def pick_table(host: str, db: sqlite3.Connection) -> Table | None:
    with sql.connection(host) as conn:
        tables = live_tables(conn)
        if len(tables) < MAX_TABLES and rng.random() < 0.2:
            create_table(conn, db)
            tables = live_tables(conn)
    if not tables:
        return None
    return tables[rng.choice(sorted(tables))]


def driver_main() -> int:
    history.start_watchdog(DRIVER_BUDGET_S + history.WATCHDOG_GRACE_S)
    try:
        host = Environment().sql_host()
    except Exception as e:
        log(f"no environmentd host: {e}")
        return 0
    db = open_state()
    if not ensure_setup(host, db):
        return 0
    end = time.monotonic() + DRIVER_BUDGET_S
    touched: dict[str, Table] = {}
    try:
        for _ in range(rng.choice(RACES_MENU)):
            if time.monotonic() >= end:
                break
            table = pick_table(host, db)
            if table is None:
                break
            prune(host, db, table)
            with sql.connection(host) as conn:
                targets = replicas(conn)
            race = Race(host, table, targets)
            race.run(db)
            after = race.alter.table_after or table
            touched[after.name] = after
            if rng.random() < CHURN_P:
                churn(host, db, after)
        for table in touched.values():
            if time.monotonic() >= end:
                break
            check_table(host, db, table)
            maybe_rotate(host, db, table)
    except (psycopg.Error, OSError) as e:
        log(f"giving up this invocation: {e}")
    return 0


def check_main() -> int:
    history.start_watchdog(CHECK_BUDGET_S + history.WATCHDOG_GRACE_S)
    try:
        host = Environment().sql_host()
    except Exception as e:
        log(f"no environmentd host: {e}")
        return 0
    db = open_state()
    if not ensure_setup(host, db):
        return 0
    end = time.monotonic() + CHECK_BUDGET_S
    try:
        with sql.connection(host) as conn:
            tables = live_tables(conn)
    except (psycopg.Error, OSError) as e:
        log(f"listing tables failed: {e}")
        return 0
    for name in sorted(tables):
        if time.monotonic() >= end:
            break
        check_table(host, db, tables[name])
    return 0

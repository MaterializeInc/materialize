# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Session-variable mix (generator G7) racing small DDL.

Properties: the DDL-race clause of `client-errors-match-fault-context` for
peeks, replica-targeted peeks, `SUBSCRIBE`, `COPY TO` and real-time recency
reads; `real-time-recency-reads-reflect-upstream`; and the sustained RTR load
`coordinator-canaries-complete-under-load` needs to be non-vacuous.

Each invocation opens several sessions with drawn `transaction_isolation`,
`real_time_recency` and `real_time_recency_timeout`, `statement_timeout`, and a
`cluster` / `cluster_replica` target picked from the replicas that exist in any
user cluster at the time. Sessions run a bounded mix of statement families
while one thread races DDL against `session_mix_c`, the cluster this driver
owns: replication factor and size changes (which drop and recreate replicas)
and a scratch index dropped and recreated.

Real-time recency oracle: a session at strict serializable with
`real_time_recency` on commits a marker row to the upstream Postgres table
`session_mix.rtr_marker`, then counts it in `session_mix.rtr`, a table fed
from that upstream through `session_mix.pg_src`. A successful count must be 1.

NOTE: `allow_real_time_recency` gates both RTR variables. The harness sets it
in every profile (`render.BASE_SYSTEM_PARAMETERS`); if it is ever off, setting
them fails with 55P02 and the session runs without RTR, which the "real-time
recency was enabled" anchor makes visible.
"""

from __future__ import annotations

import re
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import unquote, urlparse

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    sometimes,
    unreachable,
)

from materialize.antithesis import sql, state
from materialize.antithesis.drivers import configure, history
from materialize.antithesis.endpoints import Endpoints
from materialize.antithesis.environment import Environment
from materialize.antithesis.rng import rng

STATE_DB = "session_mix"
SCHEMA = "session_mix"
CLUSTER = "session_mix_c"
SIZES = ("antithesis-1", "antithesis-2")
REPLICATION_FACTORS = (1, 2)
TABLE = f"{SCHEMA}.t"
INDEX = "t_idx"
RTR_TABLE = f"{SCHEMA}.rtr"
SOURCE = f"{SCHEMA}.pg_src"
PG_CONNECTION = f"{SCHEMA}.pg"
PG_SECRET = f"{SCHEMA}.pg_password"
SOURCE_CLUSTER = configure.SHARED_CLUSTER
UPSTREAM_SCHEMA = "session_mix"
UPSTREAM_TABLE = f"{UPSTREAM_SCHEMA}.rtr_marker"
PUBLICATION = "session_mix_pub"
SETUP_LOCK_KEY = 727_275
UPSTREAM_OPTIONS = "-c statement_timeout=30000 -c lock_timeout=10000"

ISOLATIONS = history.ISOLATIONS
STRICT = history.STRICT

# Calibration: every bound below is a first guess for one simulated core and
# needs a fault-free baseline before it is trusted. The client deadline must
# exceed the largest drawn statement timeout and RTR timeout, so a server-side
# timeout is the usual answer and the client cancel only catches hangs.
SESSIONS_MENU = (2, 4, 6)
OPS_MENU = (5, 20, 60)
BUDGET_S = (20.0, 90.0)
RTR_TIMEOUT_MENU = ("100ms", "1s", "10s", "60s")
STATEMENT_TIMEOUT_MENU = ("0", "500ms", "5s", "30s")
CLIENT_DEADLINE_S = 75.0
CANCEL_TIMEOUT_S = 5.0
WATCHDOG_GRACE_S = 120.0
CONNECT_DEADLINE_S = 30.0
DDL_TIMEOUT_MS = 60_000
FETCHES_MENU = (1, 3, 8)
FETCH_TIMEOUT_MENU = ("0s", "500ms", "2s")
DDL_PAUSE_S = (0.0, 0.5, 3.0)
RETARGET_P = 0.2
"""Chance per operation that a session draws a new cluster and replica target."""
MAX_ROWS = 200
KEYSPACE = 64
MARKER_CLEANUP_P = 0.05
MARKER_RETENTION = "10 minutes"

FAMILIES = ("peek", "subscribe", "copy_to", "rtr_read")

# Read outcomes that a correct system returns when the cluster or replica a
# session targets changes underneath it. Matched against masked templates for
# any SQLSTATE. Keep this list short: each entry hides a class of errors from
# every read family in this module and in `read_paths`.
DESIGNED_READ_REJECTIONS = [
    re.compile(p)
    for p in (
        # `ERROR_TARGET_REPLICA_FAILED` in the compute controller: a peek or
        # subscribe pinned to a replica that crashed or was dropped. It reaches
        # the client as `AdapterError::Unstructured`, so XX000.
        r"^target replica failed or was dropped",
        # `NoClusterReplicasAvailable` (0A000): the target cluster has no
        # replicas, for example after a concurrent `REPLICATION FACTOR 0`.
        r"^CLUSTER .* has no replicas available to service request",
        r"the transaction's active cluster has been dropped",
    )
]


def log(message: str) -> None:
    print(f"session-mix: {message}", flush=True)


def classify_read(error: BaseException) -> sql.Classified:
    """`sql.classify`, with `DESIGNED_READ_REJECTIONS` downgraded to rejections."""
    c = sql.classify(error)
    if c.outcome is sql.Outcome.VIOLATION and c.sqlstate is not None:
        if any(p.search(c.template) for p in DESIGNED_READ_REJECTIONS):
            return sql.Classified(sql.Outcome.REJECTED, c.sqlstate, c.template)
    return c


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class ClientDeadline:
    """Cancel the statement running on `conn` if the block outlives `seconds`.

    `statement_timeout = 0` sessions rely on this to end. A cancelled statement
    fails with 57014, which `sql.classify` treats as indeterminate.
    """

    def __init__(self, conn: psycopg.Connection, seconds: float) -> None:
        self.conn = conn
        self.timer = threading.Timer(seconds, self._cancel)
        self.timer.daemon = True

    def _cancel(self) -> None:
        try:
            self.conn.cancel_safe(timeout=CANCEL_TIMEOUT_S)
        except Exception as e:
            log(f"client cancel failed: {e}")

    def __enter__(self) -> ClientDeadline:
        self.timer.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.timer.cancel()


def open_state() -> sqlite3.Connection:
    db = state.open_db(STATE_DB)
    with db:
        db.execute("CREATE TABLE IF NOT EXISTS ready (name TEXT PRIMARY KEY)")
    return db


def _is_ready(db: sqlite3.Connection, name: str) -> bool:
    return (
        db.execute("SELECT 1 FROM ready WHERE name = ?", (name,)).fetchone() is not None
    )


def _set_ready(db: sqlite3.Connection, name: str, ready: bool) -> None:
    with db:
        if ready:
            db.execute("INSERT OR IGNORE INTO ready VALUES (?)", (name,))
        else:
            db.execute("DELETE FROM ready WHERE name = ?", (name,))


def _ddl(conn: psycopg.Connection, statement: str) -> None:
    try:
        conn.execute(statement.encode())
    except psycopg.Error as e:
        if sql.classify(e).race is not sql.CatalogRace.EXISTS:
            raise


def _upstream(endpoints: Endpoints) -> psycopg.Connection:
    return psycopg.connect(
        endpoints.upstream_postgres_url,
        connect_timeout=10,
        autocommit=True,
        options=UPSTREAM_OPTIONS,
    )


def _mz(
    host: str, statement_timeout_ms: int | None = DDL_TIMEOUT_MS
) -> psycopg.Connection:
    return sql.connect_with_retry(
        host, CONNECT_DEADLINE_S, statement_timeout_ms=statement_timeout_ms
    )


def ensure_cluster_objects(host: str, db: sqlite3.Connection) -> bool:
    """Schema, `session_mix_c`, the scratch table, and its index. Idempotent."""
    if _is_ready(db, "cluster"):
        return True
    try:
        with _mz(host) as conn:
            _ddl(conn, f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
            exists = conn.execute(
                "SELECT 1 FROM mz_clusters WHERE name = %s", (CLUSTER,)
            ).fetchone()
            if exists is None:
                _ddl(
                    conn,
                    f"CREATE CLUSTER {CLUSTER} (SIZE {quote_literal(rng.choice(SIZES))},"
                    f" REPLICATION FACTOR {int(rng.choice(REPLICATION_FACTORS))})",
                )
            _ddl(
                conn,
                f"CREATE TABLE IF NOT EXISTS {TABLE} (k int NOT NULL, v int NOT NULL)",
            )
            _ddl(
                conn,
                f"CREATE INDEX IF NOT EXISTS {INDEX} IN CLUSTER {CLUSTER} ON {TABLE} (k)",
            )
    except (psycopg.Error, OSError) as e:
        c = sql.classify(e)
        if c.outcome is sql.Outcome.VIOLATION:
            unreachable(
                "session mix: setup DDL returns only classified errors",
                {"sqlstate": c.sqlstate, "template": c.template},
            )
        log(f"cluster setup failed: {c.template}")
        return False
    _set_ready(db, "cluster", True)
    return True


def ensure_rtr_objects(host: str, endpoints: Endpoints, db: sqlite3.Connection) -> bool:
    """Upstream marker table and publication, and the source feeding `session_mix.rtr`."""
    if _is_ready(db, "rtr"):
        return True
    try:
        with _upstream(endpoints) as up:
            with up.transaction():
                up.execute("SELECT pg_advisory_xact_lock(%s)", (SETUP_LOCK_KEY,))
                up.execute(f"CREATE SCHEMA IF NOT EXISTS {UPSTREAM_SCHEMA}".encode())
                up.execute(
                    f"CREATE TABLE IF NOT EXISTS {UPSTREAM_TABLE} (id text PRIMARY KEY,"
                    " written_at timestamptz NOT NULL DEFAULT now())".encode()
                )
                # Postgres sources reject tables without full replica identity.
                up.execute(
                    f"ALTER TABLE {UPSTREAM_TABLE} REPLICA IDENTITY FULL".encode()
                )
                if (
                    up.execute(
                        "SELECT 1 FROM pg_publication WHERE pubname = %s",
                        (PUBLICATION,),
                    ).fetchone()
                    is None
                ):
                    up.execute(
                        f"CREATE PUBLICATION {PUBLICATION} FOR TABLE {UPSTREAM_TABLE}".encode()
                    )
    except (psycopg.Error, OSError) as e:
        log(f"upstream setup failed: {e}")
        return False
    url = urlparse(endpoints.upstream_postgres_url)
    password = unquote(url.password or "")
    try:
        with _mz(host) as conn:
            _ddl(conn, f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
            _ddl(
                conn,
                f"CREATE SECRET IF NOT EXISTS {PG_SECRET} AS {quote_literal(password)}",
            )
            _ddl(
                conn,
                f"CREATE CONNECTION IF NOT EXISTS {PG_CONNECTION} TO POSTGRES"
                f" (HOST {quote_literal(url.hostname or '')}, PORT {url.port or 5432},"
                f" USER {quote_literal(unquote(url.username or 'postgres'))},"
                f" PASSWORD SECRET {PG_SECRET},"
                f" DATABASE {quote_literal(url.path.lstrip('/'))})"
                " WITH (VALIDATE = false)",
            )
            _ddl(
                conn,
                f"CREATE SOURCE IF NOT EXISTS {SOURCE} IN CLUSTER {SOURCE_CLUSTER}"
                f" FROM POSTGRES CONNECTION {PG_CONNECTION}"
                f" (PUBLICATION {quote_literal(PUBLICATION)})",
            )
            _ddl(
                conn,
                f"CREATE TABLE IF NOT EXISTS {RTR_TABLE} FROM SOURCE {SOURCE}"
                f' (REFERENCE "{UPSTREAM_SCHEMA}"."rtr_marker")',
            )
    except (psycopg.Error, OSError) as e:
        log(f"RTR source setup failed: {sql.classify(e).template}")
        return False
    _set_ready(db, "rtr", True)
    return True


@dataclass(frozen=True)
class Target:
    cluster: str
    replica: str | None


@dataclass(frozen=True)
class DdlEvent:
    cluster: str
    start: float
    end: float
    kind: str


@dataclass
class RaceLog:
    """Replica-dropping DDL intervals on `time.monotonic()`, shared across threads."""

    lock: threading.Lock = field(default_factory=threading.Lock)
    events: list[DdlEvent] = field(default_factory=list)

    def add(self, event: DdlEvent) -> None:
        with self.lock:
            self.events.append(event)

    def overlaps(self, cluster: str, start: float, end: float) -> bool:
        with self.lock:
            return any(
                e.cluster == cluster and e.start < end and e.end > start
                for e in self.events
            )


def replica_targets(conn: psycopg.Connection) -> list[tuple[str, str | None]]:
    """`(cluster, replica)` for every user cluster; `replica` None for a cluster with none."""
    rows = conn.execute(
        "SELECT c.name, r.name FROM mz_catalog.mz_clusters c"
        " LEFT JOIN mz_catalog.mz_cluster_replicas r ON r.cluster_id = c.id"
        " WHERE c.id LIKE 'u%'"
    ).fetchall()
    return [(str(c), None if r is None else str(r)) for c, r in rows]


def draw_target(targets: list[tuple[str, str | None]]) -> Target:
    clusters = sorted({c for c, _ in targets})
    if not clusters:
        return Target(CLUSTER, None)
    cluster = (
        CLUSTER if CLUSTER in clusters and rng.random() < 0.5 else rng.choice(clusters)
    )
    replicas = sorted(r for c, r in targets if c == cluster and r is not None)
    replica = rng.choice(replicas) if replicas and rng.random() < 0.5 else None
    return Target(cluster, replica)


@dataclass
class SessionConfig:
    isolation: str
    rtr: bool
    rtr_timeout: str
    statement_timeout: str


def draw_config(force_rtr: bool) -> SessionConfig:
    rtr = force_rtr or rng.random() < 0.3
    return SessionConfig(
        isolation=STRICT if force_rtr else rng.choice(ISOLATIONS),
        rtr=rtr,
        rtr_timeout=rng.choice(RTR_TIMEOUT_MENU),
        statement_timeout=rng.choice(STATEMENT_TIMEOUT_MENU),
    )


class Session:
    def __init__(
        self,
        host: str,
        endpoints: Endpoints,
        index: int,
        config: SessionConfig,
        targets: list[tuple[str, str | None]],
        races: RaceLog,
        rtr_ready: bool,
    ) -> None:
        self.host = host
        self.endpoints = endpoints
        self.index = index
        self.config = config
        self.targets = targets
        self.races = races
        self.rtr_ready = rtr_ready
        self.rtr_on = False
        self.target = draw_target(targets)
        self.conn: psycopg.Connection | None = None
        self.upstream: psycopg.Connection | None = None

    def details(self, **extra: Any) -> dict[str, Any]:
        return {
            "session": self.index,
            "isolation": self.config.isolation,
            "rtr": self.rtr_on,
            "rtr_timeout": self.config.rtr_timeout,
            "statement_timeout": self.config.statement_timeout,
            "cluster": self.target.cluster,
            "cluster_replica": self.target.replica,
            **extra,
        }

    def connect(self) -> psycopg.Connection:
        conn = sql.connect(self.host, statement_timeout_ms=None)
        conn.execute(
            f"SET transaction_isolation = {quote_literal(self.config.isolation)}".encode()
        )
        conn.execute(
            f"SET statement_timeout = {quote_literal(self.config.statement_timeout)}".encode()
        )
        self.rtr_on = False
        if self.config.rtr:
            try:
                conn.execute("SET real_time_recency = true")
                conn.execute(
                    f"SET real_time_recency_timeout = {quote_literal(self.config.rtr_timeout)}".encode()
                )
                self.rtr_on = True
            except psycopg.Error as e:
                # 55P02: `allow_real_time_recency` is off.
                if e.sqlstate != "55P02":
                    raise
        sometimes(
            self.rtr_on,
            "session mix: real-time recency was enabled for a session",
            {},
        )
        self.apply_target(conn)
        self.conn = conn
        return conn

    def apply_target(self, conn: psycopg.Connection) -> None:
        conn.execute(f"SET cluster = {quote_literal(self.target.cluster)}".encode())
        if self.target.replica is None:
            conn.execute("RESET cluster_replica")
        else:
            conn.execute(
                f"SET cluster_replica = {quote_literal(self.target.replica)}".encode()
            )

    def close(self) -> None:
        for c in (self.conn, self.upstream):
            if c is not None:
                try:
                    c.close()
                except Exception:
                    pass
        self.conn = None
        self.upstream = None

    def run(self, ops: int, end: float) -> None:
        families = [f for f in FAMILIES if f != "rtr_read" or self.rtr_ready]
        try:
            for _ in range(ops):
                if time.monotonic() >= end:
                    return
                if self.conn is None or self.conn.closed:
                    try:
                        self.connect()
                    except Exception as e:
                        c = classify_read(e)
                        if c.outcome is sql.Outcome.VIOLATION:
                            unreachable(
                                "session mix: session setup returns only classified errors",
                                self.details(sqlstate=c.sqlstate, template=c.template),
                            )
                        self.close()
                        time.sleep(1.0)
                        continue
                assert self.conn is not None
                if rng.random() < RETARGET_P:
                    self.target = draw_target(self.targets)
                    try:
                        self.apply_target(self.conn)
                    except Exception:
                        self.close()
                        continue
                family = rng.choice(families)
                if family == "rtr_read" and not (
                    self.rtr_on and self.config.isolation == STRICT
                ):
                    family = "peek"
                self.run_op(family)
        finally:
            self.close()

    def run_op(self, family: str) -> None:
        conn = self.conn
        assert conn is not None
        fn: Callable[[psycopg.Connection], dict[str, Any]] = {
            "peek": self.peek,
            "subscribe": self.subscribe,
            "copy_to": self.copy_to,
            "rtr_read": self.rtr_read,
        }[family]
        targeted = self.target.replica is not None
        start = time.monotonic()
        error: sql.Classified | None = None
        info: dict[str, Any] = {}
        try:
            with ClientDeadline(conn, CLIENT_DEADLINE_S):
                info = fn(conn)
        except Exception as e:
            error = classify_read(e)
            if error.outcome is sql.Outcome.INDETERMINATE and conn.closed:
                self.close()
        end = time.monotonic()
        ok = error is None
        details = self.details(family=family, elapsed_s=end - start, **info)
        if error is not None:
            details.update(sqlstate=error.sqlstate, template=error.template)
            if error.outcome is sql.Outcome.VIOLATION:
                self.report_violation(family, targeted, details)
        self.report_reached(family, targeted, ok, details)
        if targeted and family == "peek":
            raced = self.races.overlaps(self.target.cluster, start, end) or (
                error is not None
                and error.outcome is sql.Outcome.REJECTED
                and (
                    "target replica failed" in error.template
                    or "cluster replica" in error.template
                )
            )
            sometimes(
                raced,
                "session mix: replica-targeted peek raced a replica drop",
                details,
            )
        if error is not None:
            sometimes(
                targeted and "target replica failed" in error.template,
                "session mix: a replica-targeted read ended with the target-replica-failed error",
                details,
            )

    @staticmethod
    def report_violation(family: str, targeted: bool, details: dict[str, Any]) -> None:
        if family == "peek" and targeted:
            unreachable(
                "session mix: replica-targeted peek returns only classified errors",
                details,
            )
        elif family == "peek":
            unreachable("session mix: peek returns only classified errors", details)
        elif family == "subscribe":
            unreachable(
                "session mix: SUBSCRIBE returns only classified errors", details
            )
        elif family == "copy_to":
            unreachable(
                "session mix: COPY TO STDOUT returns only classified errors", details
            )
        else:
            unreachable(
                "session mix: real-time recency read returns only classified errors",
                details,
            )

    @staticmethod
    def report_reached(
        family: str, targeted: bool, ok: bool, details: dict[str, Any]
    ) -> None:
        if family == "peek" and targeted:
            sometimes(ok, "session mix: replica-targeted peek succeeded", details)
        elif family == "peek":
            sometimes(ok, "session mix: peek succeeded", details)
        elif family == "subscribe":
            sometimes(ok, "session mix: SUBSCRIBE fetched and closed", details)
        elif family == "copy_to":
            sometimes(ok, "session mix: COPY TO STDOUT completed", details)
        else:
            sometimes(
                ok, "session mix: real-time recency read observed its marker", details
            )

    def peek(self, conn: psycopg.Connection) -> dict[str, Any]:
        if rng.random() < 0.5:
            rows = conn.execute(f"SELECT k, v FROM {TABLE}".encode()).fetchall()
        else:
            rows = conn.execute(
                f"SELECT k, v FROM {TABLE} WHERE k = %s".encode(),
                (rng.randrange(KEYSPACE),),
            ).fetchall()
        return {"rows": len(rows)}

    def subscribe(self, conn: psycopg.Connection) -> dict[str, Any]:
        snapshot = "true" if rng.random() < 0.7 else "false"
        fetched = 0
        progressed = False
        with conn.transaction():
            conn.execute(
                f"DECLARE c CURSOR FOR SUBSCRIBE {TABLE}"
                f" WITH (PROGRESS, SNAPSHOT = {snapshot})".encode()
            )
            for _ in range(rng.choice(FETCHES_MENU)):
                rows = conn.execute(
                    f"FETCH ALL c WITH (timeout = {quote_literal(rng.choice(FETCH_TIMEOUT_MENU))})".encode()
                ).fetchall()
                fetched += len(rows)
                progressed = progressed or any(bool(r[1]) for r in rows)
            conn.execute("CLOSE c")
        return {"fetched": fetched, "progressed": progressed}

    def copy_to(self, conn: psycopg.Connection) -> dict[str, Any]:
        size = 0
        with conn.cursor() as cur:
            with cur.copy(f"COPY (SELECT k, v FROM {TABLE}) TO STDOUT".encode()) as cp:
                for data in cp:
                    size += len(data)
        return {"bytes": size}

    def rtr_read(self, conn: psycopg.Connection) -> dict[str, Any]:
        marker = str(uuid.UUID(int=rng.getrandbits(128)))
        try:
            if self.upstream is None or self.upstream.closed:
                self.upstream = _upstream(self.endpoints)
            self.upstream.execute(
                f"INSERT INTO {UPSTREAM_TABLE} (id) VALUES (%s)".encode(), (marker,)
            )
            if rng.random() < MARKER_CLEANUP_P:
                self.upstream.execute(
                    f"DELETE FROM {UPSTREAM_TABLE}"
                    f" WHERE written_at < now() - interval '{MARKER_RETENTION}'".encode()
                )
        except (psycopg.Error, OSError) as e:
            # The marker's commit is unknown, so the read proves nothing.
            log(f"upstream marker write failed: {e}")
            if self.upstream is not None:
                try:
                    self.upstream.close()
                except Exception:
                    pass
            self.upstream = None
            return {"marker": None}
        try:
            row = conn.execute(
                f"SELECT count(*) FROM {RTR_TABLE} WHERE id = %s".encode(), (marker,)
            ).fetchone()
        except psycopg.Error as e:
            c = sql.classify(e)
            if c.race is sql.CatalogRace.MISSING:
                # sqlite connections are per thread.
                db = open_state()
                try:
                    _set_ready(db, "rtr", False)
                finally:
                    db.close()
            sometimes(
                c.sqlstate == "57014" and c.template.startswith("timed out before"),
                "session mix: real-time recency read hit its RTR timeout",
                {"rtr_timeout": self.config.rtr_timeout},
            )
            raise
        seen = int(row[0]) if row else 0
        always(
            seen == 1,
            "session mix: real-time recency read includes an upstream marker committed before it started",
            self.details(marker=marker, seen=seen),
        )
        return {"marker": marker, "seen": seen}


def ddl_racer(host: str, races: RaceLog, end: float, stop: threading.Event) -> None:
    """Race replica drops and index churn on `session_mix_c` until `end` or `stop`."""
    conn: psycopg.Connection | None = None
    while not stop.is_set() and time.monotonic() < end:
        action = rng.choice(("rf", "size", "index", "insert"))
        start = time.monotonic()
        drops = False
        try:
            if conn is None or conn.closed:
                conn = sql.connect(host, statement_timeout_ms=DDL_TIMEOUT_MS)
            with ClientDeadline(conn, CLIENT_DEADLINE_S):
                if action == "rf":
                    row = conn.execute(
                        "SELECT replication_factor FROM mz_clusters WHERE name = %s",
                        (CLUSTER,),
                    ).fetchone()
                    current = int(row[0]) if row and row[0] is not None else 0
                    new = int(rng.choice(REPLICATION_FACTORS))
                    drops = new < current
                    conn.execute(
                        f"ALTER CLUSTER {CLUSTER} SET (REPLICATION FACTOR {new})".encode()
                    )
                elif action == "size":
                    drops = True
                    conn.execute(
                        f"ALTER CLUSTER {CLUSTER}"
                        f" SET (SIZE {quote_literal(rng.choice(SIZES))})".encode()
                    )
                elif action == "index":
                    conn.execute(f"DROP INDEX IF EXISTS {SCHEMA}.{INDEX}".encode())
                    conn.execute(
                        f"CREATE INDEX IF NOT EXISTS {INDEX} IN CLUSTER {CLUSTER}"
                        f" ON {TABLE} (k)".encode()
                    )
                else:
                    row = conn.execute(
                        f"SELECT count(*) FROM {TABLE}".encode()
                    ).fetchone()
                    if row is not None and int(row[0]) >= MAX_ROWS:
                        conn.execute(
                            f"DELETE FROM {TABLE} WHERE k = %s".encode(),
                            (rng.randrange(KEYSPACE),),
                        )
                    else:
                        conn.execute(
                            f"INSERT INTO {TABLE} VALUES (%s, %s)".encode(),
                            (rng.randrange(KEYSPACE), rng.randrange(-1000, 1000)),
                        )
        except Exception as e:
            c = classify_read(e)
            if c.outcome is sql.Outcome.VIOLATION:
                unreachable(
                    "session mix: racing DDL on session_mix_c returns only classified errors",
                    {"action": action, "sqlstate": c.sqlstate, "template": c.template},
                )
            if conn is not None and conn.closed:
                conn = None
        finally:
            if drops:
                races.add(DdlEvent(CLUSTER, start, time.monotonic(), action))
        time.sleep(rng.choice(DDL_PAUSE_S))
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass


def main() -> int:
    budget = rng.uniform(*BUDGET_S)
    history.start_watchdog(budget + CLIENT_DEADLINE_S + WATCHDOG_GRACE_S)
    env = Environment()
    try:
        host = env.sql_host()
    except Exception as e:
        log(f"no environmentd host: {e}")
        return 0
    db = open_state()
    cluster_ready = ensure_cluster_objects(host, db)
    rtr_ready = ensure_rtr_objects(host, env.endpoints, db)
    try:
        with _mz(host) as conn:
            targets = replica_targets(conn)
    except (psycopg.Error, OSError) as e:
        log(f"no replica targets: {e}")
        return 0

    end = time.monotonic() + budget
    races = RaceLog()
    stop = threading.Event()
    threads = []
    if cluster_ready:
        racer = threading.Thread(
            target=ddl_racer, args=(host, races, end, stop), daemon=True
        )
        racer.start()
        threads.append(racer)
    n = rng.choice(SESSIONS_MENU)
    for i in range(n):
        # The first session carries the sustained strict serializable RTR load.
        config = draw_config(force_rtr=rtr_ready and i == 0)
        session = Session(host, env.endpoints, i, config, targets, races, rtr_ready)
        t = threading.Thread(
            target=session.run, args=(rng.choice(OPS_MENU), end), daemon=True
        )
        t.start()
        threads.append(t)
    for t in threads:
        t.join(max(0.0, end - time.monotonic()) + CLIENT_DEADLINE_S + CANCEL_TIMEOUT_S)
    stop.set()
    log(f"done: {n} sessions, cluster ready {cluster_ready}, rtr ready {rtr_ready}")
    return 0

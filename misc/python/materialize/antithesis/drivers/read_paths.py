# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Every read path returns the same rows at one timestamp.

Property: `read-paths-agree-at-timestamp`.

Objects, all in schema `read_paths` and maintained on `read_paths_c`
(replication factor 2): tables `t (k, g, v)` and `dim (g, label)`; MVs `agg`
(count, sum, min, max per group), `joined` (`t` join `dim`), and `topk` (top 2
`v` per group through `LATERAL ... LIMIT`); and an index on `t` and on each MV.
`load_main` inserts, updates and deletes rows of both tables.

`check_main` runs rounds. Each round opens a read transaction on `read_paths_c`
whose first query takes `mz_now()`. A read transaction holds back compaction of
every collection in its time domain until it ends, so the other sessions can
read `AS OF` that time. The round then compares, per relation:

* `index`: a fast-path peek of the relation's index;
* `replica:<name>`: the same peek with `cluster_replica` pinned to each replica;
* `persist`: the relation read on `antithesis_shared`, where it has no index;
* `recompute`: the MV's defining query over the base tables, a new dataflow;
* `subscribe`: `SUBSCRIBE ... AS OF T WITH (PROGRESS)` accumulated until a
  progress message passes T.

A read that fails with a classified error is skipped. A since race (the drawn
margin put T below a since that the transaction does not hold) retries the
round at the transaction's own timestamp.
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
    reachable,
    sometimes,
    unreachable,
)
from kubernetes import client  # type: ignore

from materialize.antithesis import sql, state
from materialize.antithesis.drivers import configure, history
from materialize.antithesis.drivers.recovery import observe_pods
from materialize.antithesis.drivers.session_mix import classify_read, quote_literal
from materialize.antithesis.drivers.sources_common import (
    bag_diff,
    is_since_race,
    timeline_choice,
)
from materialize.antithesis.environment import Environment
from materialize.antithesis.rng import rng

STATE_DB = "read_paths"
SCHEMA = "read_paths"
CLUSTER = "read_paths_c"
CLUSTER_SIZE = "antithesis-1"
REPLICATION_FACTOR = 2
OTHER_CLUSTER = configure.SHARED_CLUSTER
GROUPS = 8
KEYSPACE = 128
MAX_ROWS = 300
VALUES = (-(2**31) + 1, -1, 0, 1, 2**31 - 1)

AGG_SQL = (
    f"SELECT g, count(*) AS n, sum(v) AS s, min(v) AS lo, max(v) AS hi"
    f" FROM {SCHEMA}.t GROUP BY g"
)
JOIN_SQL = (
    f"SELECT t.k, t.v, d.label FROM {SCHEMA}.t AS t"
    f" JOIN {SCHEMA}.dim AS d ON t.g = d.g"
)
TOPK_SQL = (
    f"SELECT grp.g, lat.k, lat.v FROM (SELECT DISTINCT g FROM {SCHEMA}.t) AS grp,"
    f" LATERAL (SELECT i.k, i.v FROM {SCHEMA}.t AS i WHERE i.g = grp.g"
    f" ORDER BY i.v DESC, i.k LIMIT 2) AS lat"
)


@dataclass(frozen=True)
class Relation:
    name: str
    columns: str
    definition: str | None
    """Defining query of an MV, recomputed as the `recompute` path."""

    @property
    def read_sql(self) -> str:
        return f"SELECT {self.columns} FROM {SCHEMA}.{self.name}"


RELATIONS = (
    Relation("t", "k, g, v", None),
    Relation("agg", "g, n, s, lo, hi", AGG_SQL),
    Relation("joined", "k, v, label", JOIN_SQL),
    Relation("topk", "g, k, v", TOPK_SQL),
)
OBJECTS = (
    "t",
    "dim",
    "agg",
    "joined",
    "topk",
    "t_idx",
    "agg_idx",
    "joined_idx",
    "topk_idx",
)

# Calibration: first guesses for one simulated core, to be replaced by a
# fault-free baseline. The statement timeout bounds each AS OF read, which
# waits for the replica to pass T; a restarted replica must rehydrate first.
CHECK_BUDGET_S = 150.0
ROUND_ATTEMPTS = 3
ROUNDS_MENU = (1, 2, 4)
STATEMENT_TIMEOUT_MS = 45_000
SUBSCRIBE_DEADLINE_S = 45.0
FETCH_TIMEOUT = "2s"
MARGIN_MENU_MS = (0, 0, 500, 2_000)
"""Distance below the hold transaction's timestamp; nonzero draws read near
or below the since and exercise the since-race path."""
LOAD_BUDGET_S = (10.0, 60.0)
LOAD_OPS_MENU = (10, 50, 200)
KILL_P_MENU = (0.0, 0.05, 0.2)
"""Per-invocation chance that the load driver deletes a `read_paths_c` pod."""
WATCHDOG_GRACE_S = 120.0
CONNECT_DEADLINE_S = 30.0


def log(message: str) -> None:
    print(f"read-paths: {message}", flush=True)


def open_state() -> sqlite3.Connection:
    db = state.open_db(STATE_DB)
    with db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS replica_sig"
            " (replica_id TEXT PRIMARY KEY, sig TEXT NOT NULL)"
        )
    return db


def _connect(host: str, **kwargs: Any) -> psycopg.Connection:
    kwargs.setdefault("statement_timeout_ms", STATEMENT_TIMEOUT_MS)
    return sql.connect_with_retry(host, CONNECT_DEADLINE_S, **kwargs)


def _ddl(conn: psycopg.Connection, statement: str) -> None:
    try:
        conn.execute(statement.encode())
    except psycopg.Error as e:
        if sql.classify(e).race is not sql.CatalogRace.EXISTS:
            raise


def _objects_present(conn: psycopg.Connection) -> bool:
    row = conn.execute(
        "SELECT"
        " (SELECT count(*) FROM mz_catalog.mz_clusters WHERE name = %s),"
        " (SELECT count(DISTINCT o.name) FROM mz_catalog.mz_objects o"
        "  JOIN mz_catalog.mz_schemas s ON o.schema_id = s.id"
        "  JOIN mz_catalog.mz_databases d ON s.database_id = d.id"
        "  WHERE d.name = 'materialize' AND s.name = %s AND o.name = ANY(%s::text[]))",
        (CLUSTER, SCHEMA, list(OBJECTS)),
    ).fetchone()
    return row is not None and int(row[0]) == 1 and int(row[1]) == len(OBJECTS)


def ensure_objects(host: str) -> bool:
    """Create the cluster, tables, MVs and indexes if any is missing. Idempotent."""
    try:
        with _connect(host, statement_timeout_ms=60_000) as conn:
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
                    f" REPLICATION FACTOR {REPLICATION_FACTOR})",
                )
            _ddl(
                conn,
                f"CREATE TABLE IF NOT EXISTS {SCHEMA}.t"
                " (k int NOT NULL, g int NOT NULL, v int NOT NULL)",
            )
            _ddl(
                conn,
                f"CREATE TABLE IF NOT EXISTS {SCHEMA}.dim"
                " (g int NOT NULL, label text NOT NULL)",
            )
            for rel in RELATIONS:
                if rel.definition is not None:
                    _ddl(
                        conn,
                        f"CREATE MATERIALIZED VIEW IF NOT EXISTS {SCHEMA}.{rel.name}"
                        f" IN CLUSTER {CLUSTER} AS {rel.definition}",
                    )
                key = rel.columns.split(",")[0].strip()
                _ddl(
                    conn,
                    f"CREATE INDEX IF NOT EXISTS {rel.name}_idx IN CLUSTER {CLUSTER}"
                    f" ON {SCHEMA}.{rel.name} ({key})",
                )
            row = conn.execute(f"SELECT count(*) FROM {SCHEMA}.dim".encode()).fetchone()
            if row is not None and int(row[0]) == 0:
                conn.execute(
                    f"INSERT INTO {SCHEMA}.dim"
                    f" SELECT g, 'label-' || g FROM generate_series(0, {GROUPS - 1}) AS g".encode()
                )
        return True
    except (psycopg.Error, OSError) as e:
        c = sql.classify(e)
        if c.outcome is sql.Outcome.VIOLATION:
            unreachable(
                "read paths: setup DDL returns only classified errors",
                {"sqlstate": c.sqlstate, "template": c.template},
            )
        log(f"setup failed: {c.template}")
        return False


def replicas(conn: psycopg.Connection) -> list[tuple[str, str, str]]:
    """`(cluster_id, replica_id, replica_name)` for each replica of `read_paths_c`."""
    rows = conn.execute(
        "SELECT c.id, r.id, r.name FROM mz_catalog.mz_cluster_replicas r"
        " JOIN mz_catalog.mz_clusters c ON r.cluster_id = c.id WHERE c.name = %s"
        " ORDER BY r.name",
        (CLUSTER,),
    ).fetchall()
    return [(str(c), str(i), str(n)) for c, i, n in rows]


def replica_signatures(
    env: Environment, conn: psycopg.Connection, reps: list[tuple[str, str, str]]
) -> dict[str, str]:
    """Per replica id, a value that changes when any of its processes restarts.

    Combines the latest `offline` event in the replica status history with the
    uid and restart count of every pod of the replica, when the Kubernetes API
    answers.
    """
    offline: dict[str, str | None] = {}
    rows = conn.execute(
        "SELECT r.id, max(h.occurred_at)::text FROM mz_catalog.mz_cluster_replicas r"
        " JOIN mz_catalog.mz_clusters c ON r.cluster_id = c.id"
        " LEFT JOIN mz_internal.mz_cluster_replica_status_history h"
        "  ON h.replica_id = r.id AND h.status = 'offline'"
        " WHERE c.name = %s GROUP BY r.id",
        (CLUSTER,),
    ).fetchall()
    for rid, at in rows:
        offline[str(rid)] = None if at is None else str(at)
    try:
        pods = observe_pods(env)
    except Exception as e:
        log(f"pod observation failed: {e}")
        pods = []
    out = {}
    for _, rid, _ in reps:
        marker = f"-replica-{rid}-gen-"
        procs = sorted(
            (p.pod_uid, p.container, p.restart_count) for p in pods if marker in p.pod
        )
        out[rid] = json.dumps({"offline": offline.get(rid), "pods": procs})
    return out


def restarted_since_last(db: sqlite3.Connection, sigs: dict[str, str]) -> list[str]:
    """Replica ids whose signature changed since the stored one; stores `sigs`."""
    changed = []
    for rid, sig in sigs.items():
        row = db.execute(
            "SELECT sig FROM replica_sig WHERE replica_id = ?", (rid,)
        ).fetchone()
        if row is not None and row[0] != sig:
            changed.append(rid)
    with db:
        db.executemany(
            "INSERT OR REPLACE INTO replica_sig VALUES (?, ?)", list(sigs.items())
        )
    return changed


def _norm(row: tuple) -> tuple:
    return tuple(None if x is None else str(x) for x in row)


class SinceRace(Exception):
    pass


@dataclass
class Read:
    rows: Counter | None
    """None when the read failed with a classified error."""
    negatives: list[tuple] | None = None


class Round:
    def __init__(self, host: str, t: int, reps: list[tuple[str, str, str]]) -> None:
        self.host = host
        self.t = t
        self.reps = reps
        self.conn: psycopg.Connection | None = None
        self.since_race = False

    def reader(self) -> psycopg.Connection:
        if self.conn is None or self.conn.closed:
            self.conn = _connect(self.host)
            self.conn.execute("SET transaction_isolation = 'serializable'")
        return self.conn

    def close(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = None

    def target(self, cluster: str, replica: str | None) -> psycopg.Connection:
        conn = self.reader()
        conn.execute(f"SET cluster = {quote_literal(cluster)}".encode())
        if replica is None:
            conn.execute("RESET cluster_replica")
        else:
            conn.execute(f"SET cluster_replica = {quote_literal(replica)}".encode())
        return conn

    def guarded(self, path: str, rel: Relation, fn: Any) -> Read:
        try:
            return fn()
        except Exception as e:
            if is_since_race(e):
                self.since_race = True
                return Read(None)
            c = classify_read(e)
            if c.outcome is sql.Outcome.VIOLATION:
                unreachable(
                    "read paths: AS OF reads return only classified errors",
                    {
                        "relation": rel.name,
                        "path": path,
                        "t": self.t,
                        "sqlstate": c.sqlstate,
                        "template": c.template,
                    },
                )
            if self.conn is not None and self.conn.closed:
                self.close()
            return Read(None)

    def select(
        self, rel: Relation, path: str, cluster: str, replica: str | None
    ) -> Read:
        def run() -> Read:
            conn = self.target(cluster, replica)
            query = rel.read_sql
            if path == "recompute":
                assert rel.definition is not None
                query = f"SELECT * FROM ({rel.definition}) AS q"
            rows = conn.execute(f"{query} AS OF {self.t}".encode()).fetchall()
            return Read(Counter(_norm(r) for r in rows))

        return self.guarded(path, rel, run)

    def subscribe(self, rel: Relation, replica: str | None) -> Read:
        def run() -> Read:
            conn = self.target(CLUSTER, replica)
            acc: Counter = Counter()
            deadline = time.monotonic() + SUBSCRIBE_DEADLINE_S
            done = False
            with conn.transaction():
                conn.execute(
                    f"DECLARE c CURSOR FOR SUBSCRIBE ({rel.read_sql})"
                    f" WITH (PROGRESS) AS OF {self.t}".encode()
                )
                while not done and time.monotonic() < deadline:
                    rows = conn.execute(
                        f"FETCH ALL c WITH (timeout = '{FETCH_TIMEOUT}')"
                    ).fetchall()
                    for r in rows:
                        ts, progressed, diff = int(r[0]), bool(r[1]), r[2]
                        if ts > self.t:
                            done = True
                            break
                        if not progressed:
                            acc[_norm(tuple(r[3:]))] += int(diff)
                conn.execute("CLOSE c")
            if not done:
                return Read(None)
            negatives = [row for row, n in acc.items() if n < 0]
            return Read(+acc, negatives)

        return self.guarded("subscribe", rel, run)

    def compare(self, rel: Relation, restarted: list[str]) -> int:
        """Read `rel` through every path at T and assert agreement. Returns paths compared."""
        reads: dict[str, Read] = {
            "index": self.select(rel, "index", CLUSTER, None),
        }
        for _, _, name in self.reps:
            reads[f"replica:{name}"] = self.select(
                rel, f"replica:{name}", CLUSTER, name
            )
        reads["persist"] = self.select(rel, "persist", OTHER_CLUSTER, None)
        if rel.definition is not None:
            reads["recompute"] = self.select(rel, "recompute", CLUSTER, None)
        sub_replica = (
            rng.choice([None] + [n for _, _, n in self.reps]) if self.reps else None
        )
        reads["subscribe"] = self.subscribe(rel, sub_replica)

        sub = reads["subscribe"]
        if sub.negatives is not None:
            always(
                not sub.negatives,
                "read paths: accumulated SUBSCRIBE state has no negative multiplicity",
                {
                    "relation": rel.name,
                    "t": self.t,
                    "replica": sub_replica,
                    "negatives": [list(r) for r in sub.negatives[:10]],
                },
            )

        ok = {p: r.rows for p, r in reads.items() if r.rows is not None}
        if len(ok) < 2:
            return len(ok)
        ref_path = "index" if "index" in ok else next(iter(ok))
        ref = ok[ref_path]
        diffs = {
            p: bag_diff(ref.elements(), rows.elements())
            for p, rows in ok.items()
            if p != ref_path and rows != ref
        }
        details = {
            "t": self.t,
            "reference": ref_path,
            "paths": sorted(ok),
            "skipped": sorted(set(reads) - set(ok)),
            "rows": sum(ref.values()),
            "diffs": diffs,
            "restarted_replicas": restarted,
            "frontend_peek_sequencing": _flag("enable_frontend_peek_sequencing"),
            "frontend_subscribes": _flag("enable_frontend_subscribes"),
        }
        if rel.name == "t":
            always(
                not diffs,
                "read paths: every path returns the same rows of the base table at one timestamp",
                details,
            )
        elif rel.name == "agg":
            always(
                not diffs,
                "read paths: every path returns the same rows of the reducing MV at one timestamp",
                details,
            )
        elif rel.name == "joined":
            always(
                not diffs,
                "read paths: every path returns the same rows of the join MV at one timestamp",
                details,
            )
        else:
            always(
                not diffs,
                "read paths: every path returns the same rows of the TopK MV at one timestamp",
                details,
            )
        sometimes(
            sum(ref.values()) > 0,
            "read paths: a round compared a non-empty relation across paths",
            {"relation": rel.name, "paths": sorted(ok), "rows": sum(ref.values())},
        )
        replica_paths = [p for p in ok if p.startswith("replica:")]
        sometimes(
            len(replica_paths) >= 2,
            "read paths: both replicas of read_paths_c answered targeted peeks in one round",
            {"relation": rel.name},
        )
        return len(ok)


def _flag(name: str) -> str | None:
    try:
        db = configure.open_state()
        try:
            return configure.current_flag_value(db, name)
        finally:
            db.close()
    except Exception:
        return None


def _max_since(conn: psycopg.Connection) -> int | None:
    row = conn.execute(
        "SELECT max(f.read_frontier)::text FROM mz_internal.mz_frontiers f"
        " JOIN mz_catalog.mz_objects o ON o.id = f.object_id"
        " JOIN mz_catalog.mz_schemas s ON o.schema_id = s.id"
        " WHERE s.name = %s",
        (SCHEMA,),
    ).fetchone()
    return None if row is None or row[0] is None else int(row[0])


def run_round(
    env: Environment, host: str, db: sqlite3.Connection, margin_ms: int
) -> bool | None:
    """One comparison round. True if compared, False on a since race, None if skipped."""
    # The hold transaction idles while side sessions read, which on one
    # simulated core can outlast the default idle-in-transaction timeout.
    with _connect(host, options={"idle_in_transaction_session_timeout": "0"}) as hold:
        reps = replicas(hold)
        if not reps:
            log("read_paths_c has no replicas")
            return None
        restarted = restarted_since_last(db, replica_signatures(env, hold, reps))
        hold.execute(f"SET cluster = {quote_literal(CLUSTER)}".encode())
        hold.execute("SET transaction_isolation = 'strict serializable'")
        with hold.transaction():
            row = hold.execute(
                f"SELECT mz_now()::text FROM (SELECT count(*) FROM {SCHEMA}.t) AS c".encode()
            ).fetchone()
            assert row is not None
            held = int(row[0])
            t = held
            if margin_ms > 0:
                with _connect(host) as side:
                    since = _max_since(side)
                if since is not None and held - margin_ms >= since:
                    t = held - margin_ms
            r = Round(host, t, reps)
            try:
                compared = [r.compare(rel, restarted) for rel in RELATIONS]
            finally:
                r.close()
    sometimes(
        bool(restarted) and max(compared) >= 2,
        "read paths: compared reads after a read_paths_c replica restarted since the previous round",
        {"restarted": restarted, "t": t, "held": held},
    )
    sometimes(
        t < held and max(compared) >= 2,
        "read paths: compared reads at a timestamp below the hold transaction's",
        {"t": t, "held": held},
    )
    if r.since_race and max(compared) < 2:
        return False
    return max(compared) >= 2


def check_main() -> int:
    history.start_watchdog(CHECK_BUDGET_S + WATCHDOG_GRACE_S)
    env = Environment()
    try:
        host = env.sql_host()
    except Exception as e:
        log(f"no environmentd host: {e}")
        return 0
    if not ensure_objects(host):
        return 0
    db = open_state()
    end = time.monotonic() + CHECK_BUDGET_S
    for _ in range(rng.choice(ROUNDS_MENU)):
        margin = rng.choice(MARGIN_MENU_MS)
        for _ in range(ROUND_ATTEMPTS):
            if time.monotonic() >= end:
                return 0
            try:
                result = run_round(env, host, db, margin)
            except (psycopg.Error, OSError) as e:
                c = sql.classify(e)
                if c.outcome is sql.Outcome.VIOLATION and not is_since_race(e):
                    unreachable(
                        "read paths: the hold transaction returns only classified errors",
                        {"sqlstate": c.sqlstate, "template": c.template},
                    )
                log(f"round failed: {c.template}")
                result = None
            if result is not False:
                break
            margin = 0
    return 0


def _kill_replica_pod(env: Environment, conn: psycopg.Connection) -> None:
    reps = replicas(conn)
    if not reps:
        return
    cluster_id, replica_id, name = rng.choice(reps)
    marker = f"cluster-{cluster_id}-replica-{replica_id}-gen-"
    core = client.CoreV1Api()
    pods = [
        p.metadata.name
        for p in core.list_namespaced_pod(env.endpoints.namespace).items
        if p.metadata is not None and p.metadata.name and marker in p.metadata.name
    ]
    if not pods:
        return
    victim = rng.choice(pods)
    core.delete_namespaced_pod(victim, env.endpoints.namespace)
    reachable(
        "read paths load: deleted a read_paths_c replica pod",
        {"replica": name, "pod": victim},
    )


def _dml(conn: psycopg.Connection, rows: int) -> str:
    kind = rng.choice(("insert", "insert", "update", "delete", "delete_range", "dim"))
    if kind == "insert" or (kind != "dim" and rows == 0):
        conn.execute(
            f"INSERT INTO {SCHEMA}.t VALUES (%s, %s, %s)".encode(),
            (rng.randrange(KEYSPACE), rng.randrange(GROUPS), _value()),
        )
        return "insert"
    if kind == "update":
        conn.execute(
            f"UPDATE {SCHEMA}.t SET v = %s, g = %s WHERE k = %s".encode(),
            (_value(), rng.randrange(GROUPS), rng.randrange(KEYSPACE)),
        )
    elif kind == "delete":
        conn.execute(
            f"DELETE FROM {SCHEMA}.t WHERE k = %s".encode(), (rng.randrange(KEYSPACE),)
        )
    elif kind == "delete_range":
        lo = rng.randrange(KEYSPACE)
        conn.execute(
            f"DELETE FROM {SCHEMA}.t WHERE k BETWEEN %s AND %s".encode(),
            (lo, lo + rng.choice((1, 4, 16))),
        )
    else:
        conn.execute(
            f"UPDATE {SCHEMA}.dim SET label = %s WHERE g = %s".encode(),
            (f"label-{rng.getrandbits(16)}", rng.randrange(GROUPS)),
        )
    return kind


def _value() -> int:
    if rng.random() < 0.1:
        return rng.choice(VALUES)
    return rng.randrange(-1000, 1000)


def load_main() -> int:
    budget = rng.uniform(*LOAD_BUDGET_S)
    history.start_watchdog(budget + STATEMENT_TIMEOUT_MS / 1000 + WATCHDOG_GRACE_S)
    env = Environment()
    try:
        host = env.sql_host()
    except Exception as e:
        log(f"no environmentd host: {e}")
        return 0
    if not ensure_objects(host):
        return 0
    db = open_state()
    kill_p = timeline_choice(db, "kill_p", KILL_P_MENU)
    end = time.monotonic() + budget
    conn: psycopg.Connection | None = None
    rows = 0
    for i in range(rng.choice(LOAD_OPS_MENU)):
        if time.monotonic() >= end:
            break
        try:
            if conn is None or conn.closed:
                conn = _connect(host)
                conn.execute(f"SET cluster = {quote_literal(CLUSTER)}".encode())
            if i % 20 == 0:
                row = conn.execute(
                    f"SELECT count(*) FROM {SCHEMA}.t".encode()
                ).fetchone()
                rows = int(row[0]) if row else 0
                if i == 0 and rng.random() < kill_p:
                    try:
                        _kill_replica_pod(env, conn)
                    except Exception as e:
                        log(f"replica pod delete failed: {e}")
            if rows >= MAX_ROWS:
                lo = rng.randrange(KEYSPACE)
                conn.execute(
                    f"DELETE FROM {SCHEMA}.t WHERE k BETWEEN %s AND %s".encode(),
                    (lo, lo + 16),
                )
                rows = 0
            else:
                _dml(conn, rows)
        except Exception as e:
            c = classify_read(e)
            if c.outcome is sql.Outcome.VIOLATION:
                unreachable(
                    "read paths load: DML returns only classified errors",
                    {"sqlstate": c.sqlstate, "template": c.template},
                )
            if conn is not None and conn.closed:
                conn = None
    if conn is not None:
        conn.close()
    return 0

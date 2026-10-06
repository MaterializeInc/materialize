# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Object lifecycle DDL (generator G1) with interrupted multi-step operations.

Each invocation first resumes some pending multi-step operations left by
earlier invocations, then runs a few random DDL/DML steps, checking after each
step that every live collection in schema `lifecycle` is readable and holds a
row count within the insert ledger's bounds. Multi-step operations (pending
replacement MVs, `APPLY REPLACEMENT` waits, graceful `ALTER CLUSTER`,
`ALTER TABLE ADD COLUMN` chains, unmanaged replica swaps) are recorded in the
`pending` table when started, and an invocation may exit right after starting
one, so later invocations, restarts, and faults land between the steps.

Every object lives in `materialize.lifecycle` or in a cluster named
`lifecycle_c<n>` (managed) or `lifecycle_u<n>` (unmanaged). Names encode the
row-count lineage the readability oracle relies on: table `t<n>`, MV
`mv<k>_t<n>`, replacement `rp<j>_mv<k>_t<n>`. Every MV definition preserves the
row count of its base table, so one ledger bounds every read.

Restart snapshot (restart-preserves-derived-state). `projection` reads a
restart-invariant view of the lifecycle objects, keyed by id:

* catalog items (id, name, type, cluster), and their global ids,
* `mz_storage_shards` shard mapping for those global ids,
* `mz_history_retention_strategies`,
* `mz_replacements` (pending replacement to target),
* index placement (index, on, cluster),
* cluster shape (managed, size, replication factor) and replicas (name, size),
  omitted for clusters with an in-progress reconfiguration in either snapshot
  because the controller legitimately changes them without DDL,
* graceful reconfiguration records without `status`, which advances on its
  own,
* `dump`: in-memory storage controller state from environmentd's
  `/api/coordinator/dump` (internal HTTP), per lifecycle global id, parsed
  from the `Debug` form of `controller.storage_collections.collections`:
  `primary`, `ingestion_remap_collection_id`, `storage_dependencies`, and
  the `data_shard` and `txns_shard` of `collection_metadata`; plus
  `read_policy` with every frontier erased to its shape. A `NoPolicy`
  collection has not had its policy installed since the restart and is
  left out. The catalog families above are durable state, equal whenever
  the catalog is intact; this family is what a restart rebuilds. It is
  `None` when the dump is unreadable, and compared only when both
  snapshots have it.

Replica online/offline status, frontiers, read capabilities, and the
finalizable shard set are excluded: all legitimately change across a
restart. A snapshot is only compared to a later one when no
lifecycle DDL started in between (`ddl_seq` unchanged, no in-flight DDL), and
the pod restart signature changed.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import sys
import time
from typing import Any

import psycopg
import requests
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always_greater_than_or_equal_to,
    always_less_than_or_equal_to,
    always_or_unreachable,
    reachable,
    sometimes,
)
from kubernetes import client  # type: ignore
from kubernetes.client.rest import ApiException  # type: ignore

from materialize.antithesis import sql, state
from materialize.antithesis.drivers import configure
from materialize.antithesis.endpoints import INTERNAL_HTTP_PORT
from materialize.antithesis.environment import Environment
from materialize.antithesis.rng import rng

SCHEMA = "lifecycle"
QUALIFIED = f"materialize.{SCHEMA}"
CLUSTER_PREFIX = "lifecycle_"
STATE_DB = "lifecycle"
RECOVERY_DB = "recovery"

# One invocation is a few DDL steps; faults land mid-invocation often enough
# that a longer budget only adds abandoned work. Not yet calibrated.
RUN_BUDGET_S = 300
STEP_MENU = (1, 3, 8)
RESUME_MENU = (0, 1, 3)
CONNECT_DEADLINE_S = 60
DDL_TIMEOUT_MS = 60_000
READ_TIMEOUT_MS = 30_000
DUMP_TIMEOUT_S = 30

# Object caps keep the whole environment within one simulated core. Creates
# over a cap turn into the matching drop.
MAX_TABLES = 4
MAX_MVS = 6
MAX_INDEXES = 4
MAX_CLUSTERS = 2
MAX_REPLICAS = 2
INSERT_MENU = (0, 1, 2, 100)
ADD_COLUMN_CHAIN_MENU = (1, 2, 3)
SIZES = ("antithesis-1", "antithesis-2", "antithesis-4")
RF_MENU = (0, 1, 2)
# '0s' cuts over or rolls back at the first controller tick; '600s' outlives
# most faults; '5s' sits near one hydration on one core.
WAIT_TIMEOUT_MENU = ("0s", "1s", "5s", "60s", "600s")
# APPLY REPLACEMENT waits for the target frontier. A short statement timeout
# abandons the wait mid-flight.
APPLY_TIMEOUT_MENU_MS = (1, 500, 5_000, 60_000)
ABANDON_MENU = (0.02, 0.2, 0.6)
WEIGHT_MENU = (0.0, 1.0, 4.0, 16.0)

ACTIONS = (
    "create_table",
    "insert",
    "add_column",
    "create_mv",
    "create_replacement",
    "apply_replacement",
    "drop_replacement",
    "create_index",
    "drop_object",
    "create_cluster",
    "alter_cluster_graceful",
    "alter_cluster_rf",
    "replica_swap",
    "drop_cluster",
    "flag_roll",
)

# Rejections a correct system gives this workload's DDL under concurrent
# lifecycle invocations: a dependency changed by another invocation, a second
# replacement for one target, a resource limit. Taken from the DDL-complexity
# part of `errors_to_ignore` in `materialize.parallel_workload.action`, which
# cannot be imported here (it pulls in the mzcompose service stack), plus the
# replacement and cluster reconfiguration rejections this driver can provoke.
# Several are `PlanError`s, which Materialize reports as XX000, so these apply
# to every SQLSTATE. Unknown and duplicate names and drops of a depended-upon
# object are `sql.CATALOG_RACE_TEMPLATES`.
EXPECTED_REJECTIONS = [
    re.compile(p)
    for p in (
        r"was concurrently (dropped|modified)",
        r"' was dropped",
        r"was dropped while executing a statement",
        r"was removed",
        r"another session modified the catalog",
        r"object state changed while transaction was in progress",
        r"is not readable at any timestamp",
        r"query could not complete",
        r"the transaction's active cluster has been dropped",
        r"already has a replacement",
        r"replacement schema differs",
        r"cannot replace .* with",
        r"replacement .* cannot be depended upon",
        r"would violate .* limit",
        r"resource exhausted|exceeds .* limit",
        r"WAIT is not supported",
        r"cannot (alter|drop|modify) .* (reconfigur|in progress)",
        r"sealed",
        r"cannot write in read-only mode",
    )
]
# Errors a read of a still-live collection gets when another invocation drops
# the cluster or index it reads through, beyond the catalog races `sql.classify`
# already rejects. Deliberately excludes "is not readable at any timestamp",
# the symptom of a tombstoned live shard.
READ_RACE_REJECTIONS = [
    re.compile(p)
    for p in (
        r"the transaction's active cluster has been dropped",
        r"' was dropped",
        r"was dropped while executing a statement",
        r"was removed",
    )
]
EXPECTED_REJECTION_SQLSTATES = {
    "2BP01",  # dependent_objects_still_exist
    "53400",  # configuration_limit_exceeded
    # insufficient_resources: a resource limit, or `AlterClusterResourceExhausted`
    # when the controller cannot provision a graceful reconfiguration's target.
    # The reconfiguration record stays, but nothing is left in progress.
    "53000",
    "0A000",  # feature_not_supported: a profile disabled the feature
}


def log(message: str) -> None:
    print(f"lifecycle[{os.getpid()}]: {message}", flush=True)


def outcome_of(error: BaseException) -> sql.Classified:
    """`sql.classify`, with this workload's designed rejections downgraded."""
    c = sql.classify(error)
    if c.outcome is sql.Outcome.VIOLATION and c.sqlstate is not None:
        if c.sqlstate in EXPECTED_REJECTION_SQLSTATES or any(
            p.search(c.template) for p in EXPECTED_REJECTIONS
        ):
            return sql.Classified(sql.Outcome.REJECTED, c.sqlstate, c.template)
    return c


def open_state() -> sqlite3.Connection:
    db = state.open_db(STATE_DB)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS counters (name TEXT PRIMARY KEY, value INTEGER);
        CREATE TABLE IF NOT EXISTS knobs (name TEXT PRIMARY KEY, value REAL);
        CREATE TABLE IF NOT EXISTS rows (
            tbl TEXT PRIMARY KEY, attempted INTEGER, acked INTEGER);
        CREATE TABLE IF NOT EXISTS pending (
            id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT, payload TEXT,
            owner INTEGER, created REAL, signature TEXT);
        CREATE TABLE IF NOT EXISTS inflight (
            token INTEGER PRIMARY KEY AUTOINCREMENT, pid INTEGER, started REAL);
        CREATE TABLE IF NOT EXISTS snapshot (
            id INTEGER PRIMARY KEY CHECK (id = 1), ddl_seq INTEGER,
            signature TEXT, projection TEXT, taken REAL);
        """)
    db.commit()
    rdb = state.open_db(RECOVERY_DB)
    rdb.execute(
        "CREATE TABLE IF NOT EXISTS retry_ledger ("
        " family TEXT, probe_sql TEXT, recorded_at REAL)"
    )
    rdb.commit()
    rdb.close()
    return db


def next_id(db: sqlite3.Connection, name: str) -> int:
    with db:
        db.execute(
            "INSERT INTO counters VALUES (?, 0) ON CONFLICT(name) DO NOTHING", (name,)
        )
        db.execute("UPDATE counters SET value = value + 1 WHERE name = ?", (name,))
        return db.execute(
            "SELECT value FROM counters WHERE name = ?", (name,)
        ).fetchone()[0]


def counter(db: sqlite3.Connection, name: str) -> int:
    row = db.execute("SELECT value FROM counters WHERE name = ?", (name,)).fetchone()
    return row[0] if row else 0


def knobs(db: sqlite3.Connection) -> dict[str, float]:
    """Per-timeline swarm parameters, drawn by the first invocation."""
    rows = dict(db.execute("SELECT name, value FROM knobs").fetchall())
    if rows:
        return rows
    drawn: dict[str, float] = {}
    while not any(drawn.get(a, 0.0) for a in ACTIONS):
        drawn = {a: rng.choice(WEIGHT_MENU) for a in ACTIONS}
    drawn["abandon_p"] = rng.choice(ABANDON_MENU)
    with db:
        db.executemany(
            "INSERT INTO knobs VALUES (?, ?) ON CONFLICT(name) DO NOTHING",
            list(drawn.items()),
        )
    return dict(db.execute("SELECT name, value FROM knobs").fetchall())


def record_retry(family: str) -> None:
    rdb = state.open_db(RECOVERY_DB)
    with rdb:
        rdb.execute(
            "INSERT INTO retry_ledger (family, probe_sql, recorded_at) VALUES (?, NULL, ?)",
            (family, time.time()),
        )
    rdb.close()


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def prune_dead_inflight(db: sqlite3.Connection) -> int:
    """Drop in-flight DDL markers of killed invocations.

    Their DDL outcome is unknown, so pruning also bumps `ddl_seq`, which
    invalidates any snapshot taken before them.
    """
    dead = [
        token
        for token, pid in db.execute("SELECT token, pid FROM inflight").fetchall()
        if not pid_alive(pid)
    ]
    if dead:
        with db:
            db.executemany("DELETE FROM inflight WHERE token = ?", [(t,) for t in dead])
        next_id(db, "ddl_seq")
    return len(dead)


class Session:
    """Connections to the active environmentd, reopened after faults."""

    def __init__(self, env: Environment, db: sqlite3.Connection) -> None:
        self.env = env
        self.host = env.sql_host()
        self.db = db
        self._conn: psycopg.Connection | None = None

    def conn(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self._conn = sql.connect_with_retry(
                self.host,
                CONNECT_DEADLINE_S,
                statement_timeout_ms=DDL_TIMEOUT_MS,
                options={"cluster": configure.SHARED_CLUSTER},
            )
        return self._conn

    def drop_conn(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except psycopg.Error:
                pass
        self._conn = None

    def query(
        self, text: str, params: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]] | None:
        """Run a catalog read; None on a transient failure."""
        try:
            return self.conn().execute(text.encode(), params or None).fetchall()
        except (psycopg.Error, OSError) as e:
            c = outcome_of(e)
            if c.outcome is sql.Outcome.INDETERMINATE:
                self.drop_conn()
            log(f"catalog read failed ({c.outcome.value}): {c.template}")
            return None

    def ddl(
        self,
        family: str,
        text: str,
        timeout_ms: int | None = None,
    ) -> sql.Outcome | None:
        """Run one lifecycle DDL statement. Returns None on success.

        Holds an in-flight marker while it runs and bumps `ddl_seq`, so the
        restart snapshot never spans a DDL. NOTE: the marker must be in place
        before the bump, else `quiet_projection` can read the bumped sequence
        number, see no marker, and miss a DDL that finishes during its reads.
        """
        with self.db:
            token = self.db.execute(
                "INSERT INTO inflight (pid, started) VALUES (?, ?)",
                (os.getpid(), time.time()),
            ).lastrowid
        next_id(self.db, "ddl_seq")
        try:
            conn = self.conn()
            if timeout_ms is not None:
                conn.execute(f"SET statement_timeout = '{timeout_ms}ms'".encode())
            try:
                conn.execute(text.encode())
            finally:
                if timeout_ms is not None and not conn.closed:
                    try:
                        conn.execute(
                            f"SET statement_timeout = '{DDL_TIMEOUT_MS}ms'".encode()
                        )
                    except psycopg.Error:
                        self.drop_conn()
            log(f"ok: {text}")
            return None
        except (psycopg.Error, OSError) as e:
            c = outcome_of(e)
            log(f"{c.outcome.value} ({c.sqlstate}): {text}: {c.template}")
            if c.outcome is sql.Outcome.INDETERMINATE:
                self.drop_conn()
                record_retry(family)
            always_or_unreachable(
                c.outcome is not sql.Outcome.VIOLATION,
                "lifecycle DDL fails only with a classified error",
                {
                    "family": family,
                    "sql": text,
                    "sqlstate": c.sqlstate,
                    "template": c.template,
                },
            )
            return c.outcome
        finally:
            with self.db:
                self.db.execute("DELETE FROM inflight WHERE token = ?", (token,))


def restart_signature(env: Environment) -> str:
    """Pod uid and restart count of every environmentd and clusterd pod."""
    pods = client.CoreV1Api().list_namespaced_pod(env.endpoints.namespace).items
    sig = []
    for pod in pods:
        assert pod.metadata is not None
        labels = pod.metadata.labels or {}
        is_envd = labels.get("materialize.cloud/app") == "environmentd"
        is_clusterd = any(k.endswith("/cluster-id") for k in labels)
        if not (is_envd or is_clusterd):
            continue
        assert pod.status is not None
        restarts = sum(s.restart_count for s in pod.status.container_statuses or [])
        sig.append((pod.metadata.name, pod.metadata.uid, restarts))
    return json.dumps(sorted(sig))


def safe_signature(env: Environment) -> str | None:
    try:
        return restart_signature(env)
    except (ApiException, OSError) as e:
        log(f"cannot read pods: {e}")
        return None


def live_objects(s: Session) -> dict[str, dict[str, Any]] | None:
    """Lifecycle tables, MVs, and indexes by name, from the catalog.

    `replacement` is the target id of a pending replacement MV, `on_id` the
    indexed object of an index.
    """
    rows = s.query(
        "SELECT o.id, o.name, o.type, o.cluster_id, r.target_id, i.on_id"
        " FROM mz_objects o"
        " JOIN mz_schemas sc ON o.schema_id = sc.id"
        " JOIN mz_databases d ON sc.database_id = d.id"
        " LEFT JOIN mz_internal.mz_replacements r ON r.id = o.id"
        " LEFT JOIN mz_indexes i ON i.id = o.id"
        " WHERE sc.name = %s AND d.name = 'materialize'"
        " AND o.type IN ('table', 'materialized-view', 'index')",
        (SCHEMA,),
    )
    if rows is None:
        return None
    return {
        name: {
            "id": id_,
            "type": typ,
            "cluster_id": cluster_id,
            "replacement": target_id,
            "on_id": on_id,
        }
        for id_, name, typ, cluster_id, target_id, on_id in rows
    }


def live_clusters(s: Session) -> dict[str, dict[str, Any]] | None:
    rows = s.query(
        "SELECT c.id, c.name, c.managed, c.size, c.replication_factor,"
        " (SELECT count(*) FROM mz_cluster_replicas r WHERE r.cluster_id = c.id)"
        " FROM mz_clusters c WHERE c.name LIKE %s",
        (CLUSTER_PREFIX + "%",),
    )
    if rows is None:
        return None
    return {
        name: {"id": id_, "managed": managed, "size": size, "rf": rf, "replicas": n}
        for id_, name, managed, size, rf, n in rows
    }


def base_table(name: str) -> str | None:
    m = re.search(r"(t\d+)$", name)
    return m[1] if m else None


def ledger(db: sqlite3.Connection, tbl: str) -> tuple[int, int]:
    row = db.execute(
        "SELECT attempted, acked FROM rows WHERE tbl = ?", (tbl,)
    ).fetchone()
    return (row[0], row[1]) if row else (0, 0)


def read_count(
    s: Session, name: str, cluster: str
) -> tuple[int, sql.Classified | None]:
    """`count(*)` of a lifecycle collection read on `cluster`, or the
    classified error. Errors from a concurrent drop of the cluster or index the
    read goes through are downgraded to rejections."""
    try:
        with sql.connection(
            s.host, statement_timeout_ms=READ_TIMEOUT_MS, options={"cluster": cluster}
        ) as conn:
            row = conn.execute(
                f"SELECT count(*) FROM {QUALIFIED}.{name}".encode()
            ).fetchone()
            return (row[0] if row else 0), None
    except (psycopg.Error, OSError) as e:
        c = sql.classify(e)
        if (
            c.outcome is sql.Outcome.VIOLATION
            and c.sqlstate is not None
            and any(p.search(c.template) for p in READ_RACE_REJECTIONS)
        ):
            c = sql.Classified(sql.Outcome.REJECTED, c.sqlstate, c.template)
        return 0, c


def check_readable(
    s: Session, origin: str, require_success: bool = False
) -> dict[str, Any]:
    """Read every live lifecycle table and MV, and every index's target
    through the index's cluster, and check the row count against the ledger.

    With `require_success` (after faults stop) a timeout also counts as a
    failure, unless the object or the index is on a cluster without replicas,
    where a read cannot complete by design. Returns a summary for details.
    """
    objects = live_objects(s)
    clusters = live_clusters(s) or {}
    if objects is None:
        return {"skipped": "catalog unavailable"}
    replicaless = {c["id"] for c in clusters.values() if c["replicas"] == 0}
    cluster_names = {c["id"]: name for name, c in clusters.items()}
    by_id = {o["id"]: name for name, o in objects.items()}
    # (object to read, cluster to read on, clusters the read depends on)
    reads: list[tuple[str, str, set[str]]] = []
    for name, o in objects.items():
        if o["type"] in ("table", "materialized-view") and not o["replacement"]:
            reads.append((name, configure.SHARED_CLUSTER, {o["cluster_id"]}))
    for o in objects.values():
        on = by_id.get(o["on_id"]) if o["type"] == "index" else None
        if on is None or objects[on]["replacement"]:
            continue
        cluster = cluster_names.get(o["cluster_id"])
        if cluster is None:
            rows = s.query(
                "SELECT name FROM mz_clusters WHERE id = %s", (o["cluster_id"],)
            )
            if not rows:
                continue
            cluster = rows[0][0]
        reads.append((on, cluster, {o["cluster_id"], objects[on]["cluster_id"]}))

    summary: dict[str, Any] = {"ok": 0, "transient": 0, "origin": origin}
    for name, cluster, depends_on in reads:
        tbl = base_table(name)
        if tbl is None:
            continue
        _, acked_before = ledger(s.db, tbl)
        count, c = read_count(s, name, cluster)
        if c is not None and name not in (live_objects(s) or {name: {}}):
            continue
        details = {
            "object": name,
            "cluster": cluster,
            "origin": origin,
            "sqlstate": c.sqlstate if c else None,
            "template": c.template if c else None,
        }
        if require_success and not (depends_on & replicaless):
            always_or_unreachable(
                c is None,
                "a live lifecycle collection is readable after faults stop",
                details,
            )
        if c is not None:
            always_or_unreachable(
                c.outcome is not sql.Outcome.VIOLATION,
                "a live lifecycle collection read fails only with a transient error",
                details,
            )
            summary["transient"] += 1
            continue
        attempted_after, _ = ledger(s.db, tbl)
        always_greater_than_or_equal_to(
            count,
            acked_before,
            "a live lifecycle collection reflects every acknowledged insert",
            details,
        )
        always_less_than_or_equal_to(
            count,
            attempted_after,
            "a live lifecycle collection holds no more rows than were attempted",
            details,
        )
        summary["ok"] += 1
    return summary


def projection(s: Session) -> dict[str, Any] | None:
    """The restart-invariant projection described in the module docstring."""
    objects = live_objects(s)
    clusters = s.query(
        "SELECT c.id, c.name, c.managed, c.size, c.replication_factor"
        " FROM mz_clusters c WHERE c.name LIKE %s ORDER BY c.id",
        (CLUSTER_PREFIX + "%",),
    )
    if objects is None or clusters is None:
        return None
    ids = sorted(o["id"] for o in objects.values())
    cluster_ids = sorted(c[0] for c in clusters)
    queries = {
        "items": (
            "SELECT o.id, o.name, o.type, coalesce(o.cluster_id, '') FROM mz_objects o"
            " WHERE o.id = ANY(%s::text[]) ORDER BY o.id",
            (ids,),
        ),
        "global_ids": (
            "SELECT id, global_id FROM mz_internal.mz_object_global_ids"
            " WHERE id = ANY(%s::text[]) ORDER BY id, global_id",
            (ids,),
        ),
        "shards": (
            "SELECT g.id, s.object_id, s.shard_id FROM mz_internal.mz_storage_shards s"
            " JOIN mz_internal.mz_object_global_ids g ON g.global_id = s.object_id"
            " WHERE g.id = ANY(%s::text[]) ORDER BY g.id, s.object_id",
            (ids,),
        ),
        "retention": (
            "SELECT id, strategy, value::text FROM mz_internal.mz_history_retention_strategies"
            " WHERE id = ANY(%s::text[]) ORDER BY id",
            (ids,),
        ),
        "replacements": (
            "SELECT id, target_id FROM mz_internal.mz_replacements"
            " WHERE id = ANY(%s::text[]) ORDER BY id",
            (ids,),
        ),
        "indexes": (
            "SELECT id, on_id, cluster_id FROM mz_indexes WHERE id = ANY(%s::text[]) ORDER BY id",
            (ids,),
        ),
        "replicas": (
            "SELECT id, name, cluster_id, coalesce(size, '') FROM mz_cluster_replicas"
            " WHERE cluster_id = ANY(%s::text[]) ORDER BY id",
            (cluster_ids,),
        ),
        "reconfigurations": (
            "SELECT cluster_id, status, deadline::text, on_timeout, target::text"
            " FROM mz_internal.mz_cluster_reconfigurations"
            " WHERE cluster_id = ANY(%s::text[]) ORDER BY cluster_id",
            (cluster_ids,),
        ),
    }
    result: dict[str, Any] = {"clusters": [list(map(str, c)) for c in clusters]}
    for family, (text, params) in queries.items():
        rows = s.query(text, params)
        if rows is None:
            return None
        result[family] = [list(map(str, r)) for r in rows]
    result["dump"] = dump_projection(
        s.host, sorted({g for _, g in result["global_ids"]})
    )
    return result


_ANTICHAIN = re.compile(r"Antichain \{ elements: \[[^\]]*\] \}")
_PRIMARY = re.compile(r"\bprimary: (None|Some\([^)]*\)\))")
_REMAP = re.compile(r"\bingestion_remap_collection_id: (None|Some\([^)]*\)\))")
_DEPENDENCIES = re.compile(r"\bstorage_dependencies: (\[[^\]]*\])")
_DATA_SHARD = re.compile(r"\bdata_shard: (ShardId\([0-9a-f-]+\))")
_TXNS_SHARD = re.compile(r"\btxns_shard: (None|Some\(ShardId\([0-9a-f-]+\)\))")
_READ_POLICY = re.compile(r"\bread_policy: (.*), storage_dependencies: ")


def _field(pattern: re.Pattern[str], text: str) -> str | None:
    m = pattern.search(text)
    return m[1] if m else None


def dump_projection(host: str, global_ids: list[str]) -> dict[str, Any] | None:
    """Storage controller state of `global_ids` from `/api/coordinator/dump`,
    or None if the dump cannot be read. See the module docstring."""
    try:
        response = requests.get(
            f"http://{host}:{INTERNAL_HTTP_PORT}/api/coordinator/dump",
            timeout=DUMP_TIMEOUT_S,
        )
        response.raise_for_status()
        collections = response.json()["controller"]["storage_collections"][
            "collections"
        ]
    except (requests.RequestException, ValueError, KeyError, TypeError) as e:
        log(f"cannot read the coordinator dump: {e}")
        return None
    links = []
    policies = []
    for gid in global_ids:
        text = collections.get(gid)
        if not isinstance(text, str):
            continue
        links.append(
            [
                gid,
                _field(_PRIMARY, text),
                _field(_REMAP, text),
                _field(_DEPENDENCIES, text),
                _field(_DATA_SHARD, text),
                _field(_TXNS_SHARD, text),
            ]
        )
        policy = _field(_READ_POLICY, text)
        if policy is not None and not policy.startswith("NoPolicy"):
            policies.append([gid, _ANTICHAIN.sub("Antichain", policy)])
    return {"collections": links, "read_policies": policies}


def _strip_reconfiguring(
    a: dict[str, Any], b: dict[str, Any]
) -> tuple[set[str], dict[str, list[Any]], dict[str, list[Any]]]:
    busy = {
        r[0]
        for snap in (a, b)
        for r in snap.get("reconfigurations", [])
        if r[1] == "in-progress"
    }

    def strip(snap: dict[str, Any]) -> dict[str, list[Any]]:
        return {
            "clusters": [
                c if c[0] not in busy else c[:3] for c in snap.get("clusters", [])
            ],
            "replicas": [r for r in snap.get("replicas", []) if r[2] not in busy],
            # `status` advances without DDL.
            "reconfigurations": [
                [r[0], *r[2:]] for r in snap.get("reconfigurations", [])
            ],
        }

    return busy, strip(a), strip(b)


def compare_projections(
    before: dict[str, Any], after: dict[str, Any], origin: str
) -> None:
    """One `always` per state family, so the report names what diverged."""
    busy, sa, sb = _strip_reconfiguring(before, after)

    def details(family: str, x: Any, y: Any) -> dict[str, Any]:
        return {
            "origin": origin,
            "family": family,
            "before": x,
            "after": y,
            "reconfiguring": sorted(busy),
        }

    always_or_unreachable(
        before["items"] == after["items"],
        "restart preserved lifecycle catalog items",
        details("items", before["items"], after["items"]),
    )
    always_or_unreachable(
        before["global_ids"] == after["global_ids"],
        "restart preserved lifecycle global ids",
        details("global_ids", before["global_ids"], after["global_ids"]),
    )
    always_or_unreachable(
        before["shards"] == after["shards"],
        "restart preserved lifecycle storage shard mapping",
        details("shards", before["shards"], after["shards"]),
    )
    always_or_unreachable(
        before["retention"] == after["retention"],
        "restart preserved lifecycle history retention strategies",
        details("retention", before["retention"], after["retention"]),
    )
    always_or_unreachable(
        before["replacements"] == after["replacements"],
        "restart preserved lifecycle pending replacements",
        details("replacements", before["replacements"], after["replacements"]),
    )
    always_or_unreachable(
        before["indexes"] == after["indexes"],
        "restart preserved lifecycle index placement",
        details("indexes", before["indexes"], after["indexes"]),
    )
    always_or_unreachable(
        sa["clusters"] == sb["clusters"],
        "restart preserved lifecycle cluster configuration",
        details("clusters", sa["clusters"], sb["clusters"]),
    )
    always_or_unreachable(
        sa["replicas"] == sb["replicas"],
        "restart preserved lifecycle cluster replicas",
        details("replicas", sa["replicas"], sb["replicas"]),
    )
    always_or_unreachable(
        sa["reconfigurations"] == sb["reconfigurations"],
        "restart preserved lifecycle reconfiguration records",
        details("reconfigurations", sa["reconfigurations"], sb["reconfigurations"]),
    )

    da, db = before.get("dump"), after.get("dump")
    sometimes(
        da is not None and db is not None,
        "a restart snapshot comparison included the coordinator dump",
        {"origin": origin},
    )
    if da is None or db is None:
        return
    always_or_unreachable(
        da["collections"] == db["collections"],
        "restart preserved lifecycle storage collection primaries, dependencies, and shards",
        details("dump.collections", da["collections"], db["collections"]),
    )
    # A collection between restart and its first policy install reports
    # `NoPolicy`, which `dump_projection` drops, so compare only ids present
    # on both sides.
    pa, pb = dict(map(tuple, da["read_policies"])), dict(
        map(tuple, db["read_policies"])
    )
    common = sorted(pa.keys() & pb.keys())
    always_or_unreachable(
        all(pa[g] == pb[g] for g in common),
        "restart preserved lifecycle storage collection read policies",
        details(
            "dump.read_policies",
            [[g, pa[g]] for g in common],
            [[g, pb[g]] for g in common],
        ),
    )


def quiet_projection(s: Session) -> tuple[int, dict[str, Any]] | None:
    """A projection no lifecycle DDL overlapped, with its `ddl_seq`."""
    prune_dead_inflight(s.db)
    seq0 = counter(s.db, "ddl_seq")
    if s.db.execute("SELECT count(*) FROM inflight").fetchone()[0]:
        return None
    proj = projection(s)
    seq1 = counter(s.db, "ddl_seq")
    if proj is None or seq0 != seq1:
        return None
    if s.db.execute("SELECT count(*) FROM inflight").fetchone()[0]:
        return None
    return seq0, proj


def save_snapshot(s: Session, origin: str) -> None:
    sig = safe_signature(s.env)
    snap = quiet_projection(s)
    if sig is None or snap is None:
        return
    seq, proj = snap
    with s.db:
        s.db.execute(
            "INSERT INTO snapshot VALUES (1, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET"
            " ddl_seq = excluded.ddl_seq, signature = excluded.signature,"
            " projection = excluded.projection, taken = excluded.taken",
            (seq, sig, json.dumps(proj), time.time()),
        )
    log(f"saved snapshot at ddl_seq {seq} ({origin})")


def compare_with_snapshot(s: Session, origin: str) -> bool:
    """Compare the saved snapshot with the current state if a restart
    happened in between and no lifecycle DDL did. Returns whether it compared."""
    row = s.db.execute(
        "SELECT ddl_seq, signature, projection FROM snapshot WHERE id = 1"
    ).fetchone()
    if row is None:
        return False
    seq, sig, proj = row
    current_sig = safe_signature(s.env)
    if current_sig is None or current_sig == sig:
        return False
    now = quiet_projection(s)
    comparable = now is not None and now[0] == seq
    # A run where DDL always overlaps the restart never compares; this shows it.
    sometimes(
        comparable,
        "a restart snapshot comparison ran with no lifecycle DDL since the snapshot",
        {"origin": origin},
    )
    if now is None or not comparable:
        return False
    compare_projections(json.loads(proj), now[1], origin)
    return True


def add_pending(s: Session, kind: str, payload: dict[str, Any]) -> None:
    with s.db:
        s.db.execute(
            "INSERT INTO pending (kind, payload, owner, created, signature)"
            " VALUES (?, ?, NULL, ?, ?)",
            (kind, json.dumps(payload), time.time(), safe_signature(s.env)),
        )


def claim_pending(s: Session) -> tuple[int, str, dict[str, Any], str | None] | None:
    """Take ownership of one pending op that no live invocation owns."""
    s.db.execute("BEGIN IMMEDIATE")
    try:
        rows = s.db.execute(
            "SELECT id, kind, payload, owner, signature FROM pending"
        ).fetchall()
        free = [r for r in rows if r[3] is None or not pid_alive(r[3])]
        if not free:
            s.db.commit()
            return None
        id_, kind, payload, _, sig = rng.choice(free)
        s.db.execute("UPDATE pending SET owner = ? WHERE id = ?", (os.getpid(), id_))
        s.db.commit()
    except BaseException:
        s.db.rollback()
        raise
    return id_, kind, json.loads(payload), sig


def finish_pending(s: Session, id_: int) -> None:
    with s.db:
        s.db.execute("DELETE FROM pending WHERE id = ?", (id_,))


def release_pending(s: Session, id_: int) -> None:
    with s.db:
        s.db.execute("UPDATE pending SET owner = NULL WHERE id = ?", (id_,))


def pick(names: list[str]) -> str | None:
    return rng.choice(names) if names else None


def tables(objects: dict[str, dict[str, Any]]) -> list[str]:
    return sorted(n for n, o in objects.items() if o["type"] == "table")


def mvs(objects: dict[str, dict[str, Any]], replacement: bool = False) -> list[str]:
    return sorted(
        n
        for n, o in objects.items()
        if o["type"] == "materialized-view" and bool(o["replacement"]) == replacement
    )


def compute_clusters(s: Session) -> list[str]:
    return [configure.SHARED_CLUSTER, *sorted((live_clusters(s) or {}).keys())]


class Driver:
    def __init__(self, s: Session, weights: dict[str, float]) -> None:
        self.s = s
        self.weights = weights
        self.abandon = False

    def maybe_abandon(self) -> None:
        if rng.random() < self.weights["abandon_p"]:
            self.abandon = True

    # Single-step actions.

    def create_table(self, objects: dict[str, dict[str, Any]]) -> None:
        if len(tables(objects)) >= MAX_TABLES:
            return self.drop_object(objects)
        name = f"t{next_id(self.s.db, 'table')}"
        with self.s.db:
            self.s.db.execute("INSERT OR IGNORE INTO rows VALUES (?, 0, 0)", (name,))
        self.s.ddl(
            "ddl_create_table", f"CREATE TABLE {QUALIFIED}.{name} (k int, v int)"
        )
        self.insert(objects, name)

    def insert(
        self, objects: dict[str, dict[str, Any]], name: str | None = None
    ) -> None:
        name = name or pick(tables(objects))
        if name is None:
            return
        n = rng.choice(INSERT_MENU)
        if n == 0:
            return
        start = next_id(self.s.db, "row") * 1000
        values = ", ".join(f"({start + i}, {rng.randint(-5, 5)})" for i in range(n))
        with self.s.db:
            self.s.db.execute(
                "UPDATE rows SET attempted = attempted + ? WHERE tbl = ?", (n, name)
            )
        try:
            self.s.conn().execute(
                f"INSERT INTO {QUALIFIED}.{name} VALUES {values}".encode()
            )
        except (psycopg.Error, OSError) as e:
            c = outcome_of(e)
            if c.outcome is sql.Outcome.REJECTED:
                with self.s.db:
                    self.s.db.execute(
                        "UPDATE rows SET attempted = attempted - ? WHERE tbl = ?",
                        (n, name),
                    )
            elif c.outcome is sql.Outcome.INDETERMINATE:
                self.s.drop_conn()
                record_retry("dml_insert")
            always_or_unreachable(
                c.outcome is not sql.Outcome.VIOLATION,
                "lifecycle INSERT fails only with a classified error",
                {"table": name, "sqlstate": c.sqlstate, "template": c.template},
            )
            return
        with self.s.db:
            self.s.db.execute(
                "UPDATE rows SET acked = acked + ? WHERE tbl = ?", (n, name)
            )

    def create_mv(
        self,
        objects: dict[str, dict[str, Any]],
        table: str | None = None,
        column: str | None = None,
    ) -> None:
        if len(mvs(objects)) >= MAX_MVS:
            return self.drop_object(objects)
        table = table or pick(tables(objects))
        if table is None:
            return
        cluster = rng.choice(compute_clusters(self.s))
        name = f"mv{next_id(self.s.db, 'mv')}_{table}"
        retain = (
            f" WITH (RETAIN HISTORY FOR '{rng.choice(['1s', '1m', '1h'])}')"
            if rng.random() < 0.3
            else ""
        )
        cols = f"k, v, {column}" if column else "k, v"
        self.s.ddl(
            "ddl_create_mv",
            f"CREATE MATERIALIZED VIEW {QUALIFIED}.{name} IN CLUSTER {cluster}{retain}"
            f" AS SELECT {cols} FROM {QUALIFIED}.{table}",
        )

    def create_index(self, objects: dict[str, dict[str, Any]]) -> None:
        if sum(o["type"] == "index" for o in objects.values()) >= MAX_INDEXES:
            return self.drop_object(objects)
        on = pick(tables(objects) + mvs(objects))
        if on is None:
            return
        cluster = rng.choice(compute_clusters(self.s))
        name = f"ix{next_id(self.s.db, 'index')}_{on}"
        self.s.ddl(
            "ddl_index",
            f"CREATE INDEX {name} IN CLUSTER {cluster} ON {QUALIFIED}.{on} (k)",
        )

    def drop_object(self, objects: dict[str, dict[str, Any]]) -> None:
        name = pick(sorted(objects))
        if name is None:
            return
        kind = {
            "table": "TABLE",
            "materialized-view": "MATERIALIZED VIEW",
            "index": "INDEX",
        }[objects[name]["type"]]
        cascade = " CASCADE" if rng.random() < 0.8 else ""
        self.s.ddl("ddl_drop", f"DROP {kind} {QUALIFIED}.{name}{cascade}")

    def create_cluster(self, objects: dict[str, dict[str, Any]]) -> None:
        clusters = live_clusters(self.s)
        if clusters is None:
            return
        if len(clusters) >= MAX_CLUSTERS:
            return self.drop_cluster(objects)
        n = next_id(self.s.db, "cluster")
        size = rng.choice(SIZES[:2])
        if rng.random() < 0.7:
            rf = rng.choice(RF_MENU)
            self.s.ddl(
                "ddl_cluster",
                f"CREATE CLUSTER {CLUSTER_PREFIX}c{n} (SIZE '{size}', REPLICATION FACTOR {rf})",
            )
        else:
            self.s.ddl(
                "ddl_cluster",
                f"CREATE CLUSTER {CLUSTER_PREFIX}u{n} REPLICAS (r1 (SIZE '{size}'))",
            )

    def drop_cluster(self, objects: dict[str, dict[str, Any]]) -> None:
        name = pick(sorted(live_clusters(self.s) or {}))
        if name is not None:
            self.s.ddl("ddl_drop", f"DROP CLUSTER {name} CASCADE")

    def alter_cluster_rf(self, objects: dict[str, dict[str, Any]]) -> None:
        managed = [n for n, c in (live_clusters(self.s) or {}).items() if c["managed"]]
        name = pick(managed)
        if name is not None:
            self.s.ddl(
                "ddl_alter_cluster",
                f"ALTER CLUSTER {name} SET (REPLICATION FACTOR {rng.choice(RF_MENU)})",
            )

    def flag_roll(self, objects: dict[str, dict[str, Any]]) -> None:
        try:
            flag = configure.roll_flag(self.s.env)
            log(f"rolled {flag}")
        except (ApiException, OSError, RuntimeError) as e:
            log(f"flag roll failed: {e}")

    # Multi-step actions. Each records a pending op, then may abandon.

    def add_column(
        self,
        objects: dict[str, dict[str, Any]],
        table: str | None = None,
        remaining: int | None = None,
    ) -> None:
        table = table or pick(tables(objects))
        if table is None:
            return
        remaining = (
            remaining if remaining is not None else rng.choice(ADD_COLUMN_CHAIN_MENU)
        )
        column = f"c{next_id(self.s.db, 'column')}"
        outcome = self.s.ddl(
            "ddl_alter_table",
            f"ALTER TABLE {QUALIFIED}.{table} ADD COLUMN {column} text",
        )
        if outcome is sql.Outcome.REJECTED:
            return
        if remaining > 1:
            add_pending(
                self.s,
                "add_column",
                {"table": table, "column": column, "remaining": remaining - 1},
            )
            self.maybe_abandon()
        else:
            # The chain ends with a dependent on the newest table version.
            self.create_mv(live_objects(self.s) or objects, table, column)

    def create_replacement(
        self, objects: dict[str, dict[str, Any]], target: str | None = None
    ) -> None:
        # One pending replacement per target; a second is rejected by design.
        replaced = {o["replacement"] for o in objects.values() if o["replacement"]}
        target = target or pick(
            [m for m in mvs(objects) if objects[m]["id"] not in replaced]
        )
        if target is None:
            return
        table = base_table(target)
        columns = self.s.query(
            "SELECT name FROM mz_columns WHERE id = %s ORDER BY position",
            (objects[target]["id"],),
        )
        if not columns:
            return
        # `v + 1` keeps the column name, type, nullability, and row count, so
        # the replacement's schema matches the target's.
        select = ", ".join("v + 1 AS v" if c == "v" else c for (c,) in columns)
        cluster = rng.choice(compute_clusters(self.s))
        name = f"rp{next_id(self.s.db, 'replacement')}_{target}"
        outcome = self.s.ddl(
            "ddl_replacement",
            f"CREATE REPLACEMENT MATERIALIZED VIEW {QUALIFIED}.{name}"
            f" FOR {QUALIFIED}.{target} IN CLUSTER {cluster}"
            f" AS SELECT {select} FROM {QUALIFIED}.{table}",
        )
        if outcome is sql.Outcome.REJECTED:
            return
        add_pending(self.s, "replacement", {"replacement": name, "target": target})
        self.maybe_abandon()

    def apply_replacement(
        self, objects: dict[str, dict[str, Any]], replacement: str | None = None
    ) -> None:
        replacement = replacement or pick(mvs(objects, replacement=True))
        if replacement is None:
            return
        target = re.sub(r"^rp\d+_", "", replacement)
        timeout_ms = rng.choice(APPLY_TIMEOUT_MENU_MS)
        outcome = self.s.ddl(
            "ddl_apply_replacement",
            f"ALTER MATERIALIZED VIEW {QUALIFIED}.{target}"
            f" APPLY REPLACEMENT {QUALIFIED}.{replacement}",
            timeout_ms=timeout_ms,
        )
        if outcome is sql.Outcome.INDETERMINATE:
            target_live = target in (live_objects(self.s) or {target: {}})
            if not target_live:
                # The wait hangs if the target is dropped during it.
                reachable(
                    "APPLY REPLACEMENT wait abandoned after its target was dropped (known bug database-issues#9820)",
                    {"target": target, "replacement": replacement},
                )
            add_pending(self.s, "apply", {"replacement": replacement, "target": target})
            self.maybe_abandon()

    def drop_replacement(
        self, objects: dict[str, dict[str, Any]], replacement: str | None = None
    ) -> None:
        replacement = replacement or pick(mvs(objects, replacement=True))
        if replacement is not None:
            self.s.ddl("ddl_drop", f"DROP MATERIALIZED VIEW {QUALIFIED}.{replacement}")

    def alter_cluster_graceful(
        self, objects: dict[str, dict[str, Any]], name: str | None = None
    ) -> None:
        managed = [n for n, c in (live_clusters(self.s) or {}).items() if c["managed"]]
        name = name or pick(managed)
        if name is None:
            return
        size = rng.choice(SIZES)
        shape = f"SIZE '{size}'"
        if rng.random() < 0.3:
            shape += f", REPLICATION FACTOR {rng.choice(RF_MENU[1:])}"
        timeout = rng.choice(WAIT_TIMEOUT_MENU)
        wait = rng.choice(
            [
                f" WITH (WAIT UNTIL READY (TIMEOUT '{timeout}', ON TIMEOUT 'ROLLBACK'))",
                f" WITH (WAIT UNTIL READY (TIMEOUT '{timeout}', ON TIMEOUT 'COMMIT'))",
                f" WITH (WAIT FOR '{timeout}')",
                "",
            ]
        )
        outcome = self.s.ddl(
            "ddl_alter_cluster", f"ALTER CLUSTER {name} SET ({shape}){wait}"
        )
        if outcome is sql.Outcome.REJECTED:
            return
        add_pending(self.s, "reconfig", {"cluster": name})
        self.maybe_abandon()

    def replica_swap(self, objects: dict[str, dict[str, Any]]) -> None:
        unmanaged = {
            n: c for n, c in (live_clusters(self.s) or {}).items() if not c["managed"]
        }
        name = pick(sorted(unmanaged))
        if name is None:
            return
        if unmanaged[name]["replicas"] >= MAX_REPLICAS:
            return
        rows = self.s.query(
            "SELECT r.name FROM mz_cluster_replicas r JOIN mz_clusters c"
            " ON r.cluster_id = c.id WHERE c.name = %s",
            (name,),
        )
        if rows is None:
            return
        old = pick(sorted(r[0] for r in rows))
        new = f"r{next_id(self.s.db, 'replica')}"
        outcome = self.s.ddl(
            "ddl_replica",
            f"CREATE CLUSTER REPLICA {name}.{new} SIZE '{rng.choice(SIZES[:2])}'",
        )
        if outcome is sql.Outcome.REJECTED or old is None:
            return
        add_pending(self.s, "replica_swap", {"cluster": name, "old": old, "new": new})
        self.maybe_abandon()

    def step(self, action: str) -> None:
        objects = live_objects(self.s)
        if objects is None:
            return
        getattr(self, action)(objects)

    # Resumption of operations left pending by this or earlier invocations.

    def resume(self) -> None:
        claimed = claim_pending(self.s)
        if claimed is None:
            return
        id_, kind, payload, sig = claimed
        objects = live_objects(self.s)
        clusters = live_clusters(self.s)
        if objects is None or clusters is None:
            release_pending(self.s, id_)
            return
        current_sig = safe_signature(self.s.env)
        sometimes(
            sig is not None and current_sig is not None and sig != current_sig,
            "an interrupted lifecycle operation is resumed after a pod restart",
            {"kind": kind, "payload": payload},
        )
        log(f"resuming {kind} {payload}")
        if kind == "replacement" or kind == "apply":
            replacement = payload["replacement"]
            still_pending = (
                replacement in objects and objects[replacement]["replacement"]
            )
            if kind == "replacement":
                sometimes(
                    bool(still_pending),
                    "a pending replacement MV is still pending when a later invocation resumes it",
                    payload,
                )
            else:
                sometimes(
                    bool(still_pending),
                    "an abandoned APPLY REPLACEMENT left its replacement pending for a later invocation",
                    payload,
                )
            if still_pending:
                if rng.random() < 0.6:
                    self.apply_replacement(objects, replacement)
                else:
                    self.drop_replacement(objects, replacement)
        elif kind == "reconfig":
            cluster = payload["cluster"]
            status = None
            if cluster in clusters:
                rows = self.s.query(
                    "SELECT status FROM mz_internal.mz_cluster_reconfigurations"
                    " WHERE cluster_id = %s",
                    (clusters[cluster]["id"],),
                )
                status = rows[0][0] if rows else None
            sometimes(
                status == "in-progress",
                "a graceful cluster reconfiguration is still in progress when a later invocation resumes it",
                {**payload, "status": status},
            )
            if status == "in-progress":
                choice = rng.choice(["fold", "drop", "leave"])
                if choice == "fold":
                    self.alter_cluster_graceful(objects, cluster)
                elif choice == "drop":
                    self.s.ddl("ddl_drop", f"DROP CLUSTER {cluster} CASCADE")
        elif kind == "add_column":
            table = payload["table"]
            sometimes(
                table in objects,
                "an ALTER TABLE ADD COLUMN chain is resumed with its table still live",
                payload,
            )
            if table in objects:
                self.add_column(objects, table, payload["remaining"])
        elif kind == "replica_swap":
            cluster = payload["cluster"]
            rows = self.s.query(
                "SELECT r.name FROM mz_cluster_replicas r JOIN mz_clusters c"
                " ON r.cluster_id = c.id WHERE c.name = %s",
                (cluster,),
            )
            if rows is None:
                release_pending(self.s, id_)
                return
            names = {r[0] for r in rows}
            both = payload["old"] in names and payload["new"] in names
            sometimes(
                both,
                "a replica swap left both replicas live for a later invocation",
                payload,
            )
            if payload["old"] in names:
                self.s.ddl(
                    "ddl_replica", f"DROP CLUSTER REPLICA {cluster}.{payload['old']}"
                )
        finish_pending(self.s, id_)


def choose_action(weights: dict[str, float]) -> str:
    total = sum(weights[a] for a in ACTIONS)
    x = rng.random() * total
    for a in ACTIONS:
        x -= weights[a]
        if x < 0:
            return a
    return ACTIONS[-1]


def ensure_schema(s: Session) -> bool:
    try:
        s.conn().execute(f"CREATE SCHEMA IF NOT EXISTS {QUALIFIED}")
        return True
    except (psycopg.Error, OSError) as e:
        c = outcome_of(e)
        s.drop_conn()
        log(f"schema setup failed ({c.outcome.value}): {c.template}")
        return False


def run(s: Session, weights: dict[str, float]) -> None:
    driver = Driver(s, weights)
    deadline = time.monotonic() + RUN_BUDGET_S

    if compare_with_snapshot(s, "lifecycle"):
        check_readable(s, "after-restart")

    for _ in range(rng.choice(RESUME_MENU)):
        if time.monotonic() >= deadline or driver.abandon:
            break
        driver.resume()
        check_readable(s, "after-resume")

    for _ in range(rng.choice(STEP_MENU)):
        if time.monotonic() >= deadline or driver.abandon:
            break
        action = choose_action(weights)
        log(f"step {action}")
        driver.step(action)
        log(f"readable: {check_readable(s, action)}")

    if driver.abandon:
        reachable("lifecycle invocation abandoned a multi-step operation mid-way", {})
        return
    save_snapshot(s, "lifecycle")


def main() -> int:
    db = open_state()
    weights = knobs(db)
    try:
        env = Environment()
        s = Session(env, db)
        s.conn()
    except (psycopg.Error, OSError, RuntimeError, ApiException) as e:
        log(f"environment unreachable, nothing to do: {e}")
        return 0
    try:
        if ensure_schema(s):
            run(s, weights)
    except (psycopg.Error, OSError, ApiException) as e:
        # Every SQL and Kubernetes call above already tolerates faults; this
        # catches a reconnect that outlasted its deadline.
        log(f"giving up this invocation: {e}")
    finally:
        s.drop_conn()
    return 0


if __name__ == "__main__":
    sys.exit(main())

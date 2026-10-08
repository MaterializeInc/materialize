# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Checks that a read-only (0dt candidate) generation stays isolated.

`anytime_main` (`anytime_read_only_isolation`) runs four checks per
invocation. Attribution and the never-promoted bracket are in
`materialize.antithesis.isolation`; every check that blames a generation reads
the CR before and after its observations and gives up when the two reads do
not bracket one active generation.

* Persist: while a candidate is live, `persistcli inspect state` on a sample
  of the leader's shards (`mz_storage_shards` read through the active
  generation, plus the txns shard). No writer or critical reader may come
  from a never-promoted generation, and the shard's data version may not be
  newer than the leader's build.
* Direct writes: an `INSERT` (and sometimes a `CREATE TABLE`) sent to the
  candidate environmentd's pod IP must not be acknowledged. The pod's
  incarnation is read before and after, so the IP cannot have moved to
  another pod in between.
* Upstream: replication connections (`walsender` backends) to the upstream
  Postgres are attributed to pods by client IP, in two samples at least
  `WALSENDER_GAP_S` apart. Postgres ingestions do not run in read-only mode
  (`supports_read_only` is false), so none may come from a never-promoted
  generation.
* Timestamp oracle: `tsoracle.timestamp_oracle` in the metadata Postgres is
  sampled throughout. Per timeline, `read_ts` and `write_ts` never go below a
  value committed before the sample started, and `read_ts <= write_ts`.
  Whether a read-only generation writes the oracle is not observable here,
  since the leader advances it continuously; the oracle panics on any write
  in read-only mode, which reports through the generic panic hook.

Kafka is not checked. Read-only Kafka ingestions run by design and commit
consumer offsets derived from the shared shard upper. Sink producers are not
attributed to a generation; `kafka_sinks` catches a read-only generation's
sink writes only through their effects on the committed output.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import time
from collections.abc import Callable
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always_or_unreachable,
    sometimes,
)

from materialize.antithesis import sql, state
from materialize.antithesis.drivers import rollouts, shard_audit
from materialize.antithesis.endpoints import Endpoints
from materialize.antithesis.environment import Environment
from materialize.antithesis.isolation import (
    CrPoint,
    OracleMark,
    PodRef,
    Walsender,
    advance_marks,
    data_version_exceeds,
    foreign_handles,
    foreign_walsenders,
    oracle_regressions,
    pod_generation,
    promotable_ceiling,
    version_tuple,
)
from materialize.antithesis.rng import rng

STATE_DB = "read_only_isolation"
PROBE_TABLE = "materialize.public.read_only_probe"
# CR reasons under which the candidate, if any, has not been told to promote.
READ_ONLY_REASONS = ("Applying", "ReadyToPromote")

# Wall time of one invocation. The walsender check needs at least
# `WALSENDER_GAP_S` of it.
RUN_S = 120.0
ORACLE_INTERVAL_S = 2.0
# Postgres' default `wal_sender_timeout` is 60 s, and the upstream does not
# override it. Within it a connection from a pod that died is reset once the
# IP's new owner answers a keepalive.
WALSENDER_GAP_S = 75.0
USER_SHARDS = 2
SYSTEM_SHARDS = 1
PERSISTCLI_TIMEOUT_S = 60
PG_TIMEOUT_S = 10
PROBE_TIMEOUT_MS = 10_000
STDERR_TAIL_CHARS = 2000

ORACLE_SQL = (
    "SELECT timeline, read_ts::text, write_ts::text FROM tsoracle.timestamp_oracle"
)
WALSENDER_SQL = """
SELECT a.pid, a.backend_start::text, host(a.client_addr), s.slot_name
FROM pg_stat_activity a
LEFT JOIN pg_replication_slots s ON s.active_pid = a.pid
WHERE a.backend_type = 'walsender'
"""

ERRORS: tuple[type[BaseException], ...] = (
    *rollouts.TRANSIENT_ERRORS,
    json.JSONDecodeError,
)


def log(message: str) -> None:
    print(f"read_only_isolation[{os.getpid()}]: {message}", flush=True)


def open_state() -> sqlite3.Connection:
    db = state.open_db(STATE_DB)
    db.execute(
        "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )
    db.commit()
    return db


def kv_get(db: sqlite3.Connection, key: str) -> Any:
    row = db.execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
    return json.loads(row[0]) if row else None


def kv_set(db: sqlite3.Connection, key: str, value: Any) -> None:
    db.execute(
        "INSERT INTO kv (key, value) VALUES (?, ?)"
        " ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        (key, json.dumps(value)),
    )


class Checker:
    def __init__(self) -> None:
        self.env = Environment()
        self.endpoints: Endpoints = self.env.endpoints
        self.kube = rollouts.Kube(self.env)
        self.db = open_state()
        self.host = self.env.sql_host()
        self.candidate_live = False

    def cr_point(self) -> CrPoint | None:
        snap = self.kube.try_snapshot()
        return CrPoint(snap.active, snap.reason) if snap is not None else None

    def generation_pods(self) -> dict[str, PodRef]:
        """Every environmentd and clusterd pod with an IP, keyed by IP."""
        pods = self.kube.core.list_namespaced_pod(
            self.endpoints.namespace,
            _request_timeout=rollouts.K8S_TIMEOUT_SECONDS,  # pyright: ignore[reportCallIssue]
        ).items
        out = {}
        for pod in pods:
            metadata, status = pod.metadata, pod.status
            if metadata is None or status is None or not status.pod_ip:
                continue
            generation = pod_generation(metadata.name or "")
            if generation is not None and metadata.uid is not None:
                out[status.pod_ip] = PodRef(metadata.name, metadata.uid, generation)
        return out

    def oracle_sample(self) -> None:
        started = time.monotonic()
        try:
            with psycopg.connect(
                self.endpoints.metadata_postgres_url,
                connect_timeout=PG_TIMEOUT_S,
                autocommit=True,
            ) as conn:
                conn.execute(f"SET statement_timeout = '{PG_TIMEOUT_S}s'".encode())
                raw = conn.execute(ORACLE_SQL.encode()).fetchall()
        except (psycopg.Error, OSError) as e:
            log(f"oracle sample skipped: {e}")
            return
        finished = time.monotonic()
        rows = [(str(t), int(r), int(w)) for t, r, w in raw]
        point = self.cr_point()
        active = point.active if point else None
        if point is not None:
            self.candidate_live = point.reason in READ_ONLY_REASONS
        self.db.execute("BEGIN IMMEDIATE")
        try:
            stored = kv_get(self.db, "oracle_marks") or []
            marks = {(t, c): OracleMark(int(v), float(f)) for t, c, v, f in stored}
            regressions = oracle_regressions(marks, rows, started)
            always_or_unreachable(
                not regressions,
                "timestamp oracle: read_ts and write_ts of every timeline never go backwards",
                {"regressions": regressions, "rows": [list(r) for r in rows]},
            )
            inverted = [list(r) for r in rows if r[1] > r[2]]
            always_or_unreachable(
                not inverted,
                "timestamp oracle: read_ts never exceeds write_ts",
                {"rows": inverted},
            )
            advanced = any(
                (t, "write_ts") in marks and w > marks[(t, "write_ts")].value
                for t, _, w in rows
            )
            sometimes(
                advanced and self.candidate_live,
                "timestamp oracle advanced while a read-only generation was live",
                {"rows": [list(r) for r in rows], "active": active},
            )
            previous_active = kv_get(self.db, "oracle_active")
            sometimes(
                bool(marks)
                and previous_active is not None
                and active is not None
                and active > previous_active
                and not regressions,
                "timestamp oracle check spanned a promotion",
                {"previous_active": previous_active, "active": active},
            )
            marks = advance_marks(marks, rows, finished)
            kv_set(
                self.db,
                "oracle_marks",
                [[t, c, m.value, m.finished] for (t, c), m in marks.items()],
            )
            if active is not None:
                kv_set(self.db, "oracle_active", active)
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def inspect_state(self, shard_id: str) -> dict[str, Any] | None:
        env = {
            **os.environ,
            "CONSENSUS_URI": shard_audit.consensus_url(self.endpoints),
            "BLOB_URI": shard_audit.blob_url(),
        }
        cmd = [shard_audit.PERSISTCLI, "inspect", "state", "--shard-id", shard_id]
        try:
            proc = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True,
                timeout=PERSISTCLI_TIMEOUT_S,
                check=False,
            )
        except (subprocess.TimeoutExpired, OSError) as e:
            log(f"inspect state {shard_id}: {e}")
            return None
        if proc.returncode != 0:
            log(
                f"inspect state {shard_id}: exit {proc.returncode}:"
                f" {proc.stderr[-STDERR_TAIL_CHARS:]}"
            )
            return None
        start = proc.stdout.find("{")
        try:
            report = json.loads(proc.stdout[start:]) if start >= 0 else None
        except json.JSONDecodeError:
            report = None
        if not isinstance(report, dict) or report.get("shard_id") != shard_id:
            log(f"inspect state {shard_id}: unparsable output")
            return None
        return report

    def leader_version(self) -> tuple[int, int, int] | None:
        try:
            with sql.connection(
                self.host, connect_timeout=5, statement_timeout_ms=5_000
            ) as conn:
                row = conn.execute("SELECT mz_version()").fetchone()
        except (psycopg.Error, OSError) as e:
            log(f"cannot read the leader version: {e}")
            return None
        return version_tuple(str(row[0])) if row else None

    def audit_persist(self) -> None:
        before = self.cr_point()
        if before is None or before.active is None:
            return
        if before.reason not in READ_ONLY_REASONS:
            return
        active = before.active
        if not any(p.generation > active for p in self.generation_pods().values()):
            return
        version = self.leader_version()
        live = shard_audit.live_shards(self.host)
        if not live:
            return
        picked = [
            s.shard_id for s in shard_audit.choose(live, USER_SHARDS, SYSTEM_SHARDS)
        ]
        txns = shard_audit.txns_shard(self.host)
        if txns is not None and rng.random() < 0.5:
            picked.append(txns)
        states = {}
        for shard_id in picked:
            report = self.inspect_state(shard_id)
            if report is not None:
                states[shard_id] = report
        after = self.cr_point()
        if after is None:
            return
        ceiling = promotable_ceiling(before, after)
        if ceiling is None:
            log(f"persist audit: CR moved from {before} to {after}, discarding")
            return
        audited_writers = 0
        for shard_id, report in states.items():
            mapped = live.get(shard_id)
            details = {
                "shard_id": shard_id,
                "object_id": mapped.object_id if mapped else None,
                "object_name": mapped.name if mapped else "txns",
                "cr_before": [before.active, before.reason],
                "cr_after": [after.active, after.reason],
                "ceiling": ceiling,
                "seqno": report.get("seqno"),
                "applier_version": report.get("applier_version"),
            }
            audited_writers += len(report.get("writers") or {})
            foreign = foreign_handles(report, ceiling)
            always_or_unreachable(
                not foreign,
                "read-only isolation: no writer or critical reader of a leader shard comes from a never-promoted generation",
                {**details, "foreign": foreign},
            )
            # During Promoting the Service may already route to the candidate.
            if version is not None and ceiling == before.active:
                always_or_unreachable(
                    not data_version_exceeds(report, version),
                    "read-only isolation: no leader shard's persist data version exceeds the leader's build before promotion",
                    {**details, "leader_version": list(version)},
                )
        sometimes(
            audited_writers > 0,
            "read-only isolation: audited leader shards with writers while a read-only generation was live",
            {"shards": len(states), "writers": audited_writers},
        )

    def ensure_probe_table(self) -> bool:
        if kv_get(self.db, "probe_table"):
            return True
        try:
            with sql.connection(self.host, statement_timeout_ms=30_000) as conn:
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {PROBE_TABLE} (generation int, n bigint)".encode()
                )
        except (psycopg.Error, OSError) as e:
            log(f"cannot create the probe table: {sql.classify(e).template}")
            return False
        with self.db:
            kv_set(self.db, "probe_table", True)
        return True

    def probe_writes(self) -> None:
        if not self.ensure_probe_table():
            return
        before = self.cr_point()
        if before is None or before.active is None:
            return
        candidates = [
            p
            for p in self.kube.envd_pods()
            if p.generation > before.active and p.ip is not None
        ]
        if not candidates:
            return
        pod = rng.choice(candidates)
        assert pod.ip is not None
        n = rng.getrandbits(62)
        ddl = rng.random() < 0.5
        statement = (
            f"CREATE TABLE materialize.public.read_only_probe_ddl_{n} (a int)"
            if ddl
            else f"INSERT INTO {PROBE_TABLE} VALUES ({pod.generation}, {n})"
        )
        acked = False
        outcome: sql.Classified | None = None
        try:
            with sql.connection(
                pod.ip, connect_timeout=5, statement_timeout_ms=PROBE_TIMEOUT_MS
            ) as conn:
                conn.execute(statement.encode())
            acked = True
        except (psycopg.Error, OSError) as e:
            outcome = sql.classify(e)
        later = {p.uid: p for p in self.kube.envd_pods()}
        after = self.cr_point()
        if ddl and acked:
            self.drop_probe_ddl(n)
        if after is None:
            return
        ceiling = promotable_ceiling(before, after)
        same = later.get(pod.uid)
        if ceiling is None or same is None or same.incarnation() != pod.incarnation():
            log(f"direct write probe: bracket broken ({before} -> {after}), discarding")
            return
        details = {
            "pod": pod.name,
            "generation": pod.generation,
            "statement": "create_table" if ddl else "insert",
            "cr_before": [before.active, before.reason],
            "cr_after": [after.active, after.reason],
            "ceiling": ceiling,
            "sqlstate": outcome.sqlstate if outcome else None,
            "template": outcome.template if outcome else None,
        }
        always_or_unreachable(
            not (acked and pod.generation > ceiling),
            "read-only isolation: a never-promoted environmentd generation acknowledges no write sent directly to it",
            details,
        )
        sometimes(
            outcome is not None and outcome.sqlstate == "25006",
            "read-only isolation: a write sent directly to a read-only generation was rejected as read-only",
            details,
        )

    def drop_probe_ddl(self, n: int) -> None:
        try:
            with sql.connection(self.host, statement_timeout_ms=30_000) as conn:
                conn.execute(
                    f"DROP TABLE IF EXISTS materialize.public.read_only_probe_ddl_{n}".encode()
                )
        except (psycopg.Error, OSError) as e:
            log(f"cannot drop probe table {n}: {sql.classify(e).template}")

    def walsenders(self) -> list[Walsender] | None:
        try:
            with psycopg.connect(
                self.endpoints.upstream_postgres_url,
                connect_timeout=PG_TIMEOUT_S,
                autocommit=True,
            ) as conn:
                conn.execute(f"SET statement_timeout = '{PG_TIMEOUT_S}s'".encode())
                rows = conn.execute(WALSENDER_SQL.encode()).fetchall()
        except (psycopg.Error, OSError) as e:
            log(f"cannot list walsenders: {e}")
            return None
        return [Walsender(int(r[0]), str(r[1]), r[2], r[3]) for r in rows]

    def check_walsenders(
        self,
        before: CrPoint,
        pods_first: dict[str, PodRef],
        first: list[Walsender],
    ) -> None:
        second = self.walsenders()
        if second is None:
            return
        pods_second = self.generation_pods()
        after = self.cr_point()
        if after is None:
            return
        ceiling = promotable_ceiling(before, after)
        if ceiling is None:
            log(f"walsender check: CR moved from {before} to {after}, discarding")
            return
        found = foreign_walsenders(first, second, pods_first, pods_second, ceiling)
        candidate = any(p.generation > ceiling for p in pods_second.values())
        details = {
            "cr_before": [before.active, before.reason],
            "cr_after": [after.active, after.reason],
            "ceiling": ceiling,
            "walsenders": len(second),
            "foreign": found,
        }
        always_or_unreachable(
            not found,
            "read-only isolation: no replication connection to the upstream Postgres comes from a never-promoted generation",
            details,
        )
        sometimes(
            candidate and bool(first) and bool(second),
            "read-only isolation: a Postgres replication stream was live while a read-only generation was live",
            details,
        )

    def start_walsenders(
        self,
    ) -> tuple[CrPoint, dict[str, PodRef], list[Walsender]] | None:
        # Pods are listed before the first sample, so the gap to the second
        # sample bounds how long each listed pod has held its IP.
        before = self.cr_point()
        if before is None:
            return None
        pods = self.generation_pods()
        first = self.walsenders()
        return None if first is None else (before, pods, first)

    def run(self) -> None:
        start = time.monotonic()
        walsenders = None
        try:
            walsenders = self.start_walsenders()
        except ERRORS as e:
            log(f"walsender check not started: {e}")
        step: Callable[[], None]
        for step in rng.sample([self.probe_writes, self.audit_persist], 2):
            try:
                step()
            except ERRORS as e:
                log(f"{step.__name__} skipped: {e}")
        while True:
            try:
                self.oracle_sample()
            except ERRORS as e:
                log(f"oracle sample skipped: {e}")
            if time.monotonic() - start >= RUN_S:
                break
            time.sleep(ORACLE_INTERVAL_S)
        if walsenders is not None and time.monotonic() - start >= WALSENDER_GAP_S:
            try:
                self.check_walsenders(*walsenders)
            except ERRORS as e:
                log(f"walsender check skipped: {e}")


def anytime_main() -> int:
    try:
        checker = Checker()
    except ERRORS as e:
        log(f"cannot locate the environment: {e}")
        return 0
    checker.run()
    return 0

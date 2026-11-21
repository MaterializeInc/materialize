# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""A read-only generation stays isolated until it is promoted. Kubernetes only.

During a rollout the candidate generation runs read-only next to the leader.
Attribution and the bracket that tells a never-promoted generation apart are
in `materialize.antithesis.isolation`: every check that blames a generation
reads the CR before and after its observations, and gives up when the two
reads do not bracket one active generation.

* Direct writes: an `INSERT` or a `CREATE TABLE` sent to the candidate
  environmentd's pod IP must not be acknowledged. The pod's incarnation is
  read before and after, so the IP cannot have moved to another pod.
* Upstream: replication connections to the upstream Postgres are attributed
  to pods by client IP, in two samples at least `WALSENDER_GAP_SECONDS`
  apart. Postgres ingestions do not run in read-only mode, so none may come
  from a never-promoted generation.
* Timestamp oracle: per timeline, `read_ts` and `write_ts` in the metadata
  Postgres never go below a value committed before the sample started, and
  `read_ts <= write_ts`. Whether a read-only generation writes the oracle is
  not observable here, since the leader advances it all the time.

The persist handle audit of the legacy driver is not here: it needs
`persistcli` in the driver image. The assertion messages are those of the
legacy read-only isolation driver.
"""

from __future__ import annotations

import time
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always_or_unreachable,
    sometimes,
)

from materialize.antithesis import sql
from materialize.antithesis.endpoints import SQL_PORT
from materialize.antithesis.isolation import (
    CrPoint,
    OracleMark,
    PodRef,
    Walsender,
    advance_marks,
    foreign_walsenders,
    oracle_regressions,
    pod_generation,
    promotable_ceiling,
)
from materialize.antithesis_driver.client import Op, Status, Worker
from materialize.antithesis_driver.context import RULE_WAIT_SECONDS, Context, log
from materialize.antithesis_driver.disrupt import Kube, _is_api_error
from materialize.antithesis_driver.draw import Draw

SCHEMA = "read_only"
PROBE_TABLE = f"{SCHEMA}.probe"
READ_ONLY_REASONS = ("Applying", "ReadyToPromote")
"""CR reasons under which the candidate, if any, has not been told to promote."""
WALSENDER_GAP_SECONDS = 75.0
"""Postgres' default `wal_sender_timeout` is 60 s, and the upstream does not
override it. Within it a connection from a pod that died is reset once the
IP's new owner answers a keepalive."""
WALSENDER_STALE_SECONDS = 600.0
PG_TIMEOUT_SECONDS = 10
PROBE_TIMEOUT_MS = 10_000
ORACLE_SQL = (
    "SELECT timeline, read_ts::text, write_ts::text FROM tsoracle.timestamp_oracle"
)
WALSENDER_SQL = """
SELECT a.pid, a.backend_start::text, host(a.client_addr), s.slot_name
FROM pg_stat_activity a
LEFT JOIN pg_replication_slots s ON s.active_pid = a.pid
WHERE a.backend_type = 'walsender'
"""


def setup_statements() -> list[str]:
    return [
        f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
        f"CREATE TABLE IF NOT EXISTS {PROBE_TABLE} (generation int, n bigint)",
    ]


def cr_point(cr: dict[str, Any] | None) -> CrPoint | None:
    if cr is None:
        return None
    status = cr.get("status") or {}
    condition = next(
        (c for c in status.get("conditions") or [] if c.get("type") == "UpToDate"),
        {},
    )
    return CrPoint(status.get("activeGeneration"), condition.get("reason"))


class ReadOnly:
    def __init__(self, ctx: Context, kube: Kube) -> None:
        self.ctx = ctx
        self.kube = kube
        self.worker = Worker("read-only")
        self._first: (
            tuple[float, CrPoint, dict[str, PodRef], list[Walsender]] | None
        ) = None
        self._marks: dict[tuple[str, str], OracleMark] = {}
        self._oracle_active: int | None = None
        self._candidate_live = False

    def _guarded(self, name: str, action: Any) -> Status:
        def run() -> None:
            if self.ctx.quiesced.is_set():
                return
            try:
                action()
            except Exception as e:
                # The Kubernetes API, the upstream and the metadata store are
                # all behind faults. A check that cannot observe judges nothing.
                if not (_is_api_error(e) or isinstance(e, psycopg.Error)):
                    raise
                log(f"{name} skipped: {type(e).__name__}: {e}")

        return self.worker.run(run, RULE_WAIT_SECONDS)

    def _cr_point(self) -> CrPoint | None:
        return cr_point(self.kube.env.materialize.get())

    def _pods_by_ip(self) -> dict[str, PodRef]:
        """Every environmentd and clusterd pod with an IP, keyed by IP."""
        from materialize.orchestratord import K8S_TIMEOUT_SECONDS

        pods = self.kube.core.list_namespaced_pod(
            self.kube.env.endpoints.namespace,
            _request_timeout=K8S_TIMEOUT_SECONDS,  # pyright: ignore[reportCallIssue]
        ).items
        out = {}
        for pod in pods:
            metadata, status = pod.metadata, pod.status
            if metadata is None or status is None or not status.pod_ip:
                continue
            name = metadata.name or ""
            generation = pod_generation(name)
            if generation is not None and metadata.uid is not None:
                out[status.pod_ip] = PodRef(name, metadata.uid, generation)
        return out

    def probe_write(self, d: Draw) -> Status:
        """Send a write straight to a candidate environmentd."""
        n = d.integer(0, 2**62)
        ddl = d.boolean(0.5)
        pick = d.integer(0, 3)
        cleanup = d.integer(0, len(self.ctx.sessions.sessions) - 1)

        def action() -> None:
            materialize = self.kube.env.materialize
            before = self._cr_point()
            if before is None or before.active is None:
                return
            candidates = [
                p
                for p in materialize.environmentd_pods()
                if p.generation > before.active and p.ip is not None
            ]
            if not candidates:
                return
            pod = candidates[pick % len(candidates)]
            assert pod.ip is not None
            table = f"{SCHEMA}.ddl_{n}"
            statement = (
                f"CREATE TABLE {table} (a int)"
                if ddl
                else f"INSERT INTO {PROBE_TABLE} VALUES ({pod.generation}, {n})"
            )
            acked = False
            outcome: sql.Classified | None = None
            try:
                with psycopg.connect(
                    host=pod.ip,
                    port=SQL_PORT,
                    user="materialize",
                    dbname="materialize",
                    autocommit=True,
                    connect_timeout=5,
                    options=f"-c statement_timeout={PROBE_TIMEOUT_MS}",
                ) as conn:
                    conn.execute(statement.encode())
                acked = True
            except (psycopg.Error, OSError) as e:
                outcome = sql.classify(e)
            later = {p.uid: p for p in materialize.environmentd_pods()}
            after = self._cr_point()
            if ddl and acked:
                drop = f"DROP TABLE IF EXISTS {table}".encode()
                self.ctx.sessions.call(
                    cleanup,
                    Op("drop probe table", lambda c: c.execute(drop), lambda o: None),
                    0,
                )
            if after is None:
                return
            ceiling = promotable_ceiling(before, after)
            same = later.get(pod.uid)
            if (
                ceiling is None
                or same is None
                or (same.restart_count, same.ip) != (pod.restart_count, pod.ip)
            ):
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

        return self._guarded("direct write probe", action)

    def _walsenders(self) -> list[Walsender]:
        url = self.ctx.config.upstream_postgres_url
        assert url is not None
        with psycopg.connect(
            url, connect_timeout=PG_TIMEOUT_SECONDS, autocommit=True
        ) as conn:
            conn.execute(f"SET statement_timeout = '{PG_TIMEOUT_SECONDS}s'".encode())
            rows = conn.execute(WALSENDER_SQL.encode()).fetchall()
        return [Walsender(int(r[0]), str(r[1]), r[2], r[3]) for r in rows]

    def walsenders(self, d: Draw) -> Status:
        """Take the first sample of the upstream's replication connections, or
        the second one once the first is old enough, and judge the pair."""
        if self.ctx.config.upstream_postgres_url is None:
            return Status.NOT_SENT

        def action() -> None:
            first = self._first
            now = time.monotonic()
            if first is None or now - first[0] > WALSENDER_STALE_SECONDS:
                # Pods are listed before the sample, so the gap to the second
                # sample bounds how long each listed pod has held its IP.
                before = self._cr_point()
                if before is not None:
                    pods = self._pods_by_ip()
                    self._first = (now, before, pods, self._walsenders())
                return
            taken, before, pods_first, senders_first = first
            if now - taken < WALSENDER_GAP_SECONDS:
                return
            self._first = None
            senders_second = self._walsenders()
            pods_second = self._pods_by_ip()
            after = self._cr_point()
            if after is None:
                return
            ceiling = promotable_ceiling(before, after)
            if ceiling is None:
                return
            found = foreign_walsenders(
                senders_first, senders_second, pods_first, pods_second, ceiling
            )
            details = {
                "cr_before": [before.active, before.reason],
                "cr_after": [after.active, after.reason],
                "ceiling": ceiling,
                "walsenders": len(senders_second),
                "foreign": found,
            }
            always_or_unreachable(
                not found,
                "read-only isolation: no replication connection to the upstream Postgres comes from a never-promoted generation",
                details,
            )
            sometimes(
                any(p.generation > ceiling for p in pods_second.values())
                and bool(senders_first)
                and bool(senders_second),
                "read-only isolation: a Postgres replication stream was live while a read-only generation was live",
                details,
            )

        return self._guarded("walsender check", action)

    def oracle(self, d: Draw) -> Status:
        """Sample the timestamp oracle in the metadata Postgres."""

        def action() -> None:
            started = time.monotonic()
            with psycopg.connect(
                self.kube.env.endpoints.metadata_postgres_url,
                connect_timeout=PG_TIMEOUT_SECONDS,
                autocommit=True,
            ) as conn:
                conn.execute(
                    f"SET statement_timeout = '{PG_TIMEOUT_SECONDS}s'".encode()
                )
                raw = conn.execute(ORACLE_SQL.encode()).fetchall()
            finished = time.monotonic()
            rows = [(str(t), int(r), int(w)) for t, r, w in raw]
            point = self._cr_point()
            active = point.active if point else None
            if point is not None:
                self._candidate_live = point.reason in READ_ONLY_REASONS
            marks = self._marks
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
                advanced and self._candidate_live,
                "timestamp oracle advanced while a read-only generation was live",
                {"rows": [list(r) for r in rows], "active": active},
            )
            previous = self._oracle_active
            sometimes(
                bool(marks)
                and previous is not None
                and active is not None
                and active > previous
                and not regressions,
                "timestamp oracle check spanned a promotion",
                {"previous_active": previous, "active": active},
            )
            self._marks = advance_marks(marks, rows, finished)
            if active is not None:
                self._oracle_active = active

        return self._guarded("oracle sample", action)

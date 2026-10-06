# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Coordinator canaries: a fixed probe set that must be answered within a deadline.

Property: `coordinator-canaries-complete-under-load`.

`anytime_canaries` asks Antithesis for a quiet period while every other command
keeps running, waits until environmentd serves SQL again, then runs the probes.
A starvation bug that exists only while load continues is visible only this
way. `eventually_canaries` runs the same probes after faults stopped and the
load was killed.

Quiet time is fault-free time for every other property, so an invocation runs
only with a probability drawn once per timeline, skips if another workload
quiet period is active, and holds quiet in short renewed chunks
(`quiet.held`) so faults resume soon after the probes finish.

Each bound comes in two parts. An `always` with a generous wedge bound means
"no answer at all", and a `sometimes` with the tight deadline keeps the latency
signal visible without failing on a slow schedule of one simulated core.

Each probe runs in a daemon thread on fresh connections, so the client-side
deadline holds even when the server never answers. An error is an answer: only
"no answer within the wedge bound" fails the liveness assertion. A
connection-class error is retried within the same bound, because a wedged
coordinator that gets its pod restarted by a liveness probe would otherwise
look like an answer.
"""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    always_or_unreachable,
    reachable,
    sometimes,
    unreachable,
)

from materialize.antithesis import quiet, sql, state
from materialize.antithesis.drivers import counters, history
from materialize.antithesis.drivers.sources_common import timeline_choice
from materialize.antithesis.environment import Environment
from materialize.antithesis.rng import rng

STATE_DB = "canaries"

# Calibration: every duration below is a first guess. Each needs a fault-free
# latency baseline on one simulated core (deadline at a multiple of p99) and a
# measurement of environmentd boot time after a quiet period restores pods.
PROBE_DEADLINE_S = 45.0
"""Tight per-probe deadline, checked with `sometimes`."""
PROBE_WEDGE_S = 180.0
"""Generous per-probe bound, checked with `always`."""
STATEMENT_TIMEOUT_MS = 20_000
"""Well below the probe deadline, so a statement timeout is an answer."""
RTR_TIMEOUT_S = 30
RECOVERY_BUDGET_S = 150.0
"""Tight bound on environmentd serving SQL after a quiet period begins."""
RECOVERY_WEDGE_S = 600.0
"""Generous bound: CrashLoopBackOff waits up to 300 s, plus one boot."""
EVENTUALLY_RECOVERY_BUDGET_S = 300.0
EVENTUALLY_RECOVERY_WEDGE_S = 900.0
SETTLE_S = 15.0
"""Extra wait after SQL is back, for clusterd pods the quiet period restored."""
START_JITTER_S = (0.0, 60.0)
QUIET_CHUNK_S = 60
RUN_P_MENU = (0.1, 0.3, 1.0)
"""Per-timeline chance that an `anytime_canaries` invocation probes at all."""
CANARY_CLUSTER = "quickstart"

PROBE_TABLE = "canaries_probe"
PROBE_MV = "canaries_mv"
DDL_PREFIX = "canaries_ddl_"

PROBES = ("P1", "P2", "P3", "P4", "P5", "P6", "P7")


def log(message: str) -> None:
    print(f"canaries: {message}", flush=True)


class Skip(Exception):
    """The probe's precondition does not hold in this environment."""


@dataclass
class ProbeResult:
    name: str
    status: str
    """'ok', 'error' (answered with an error), 'unanswered', or 'skipped'."""
    accepted: bool
    """Whether any connection was established within the wedge bound."""
    elapsed_s: float
    accepted_s: float | None = None
    """Seconds until the first connection was established."""
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def answered(self) -> bool:
        return self.status in ("ok", "error")

    @property
    def answered_in_time(self) -> bool:
        return self.answered and self.elapsed_s <= _deadline(self.name)

    @property
    def accepted_in_time(self) -> bool:
        return self.accepted_s is not None and self.accepted_s <= PROBE_DEADLINE_S


def _deadline(name: str) -> float:
    return PROBE_DEADLINE_S + (RTR_TIMEOUT_S if name == "P7" else 0)


def _wedge_bound(name: str) -> float:
    return PROBE_WEDGE_S + (RTR_TIMEOUT_S if name == "P7" else 0)


def _p1(conn: psycopg.Connection) -> dict[str, Any]:
    conn.execute("SET transaction_isolation = 'serializable'")
    conn.execute("SELECT 1").fetchone()
    return {}


def _p2(conn: psycopg.Connection) -> dict[str, Any]:
    conn.execute("SET transaction_isolation = 'strict serializable'")
    row = conn.execute("SELECT mz_now()::text").fetchone()
    return {"mz_now": row[0] if row else None}


def _p3(conn: psycopg.Connection) -> dict[str, Any]:
    row = conn.execute("SELECT count(*) FROM mz_catalog.mz_tables").fetchone()
    return {"tables": row[0] if row else None}


def _p4(conn: psycopg.Connection) -> dict[str, Any]:
    conn.execute("SET transaction_isolation = 'strict serializable'")
    marker = str(uuid.UUID(int=rng.getrandbits(128)))
    conn.execute(f"INSERT INTO {PROBE_TABLE} (id) VALUES (%s)", (marker,))
    row = conn.execute(
        f"SELECT count(*) FROM {PROBE_TABLE} WHERE id = %s", (marker,)
    ).fetchone()
    seen = int(row[0]) if row else 0
    always(
        seen == 1,
        "coordinator canary P4 read observes its own acknowledged insert exactly once",
        {"marker": marker, "seen": seen},
    )
    return {"marker": marker}


def _p5(conn: psycopg.Connection) -> dict[str, Any]:
    name = f"{DDL_PREFIX}{rng.getrandbits(48):012x}"
    conn.execute(f"CREATE TABLE {name} (a int)".encode())
    conn.execute(f"DROP TABLE {name}".encode())
    return {"table": name}


def _p6(conn: psycopg.Connection) -> dict[str, Any]:
    row = conn.execute(
        "SELECT"
        " (SELECT count(*) FROM mz_catalog.mz_cluster_replicas r"
        "  JOIN mz_catalog.mz_clusters c ON r.cluster_id = c.id WHERE c.name = %s),"
        " (SELECT count(*) FROM mz_catalog.mz_materialized_views WHERE name = %s)",
        (CANARY_CLUSTER, PROBE_MV),
    ).fetchone()
    if row is None or int(row[0]) == 0 or int(row[1]) == 0:
        raise Skip(f"no replica or no {PROBE_MV}: {row}")
    reachable(
        "coordinator canary: replica peek probe ran against a cluster with a replica",
        {},
    )
    conn.execute(f"SET cluster = {CANARY_CLUSTER}")
    conn.execute(f"SELECT * FROM {PROBE_MV}").fetchall()
    return {}


def _p7(conn: psycopg.Connection) -> dict[str, Any]:
    targets = conn.execute(
        "SELECT d.name, s.name, t.name FROM mz_catalog.mz_tables t"
        " JOIN mz_catalog.mz_sources src ON t.source_id = src.id"
        " JOIN mz_catalog.mz_schemas s ON t.schema_id = s.id"
        " JOIN mz_catalog.mz_databases d ON s.database_id = d.id"
        " WHERE src.type IN ('kafka', 'postgres', 'mysql')"
    ).fetchall()
    replicas = conn.execute(
        "SELECT count(*) FROM mz_catalog.mz_cluster_replicas r"
        " JOIN mz_catalog.mz_clusters c ON r.cluster_id = c.id WHERE c.name = %s",
        (CANARY_CLUSTER,),
    ).fetchone()
    if not targets or replicas is None or int(replicas[0]) == 0:
        raise Skip("no table from an upstream source, or no replica")
    database, schema, table = rng.choice(targets)
    reachable(
        "coordinator canary: real-time recency probe ran against an upstream source",
        {},
    )
    conn.execute(f"SET cluster = {CANARY_CLUSTER}")
    conn.execute("SET transaction_isolation = 'strict serializable'")
    conn.execute("SET real_time_recency = true")
    conn.execute(f"SET real_time_recency_timeout = '{RTR_TIMEOUT_S}s'".encode())
    qualified = ".".join(f'"{p}"' for p in (database, schema, table))
    conn.execute(f"SELECT count(*) FROM {qualified}".encode()).fetchone()
    return {"table": qualified}


PROBE_FNS: dict[str, Callable[[psycopg.Connection], dict[str, Any]]] = {
    "P1": _p1,
    "P2": _p2,
    "P3": _p3,
    "P4": _p4,
    "P5": _p5,
    "P6": _p6,
    "P7": _p7,
}


def _attempt(
    host: str, name: str, start: float, end: float, result: ProbeResult
) -> None:
    """Run probe `name` until it is answered or `end` passes, filling `result`.

    `result.details` is only ever replaced, never mutated, so the caller can
    copy it while this thread is still running.
    """
    fn = PROBE_FNS[name]
    details: dict[str, Any] = {}
    while True:
        remaining = end - time.monotonic()
        if remaining <= 0:
            result.status = "unanswered"
            return
        try:
            conn = sql.connect(
                host,
                connect_timeout=max(2, min(10, int(remaining))),
                statement_timeout_ms=STATEMENT_TIMEOUT_MS,
            )
        except Exception as e:
            details["last_connect_error"] = str(e)[:200]
            result.details = dict(details)
            time.sleep(min(1.0, max(0.0, end - time.monotonic())))
            continue
        if not result.accepted:
            result.accepted_s = time.monotonic() - start
        result.accepted = True
        try:
            details.update(fn(conn))
            result.details = dict(details)
            result.status = "ok"
            return
        except Skip as e:
            details["reason"] = str(e)
            result.details = dict(details)
            result.status = "skipped"
            return
        except Exception as e:
            c = sql.classify(e)
            details.update({"sqlstate": c.sqlstate, "template": c.template})
            result.details = dict(details)
            if c.outcome == sql.Outcome.VIOLATION:
                _report_violation(name, dict(details))
            if c.outcome != sql.Outcome.INDETERMINATE:
                result.status = "error"
                return
        finally:
            try:
                conn.close()
            except Exception:
                pass
        time.sleep(min(1.0, max(0.0, end - time.monotonic())))


def run_probe(host: str, name: str) -> ProbeResult:
    bound = _wedge_bound(name)
    result = ProbeResult(name=name, status="unanswered", accepted=False, elapsed_s=0.0)
    start = time.monotonic()
    worker = threading.Thread(
        target=_attempt,
        args=(host, name, start, start + bound, result),
        name=f"canary-{name}",
        daemon=True,
    )
    worker.start()
    # A hung statement keeps the thread alive past the bound; it is
    # abandoned, and the daemon flag lets the process exit without it.
    worker.join(bound + 1.0)
    elapsed = time.monotonic() - start
    # Snapshot, since an abandoned worker may still write to `result`.
    snapshot = ProbeResult(
        name=name,
        status="unanswered" if worker.is_alive() else result.status,
        accepted=result.accepted,
        elapsed_s=elapsed,
        accepted_s=result.accepted_s,
        details={
            **result.details,
            "deadline_s": _deadline(name),
            "wedge_bound_s": bound,
            "elapsed_s": elapsed,
            "accepted_s": result.accepted_s,
        },
    )
    return snapshot


def _bounded(fn: Callable[[], bool], seconds: float) -> bool:
    """Run `fn` in a daemon thread. False if it raised, returned False, or ran past `seconds`."""
    box: list[bool] = []

    def target() -> None:
        try:
            box.append(fn())
        except Exception as e:
            log(f"bounded call failed: {e}")

    worker = threading.Thread(target=target, daemon=True)
    worker.start()
    worker.join(seconds)
    return bool(box) and box[0]


def _report_violation(name: str, details: dict[str, Any]) -> None:
    if name == "P1":
        unreachable("coordinator canary P1 returns only classified errors", details)
    elif name == "P2":
        unreachable("coordinator canary P2 returns only classified errors", details)
    elif name == "P3":
        unreachable("coordinator canary P3 returns only classified errors", details)
    elif name == "P4":
        unreachable("coordinator canary P4 returns only classified errors", details)
    elif name == "P5":
        unreachable("coordinator canary P5 returns only classified errors", details)
    elif name == "P6":
        unreachable("coordinator canary P6 returns only classified errors", details)
    else:
        unreachable("coordinator canary P7 returns only classified errors", details)


def _assert_under_load(r: ProbeResult) -> None:
    d = {"status": r.status, **r.details}
    if r.name == "P1":
        always(
            r.answered,
            "coordinator canary P1 (serializable SELECT 1) answered within the wedge bound during load",
            d,
        )
        sometimes(
            r.answered_in_time,
            "coordinator canary P1 (serializable SELECT 1) answered within the tight deadline during load",
            d,
        )
    elif r.name == "P2":
        always(
            r.answered,
            "coordinator canary P2 (strict serializable mz_now) answered within the wedge bound during load",
            d,
        )
        sometimes(
            r.answered_in_time,
            "coordinator canary P2 (strict serializable mz_now) answered within the tight deadline during load",
            d,
        )
    elif r.name == "P3":
        always(
            r.answered,
            "coordinator canary P3 (catalog server peek) answered within the wedge bound during load",
            d,
        )
        sometimes(
            r.answered_in_time,
            "coordinator canary P3 (catalog server peek) answered within the tight deadline during load",
            d,
        )
    elif r.name == "P4":
        always(
            r.answered,
            "coordinator canary P4 (insert then read own write) answered within the wedge bound during load",
            d,
        )
        sometimes(
            r.answered_in_time,
            "coordinator canary P4 (insert then read own write) answered within the tight deadline during load",
            d,
        )
    elif r.name == "P5":
        always(
            r.answered,
            "coordinator canary P5 (CREATE and DROP TABLE) answered within the wedge bound during load",
            d,
        )
        sometimes(
            r.answered_in_time,
            "coordinator canary P5 (CREATE and DROP TABLE) answered within the tight deadline during load",
            d,
        )
    elif r.name == "P6":
        always_or_unreachable(
            r.answered,
            "coordinator canary P6 (replica peek of a materialized view) answered within the wedge bound during load",
            d,
        )
        sometimes(
            r.answered_in_time,
            "coordinator canary P6 (replica peek of a materialized view) answered within the tight deadline during load",
            d,
        )
    else:
        always_or_unreachable(
            r.answered,
            "coordinator canary P7 (real-time recency read) answered within the wedge bound during load",
            d,
        )
        sometimes(
            r.answered_in_time,
            "coordinator canary P7 (real-time recency read) answered within the tight deadline during load",
            d,
        )


def _assert_after_faults(r: ProbeResult) -> None:
    d = {"status": r.status, **r.details}
    if r.name == "P1":
        always(
            r.answered,
            "coordinator canary P1 (serializable SELECT 1) answered within the wedge bound after faults stopped",
            d,
        )
        sometimes(
            r.answered_in_time,
            "coordinator canary P1 (serializable SELECT 1) answered within the tight deadline after faults stopped",
            d,
        )
    elif r.name == "P2":
        always(
            r.answered,
            "coordinator canary P2 (strict serializable mz_now) answered within the wedge bound after faults stopped",
            d,
        )
        sometimes(
            r.answered_in_time,
            "coordinator canary P2 (strict serializable mz_now) answered within the tight deadline after faults stopped",
            d,
        )
    elif r.name == "P3":
        always(
            r.answered,
            "coordinator canary P3 (catalog server peek) answered within the wedge bound after faults stopped",
            d,
        )
        sometimes(
            r.answered_in_time,
            "coordinator canary P3 (catalog server peek) answered within the tight deadline after faults stopped",
            d,
        )
    elif r.name == "P4":
        always(
            r.answered,
            "coordinator canary P4 (insert then read own write) answered within the wedge bound after faults stopped",
            d,
        )
        sometimes(
            r.answered_in_time,
            "coordinator canary P4 (insert then read own write) answered within the tight deadline after faults stopped",
            d,
        )
    elif r.name == "P5":
        always(
            r.answered,
            "coordinator canary P5 (CREATE and DROP TABLE) answered within the wedge bound after faults stopped",
            d,
        )
        sometimes(
            r.answered_in_time,
            "coordinator canary P5 (CREATE and DROP TABLE) answered within the tight deadline after faults stopped",
            d,
        )
    elif r.name == "P6":
        always_or_unreachable(
            r.answered,
            "coordinator canary P6 (replica peek of a materialized view) answered within the wedge bound after faults stopped",
            d,
        )
        sometimes(
            r.answered_in_time,
            "coordinator canary P6 (replica peek of a materialized view) answered within the tight deadline after faults stopped",
            d,
        )
    else:
        always_or_unreachable(
            r.answered,
            "coordinator canary P7 (real-time recency read) answered within the wedge bound after faults stopped",
            d,
        )
        sometimes(
            r.answered_in_time,
            "coordinator canary P7 (real-time recency read) answered within the tight deadline after faults stopped",
            d,
        )


def _wait_for_sql(host: str, budget_s: float) -> float | None:
    """Poll `SELECT 1` until it succeeds or the budget runs out.

    Returns the seconds it took, or None if the budget ran out.
    """

    def select_one() -> bool:
        with sql.connection(
            host, connect_timeout=5, statement_timeout_ms=10_000
        ) as conn:
            conn.execute("SELECT 1").fetchone()
            return True

    start = time.monotonic()
    end = start + budget_s
    while (remaining := end - time.monotonic()) > 0:
        if _bounded(select_one, min(15.0, remaining)):
            return time.monotonic() - start
        time.sleep(min(2.0, max(0.0, end - time.monotonic())))
    return None


def _ensure_objects(host: str) -> None:
    """Create the probe table and view. Best effort: a probe whose object is missing answers with an error."""

    def create() -> bool:
        with sql.connection(host, statement_timeout_ms=STATEMENT_TIMEOUT_MS) as conn:
            conn.execute(f"CREATE TABLE IF NOT EXISTS {PROBE_TABLE} (id text)")
            conn.execute(
                f"CREATE MATERIALIZED VIEW IF NOT EXISTS {PROBE_MV}"
                f" IN CLUSTER {CANARY_CLUSTER}"
                f" AS SELECT count(*) AS n FROM {PROBE_TABLE}"
            )
        return True

    if not _bounded(create, PROBE_DEADLINE_S):
        log("creating probe objects did not complete")


def _run_probes(host: str) -> list[ProbeResult]:
    results = []
    for name in PROBES:
        r = run_probe(host, name)
        log(f"{name}: {r.status} in {r.elapsed_s:.1f}s {r.details}")
        results.append(r)
    return results


def _assert_accepted(results: list[ProbeResult]) -> None:
    for r in results:
        if r.status != "skipped":
            always(
                r.accepted,
                "coordinator canary: environmentd accepts a fresh connection within the probe wedge bound",
                {"probe": r.name, **r.details},
            )
            sometimes(
                r.accepted_in_time,
                "coordinator canary: environmentd accepted a fresh connection within the tight probe deadline",
                {"probe": r.name, **r.details},
            )


def _deployment_signature(env: Environment) -> dict[str, Any] | None:
    """environmentd incarnations and the CR rollout state, or None if the
    Kubernetes API cannot be read."""
    m = env.materialize
    try:
        pods = sorted([p.uid, p.restart_count] for p in m.environmentd_pods())
        status = m.status()
    except Exception as e:
        log(f"reading the deployment state failed: {e}")
        return None
    phase = next(
        (
            c.get("reason")
            for c in status.get("conditions") or []
            if c.get("type") == "UpToDate"
        ),
        None,
    )
    return {
        "pods": pods,
        "phase": phase,
        "active_generation": status.get("activeGeneration"),
    }


def _run_p() -> float:
    db = state.open_db(STATE_DB)
    try:
        return float(timeline_choice(db, "run_p", RUN_P_MENU))
    finally:
        db.close()


def anytime_canaries() -> int:
    run_p = _run_p()
    if rng.random() >= run_p:
        log(f"not probing this invocation (run_p={run_p})")
        return 0
    env = Environment()
    host = env.sql_host()
    time.sleep(rng.uniform(*START_JITTER_S))
    with quiet.held("canaries", QUIET_CHUNK_S, exclusive=True) as paused:
        if paused is None:
            log("another quiet period is active, skipping")
            return 0
        log(f"holding quiet in {QUIET_CHUNK_S}s chunks (paused={paused})")
        _probe_under_load(env, host, paused)
    return 0


def _probe_under_load(env: Environment, host: str, paused: bool) -> None:
    took = _wait_for_sql(host, RECOVERY_WEDGE_S)
    details = {
        "budget_s": RECOVERY_BUDGET_S,
        "wedge_bound_s": RECOVERY_WEDGE_S,
        "took_s": took,
        "faults_paused": paused,
    }
    always(
        took is not None,
        "coordinator canary: environmentd serves SQL within the wedge bound of a quiet period",
        details,
    )
    sometimes(
        took is not None and took <= RECOVERY_BUDGET_S,
        "coordinator canary: environmentd served SQL within the tight recovery budget of a quiet period",
        details,
    )
    if took is None:
        return
    time.sleep(SETTLE_S)
    _ensure_objects(host)
    load_before = history.recent_activity() + counters.recent_activity()
    deployment_before = _deployment_signature(env)
    results = _run_probes(host)
    deployment_after = _deployment_signature(env)
    load_after = history.recent_activity() + counters.recent_activity()
    # The rollout driver keeps running through the quiet period, and a
    # promotion or an environmentd restart legitimately leaves probes
    # unanswered for longer than the deadline.
    disrupted = (
        deployment_before is not None
        and deployment_after is not None
        and deployment_before != deployment_after
    )
    sometimes(
        disrupted,
        "coordinator canary: environmentd restarted or the rollout state changed during the probe window",
        {"before": deployment_before, "after": deployment_after},
    )
    if disrupted:
        log("deployment changed during the probes, skipping the deadline checks")
        return
    _assert_accepted(results)
    for r in results:
        if r.status != "skipped":
            _assert_under_load(r)
    sometimes(
        all(r.answered or r.status == "skipped" for r in results)
        and load_before > 0
        and load_after > 0,
        "coordinator canary: every probe answered while history or counter drivers kept writing",
        {"load_before": load_before, "load_after": load_after},
    )


def eventually_canaries() -> int:
    host = Environment().sql_host()
    took = _wait_for_sql(host, EVENTUALLY_RECOVERY_WEDGE_S)
    details = {
        "budget_s": EVENTUALLY_RECOVERY_BUDGET_S,
        "wedge_bound_s": EVENTUALLY_RECOVERY_WEDGE_S,
        "took_s": took,
    }
    always(
        took is not None,
        "coordinator canary: environmentd serves SQL within the wedge bound after faults stopped",
        details,
    )
    sometimes(
        took is not None and took <= EVENTUALLY_RECOVERY_BUDGET_S,
        "coordinator canary: environmentd served SQL within the tight recovery budget after faults stopped",
        details,
    )
    if took is None:
        return 0
    time.sleep(SETTLE_S)
    _ensure_objects(host)
    results = _run_probes(host)
    _assert_accepted(results)
    for r in results:
        if r.status != "skipped":
            _assert_after_faults(r)
    return 0

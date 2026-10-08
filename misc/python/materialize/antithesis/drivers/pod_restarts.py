# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Harness-driven pod restarts while the other drivers run.

`driver_main` (`parallel_driver_pod_restarts`) deletes one environmentd
(active or candidate), clusterd, or orchestratord pod per invocation, either
right away or once another driver's recorded state shows a moment worth
interrupting. It does nothing while a quiet period is active.

Every harness pod delete, from this driver and from the rollout driver, is
recorded in the `restarts` table of the `pod_restarts` database before it is
sent, so oracles can tell harness kills from unexplained restarts. Rows carry
`at` from `time.monotonic()` in the workload container and `outcome`
`attempted`, then `acked`, `rejected` (a 4xx: the pod was gone or replaced),
or `unknown`. An `attempted` or `unknown` delete may still have happened.
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Any

from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    reachable,
)
from kubernetes import client  # type: ignore
from kubernetes.client.rest import ApiException  # type: ignore

from materialize.antithesis import quiet
from materialize.antithesis.drivers import lifecycle, postgres_sources, recovery
from materialize.antithesis.drivers.rollouts import (
    GRACE_MENU,
    K8S_TIMEOUT_SECONDS,
    POLL_SECONDS,
    TRANSIENT_ERRORS,
    Kube,
    Snapshot,
    lifecycle_ddl_in_flight,
    peek_db,
)
from materialize.antithesis.endpoints import Endpoints
from materialize.antithesis.environment import Environment
from materialize.antithesis.rng import rng
from materialize.antithesis.state import open_db

RESTARTS_DB = "pod_restarts"

TARGETS = ("environmentd_active", "environmentd_candidate", "clusterd", "orchestratord")
TRIGGERS = (
    "now",
    "lifecycle_pending",
    "lifecycle_ddl",
    "postgres_terminal",
    "rollout_applying",
    "rollout_promoting",
)
# How long an invocation waits for its trigger before giving up without a
# kill. Needs calibration on one simulated core.
TRIGGER_WAIT_SECONDS = 120.0


def log(message: str) -> None:
    print(f"pod_restarts: {message}", flush=True)


def open_restarts(endpoints: Endpoints | None = None) -> sqlite3.Connection:
    db = open_db(RESTARTS_DB, endpoints)
    with db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS restarts ("
            " seq INTEGER PRIMARY KEY AUTOINCREMENT,"
            " at REAL NOT NULL,"
            " origin TEXT NOT NULL,"
            " role TEXT NOT NULL,"
            " namespace TEXT NOT NULL,"
            " pod TEXT NOT NULL,"
            " uid TEXT,"
            " grace INTEGER,"
            " reason TEXT NOT NULL,"
            " outcome TEXT NOT NULL)"
        )
        db.execute(
            "CREATE TABLE IF NOT EXISTS params (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
    return db


def delete_recorded(
    kube: Kube,
    *,
    origin: str,
    role: str,
    namespace: str,
    pod: str,
    uid: str | None,
    grace: int | None,
    reason: str,
) -> bool:
    """Delete a pod, recording the attempt first. Returns whether the API acknowledged it.

    `uid`, when given, is sent as a delete precondition, so a pod recreated
    under the same name since it was listed is left alone.
    """
    db = open_restarts(kube.env.endpoints)
    try:
        with db:
            seq = db.execute(
                "INSERT INTO restarts"
                " (at, origin, role, namespace, pod, uid, grace, reason, outcome)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'attempted')",
                (time.monotonic(), origin, role, namespace, pod, uid, grace, reason),
            ).lastrowid
        try:
            kube.core.delete_namespaced_pod(
                pod,
                namespace,
                grace_period_seconds=grace,
                body=client.V1DeleteOptions(
                    preconditions=client.V1Preconditions(uid=uid) if uid else None
                ),
                _request_timeout=K8S_TIMEOUT_SECONDS,  # pyright: ignore[reportCallIssue]
            )
            outcome = "acked"
        except ApiException as e:
            log(f"deleting pod {namespace}/{pod} failed: {e.status} {e.reason}")
            outcome = (
                "rejected"
                if e.status is not None and 400 <= e.status < 500
                else "unknown"
            )
        except TRANSIENT_ERRORS as e:
            log(f"deleting pod {namespace}/{pod} failed: {e}")
            outcome = "unknown"
        with db:
            db.execute("UPDATE restarts SET outcome = ? WHERE seq = ?", (outcome, seq))
        return outcome == "acked"
    finally:
        db.close()


def restarts_since(
    at: float, endpoints: Endpoints | None = None
) -> list[dict[str, Any]]:
    """Harness pod deletes recorded at or after monotonic time `at`, oldest first."""
    conn = peek_db(RESTARTS_DB, endpoints or Endpoints.from_env())
    if conn is None:
        return []
    try:
        conn.row_factory = sqlite3.Row
        return [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM restarts WHERE at >= ? ORDER BY seq", (at,)
            ).fetchall()
        ]
    except sqlite3.Error:
        return []
    finally:
        conn.close()


def quiet_period_active(endpoints: Endpoints) -> bool:
    """Whether a quiet period recorded by `materialize.antithesis.quiet` covers now."""
    del endpoints
    try:
        return quiet.active_quiet_period()
    except sqlite3.Error:
        return False


def _count(endpoints: Endpoints, name: str, query: str) -> int:
    conn = peek_db(name, endpoints)
    if conn is None:
        return 0
    try:
        row = conn.execute(query).fetchone()
        return int(row[0]) if row else 0
    except sqlite3.Error:
        return 0
    finally:
        conn.close()


def trigger_met(trigger: str, endpoints: Endpoints, snap: Snapshot | None) -> bool:
    """Whether `trigger` holds now. `snap` is a CR snapshot taken for this
    poll, needed only by the rollout triggers."""
    if trigger == "now":
        return True
    if trigger == "lifecycle_pending":
        return _count(endpoints, lifecycle.STATE_DB, "SELECT count(*) FROM pending") > 0
    if trigger == "lifecycle_ddl":
        return lifecycle_ddl_in_flight(endpoints) > 0
    if trigger == "postgres_terminal":
        return (
            _count(
                endpoints,
                postgres_sources.STATE_DB,
                postgres_sources.TERMINAL_EXPORTS_QUERY,
            )
            > 0
        )
    if snap is None:
        return False
    if trigger == "rollout_applying":
        return snap.reason == "Applying"
    assert trigger == "rollout_promoting", trigger
    return snap.reason == "Promoting"


@dataclass(frozen=True)
class Victim:
    role: str
    namespace: str
    pod: str
    uid: str | None


def pick_victim(kube: Kube, target: str) -> Victim | None:
    if target.startswith("environmentd_"):
        snap = kube.try_snapshot()
        if snap is None or snap.active is None:
            return None
        generation = snap.active if target == "environmentd_active" else snap.active + 1
        envd = [p for p in kube.envd_pods() if p.generation == generation]
        if not envd:
            return None
        chosen = rng.choice(envd)
        return Victim(target, kube.namespace, chosen.name, chosen.uid)
    namespace = kube.operator_namespace if target == "orchestratord" else kube.namespace
    candidates = []
    for pod in kube.core.list_namespaced_pod(
        namespace,
        _request_timeout=K8S_TIMEOUT_SECONDS,  # pyright: ignore[reportCallIssue]
    ).items:
        metadata = pod.metadata
        assert metadata is not None
        role = recovery.pod_role(
            namespace, kube.operator_namespace, metadata.labels or {}
        )
        if role == target and metadata.deletion_timestamp is None:
            assert metadata.name is not None
            candidates.append(Victim(target, namespace, metadata.name, metadata.uid))
    return rng.choice(candidates) if candidates else None


@dataclass(frozen=True)
class TimelineParams:
    """Swarm parameters, drawn once per timeline so timelines skew differently."""

    act_probability: float
    target_weights: dict[str, int]
    trigger_weights: dict[str, int]


def timeline_params(db: sqlite3.Connection) -> TimelineParams:
    row = db.execute("SELECT value FROM params WHERE key = 'timeline'").fetchone()
    if row is not None:
        raw = json.loads(row[0])
        return TimelineParams(
            raw["act_probability"], raw["target_weights"], raw["trigger_weights"]
        )
    while True:
        targets = {t: rng.choice([0, 1, 1, 4]) for t in TARGETS}
        if any(targets.values()):
            break
    triggers = {t: rng.choice([0, 1, 1, 4]) for t in TRIGGERS}
    triggers["now"] = max(triggers["now"], 1)
    # Zero leaves the timeline without harness restarts outside rollouts.
    params = TimelineParams(rng.choice([0.0, 0.3, 0.7, 1.0]), targets, triggers)
    with db:
        db.execute(
            "INSERT OR IGNORE INTO params (key, value) VALUES ('timeline', ?)",
            (
                json.dumps(
                    {
                        "act_probability": params.act_probability,
                        "target_weights": params.target_weights,
                        "trigger_weights": params.trigger_weights,
                    }
                ),
            ),
        )
    return timeline_params(db)


def wait_for_trigger(
    weights: dict[str, int], endpoints: Endpoints, kube: Kube
) -> str | None:
    """Wait until any trigger in `weights` holds and return one of those that
    hold, drawn by weight. None if none held within `TRIGGER_WAIT_SECONDS`.

    Each moment is short, and Test Composer often leaves the driver whose
    state a trigger reads unscheduled for the whole wait, so waiting on one
    trigger drawn up front rarely ends in a kill.
    """
    deadline = time.monotonic() + TRIGGER_WAIT_SECONDS
    needs_cr = any(t.startswith("rollout_") for t in weights)
    while True:
        snap = kube.try_snapshot() if needs_cr else None
        met = []
        for trigger in weights:
            try:
                if trigger_met(trigger, endpoints, snap):
                    met.append(trigger)
            except TRANSIENT_ERRORS as e:
                log(f"checking trigger {trigger} failed: {e}")
        if met:
            return rng.choices(met, weights=[weights[t] for t in met])[0]
        if time.monotonic() >= deadline:
            return None
        time.sleep(POLL_SECONDS)


def note_target(victim: Victim, details: dict[str, Any]) -> None:
    if victim.role == "environmentd_active":
        reachable("Pod restart driver deleted the active environmentd pod", details)
    elif victim.role == "environmentd_candidate":
        reachable("Pod restart driver deleted a candidate environmentd pod", details)
    elif victim.role == "clusterd":
        reachable("Pod restart driver deleted a clusterd pod", details)
    else:
        reachable("Pod restart driver deleted an orchestratord pod", details)


def note_trigger(trigger: str, details: dict[str, Any]) -> None:
    if trigger == "lifecycle_pending":
        reachable(
            "Pod restart driver deleted a pod while lifecycle had a pending multi-step op",
            details,
        )
    elif trigger == "lifecycle_ddl":
        reachable(
            "Pod restart driver deleted a pod while lifecycle DDL was in flight",
            details,
        )
    elif trigger == "postgres_terminal":
        reachable(
            "Pod restart driver deleted a pod while a Postgres export was terminal",
            details,
        )
    elif trigger == "rollout_applying":
        reachable(
            "Pod restart driver deleted a pod while a rollout was Applying", details
        )
    elif trigger == "rollout_promoting":
        reachable(
            "Pod restart driver deleted a pod while a rollout was Promoting", details
        )


def driver_main() -> int:
    env = Environment()
    endpoints = env.endpoints
    kube = Kube(env)
    db = open_restarts(endpoints)
    try:
        params = timeline_params(db)
    finally:
        db.close()
    if rng.random() >= params.act_probability:
        return 0
    if quiet_period_active(endpoints):
        log("quiet period active; not restarting anything")
        return 0
    # The draw only decides between an immediate and a triggered kill; a
    # triggered kill fires on whichever armed trigger holds first.
    trigger = rng.choices(
        list(params.trigger_weights), weights=list(params.trigger_weights.values())
    )[0]
    target = rng.choices(
        list(params.target_weights), weights=list(params.target_weights.values())
    )[0]
    if trigger != "now":
        armed = {
            t: w for t, w in params.trigger_weights.items() if t != "now" and w > 0
        }
        met = wait_for_trigger(armed, endpoints, kube)
        if met is None:
            log(f"no trigger of {sorted(armed)} met; not restarting anything")
            return 0
        trigger = met
    # The trigger wait may have run into a quiet period.
    if quiet_period_active(endpoints):
        log("quiet period active; not restarting anything")
        return 0
    try:
        victim = pick_victim(kube, target)
    except TRANSIENT_ERRORS as e:
        log(f"listing {target} pods failed: {e}")
        return 0
    if victim is None:
        log(f"no {target} pod to delete")
        return 0
    grace = rng.choice(GRACE_MENU)
    if not delete_recorded(
        kube,
        origin="pod_restarts",
        role=victim.role,
        namespace=victim.namespace,
        pod=victim.pod,
        uid=victim.uid,
        grace=grace,
        reason=trigger,
    ):
        return 0
    details = {
        "role": victim.role,
        "pod": victim.pod,
        "uid": victim.uid,
        "grace": grace,
        "trigger": trigger,
    }
    log(f"deleted {victim.namespace}/{victim.pod}: {details}")
    note_target(victim, details)
    note_trigger(trigger, details)
    return 0

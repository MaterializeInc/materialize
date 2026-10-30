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
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from kubernetes import client  # type: ignore
from kubernetes.client.rest import ApiException  # type: ignore

from materialize.antithesis import quiet, telemetry, templates
from materialize.antithesis.drivers import (
    kafka_sources,
    lifecycle,
    postgres_sources,
    recovery,
    schema_evolution,
)
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
from materialize.antithesis.observations import reachable
from materialize.antithesis.rng import rng
from materialize.antithesis.state import open_db

RESTARTS_DB = "pod_restarts"

TARGETS = ("environmentd_active", "environmentd_candidate", "clusterd", "orchestratord")
TRIGGERS = (
    "now",
    "lifecycle_pending",
    "lifecycle_ddl",
    "postgres_terminal",
    "schema_evo_alter",
    "rollout_applying",
    "rollout_promoting",
    "source_created",
    "source_created_applying",
)
# A source or source export created this recently is still in the window
# where its shards' frontiers start at the minimum and the controller has yet
# to install and persist the export's hold on the remap shard (SS-346).
# Needs calibration on one simulated core.
SOURCE_CREATED_WINDOW_SECONDS = 30.0
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
    cluster_id: str = "",
    victim_generation: int | None = None,
    deadline: float | None = None,
) -> bool:
    """Delete a pod, recording the attempt first. Returns whether the API acknowledged it.

    `uid`, when given, is sent as a delete precondition, so a pod recreated
    under the same name since it was listed is left alone.
    """
    if deadline is not None and time.monotonic() >= deadline:
        return False
    if uid:
        recovery.preserve_pod_exit(
            kube.env,
            namespace=namespace,
            pod=pod,
            uid=uid,
            deadline=min(time.monotonic() + 2.0, deadline or float("inf")),
        )
    bind_role = role in ("environmentd_active", "environmentd_candidate")
    before = (
        kube.try_snapshot()
        if bind_role and uid and victim_generation is not None
        else None
    )
    if deadline is not None and time.monotonic() >= deadline:
        return False
    db = open_restarts(kube.env.endpoints)
    try:
        started = time.monotonic()
        with db:
            seq = db.execute(
                "INSERT INTO restarts"
                " (at, origin, role, namespace, pod, uid, grace, reason, outcome)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'attempted')",
                (started, origin, role, namespace, pod, uid, grace, reason),
            ).lastrowid
        try:
            kube.core.delete_namespaced_pod(
                pod,
                namespace,
                grace_period_seconds=grace,
                body=client.V1DeleteOptions(
                    preconditions=client.V1Preconditions(uid=uid) if uid else None
                ),
                _request_timeout=(  # pyright: ignore[reportCallIssue]
                    K8S_TIMEOUT_SECONDS
                    if deadline is None
                    else min(
                        K8S_TIMEOUT_SECONDS, max(0.001, deadline - time.monotonic())
                    )
                ),
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
        completed = time.monotonic() if outcome == "acked" else None
        observed_role = role
        if bind_role and outcome == "acked":
            observed_role = "environmentd"
            after = (
                kube.try_snapshot()
                if before is not None
                and (deadline is None or time.monotonic() < deadline)
                else None
            )
            if (
                before is not None
                and after is not None
                and before.active is not None
                and before.active == after.active
                and before.resource_id is not None
                and before.resource_id == after.resource_id
            ):
                if victim_generation == before.active:
                    observed_role = "environmentd_active"
                elif victim_generation == before.active + 1:
                    observed_role = "environmentd_candidate"
        with db:
            db.execute(
                "UPDATE restarts SET outcome = ?, role = ? WHERE seq = ?",
                (outcome, observed_role, seq),
            )
        if outcome == "acked":
            assert completed is not None
            telemetry.fault_observed(
                observed_role,
                started,
                completed,
                key=f"pod_restarts:{seq}",
                cluster_id=cluster_id,
            )
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


def _count(endpoints: Endpoints, name: str, query: str, params: tuple = ()) -> int:
    conn = peek_db(name, endpoints)
    if conn is None:
        return 0
    try:
        row = conn.execute(query, params).fetchone()
        return int(row[0]) if row else 0
    except sqlite3.Error:
        return 0
    finally:
        conn.close()


def source_created_recently(endpoints: Endpoints) -> bool:
    """Whether the Postgres or Kafka source driver issued a source or export
    creation within `SOURCE_CREATED_WINDOW_SECONDS`. Both record wall-clock
    times in this container."""
    since = (time.time() - SOURCE_CREATED_WINDOW_SECONDS,)
    return (
        _count(
            endpoints,
            postgres_sources.STATE_DB,
            postgres_sources.RECENT_EXPORTS_QUERY,
            since,
        )
        > 0
        or _count(
            endpoints,
            kafka_sources.STATE_DB,
            kafka_sources.RECENT_GENERATIONS_QUERY,
            since,
        )
        > 0
    )


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
    if trigger == "schema_evo_alter":
        conn = peek_db(schema_evolution.STATE_DB, endpoints)
        if conn is None:
            return False
        try:
            return schema_evolution.alter_recent(conn, lifecycle.pid_alive)
        except sqlite3.Error:
            return False
        finally:
            conn.close()
    if trigger == "source_created":
        return source_created_recently(endpoints)
    if snap is None:
        return False
    if trigger == "rollout_applying":
        return snap.reason == "Applying"
    if trigger == "source_created_applying":
        return snap.reason == "Applying" and source_created_recently(endpoints)
    assert trigger == "rollout_promoting", trigger
    return snap.reason == "Promoting"


@dataclass(frozen=True)
class Victim:
    role: str
    namespace: str
    pod: str
    uid: str | None
    cluster_id: str = ""
    generation: int | None = None


def pick_victim(
    kube: Kube, target: str, *, cluster_id: str | None = None
) -> Victim | None:
    if target.startswith("environmentd_"):
        snap = kube.try_snapshot()
        if snap is None or snap.active is None:
            return None
        generation = snap.active if target == "environmentd_active" else snap.active + 1
        envd = [p for p in kube.envd_pods() if p.generation == generation]
        if not envd:
            return None
        chosen = rng.choice(envd)
        return Victim(
            target,
            kube.namespace,
            chosen.name,
            chosen.uid,
            generation=chosen.generation,
        )
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
            ids = {
                str(value)
                for key, value in (metadata.labels or {}).items()
                if key.endswith("/cluster-id")
            }
            observed_id = next(iter(ids)) if len(ids) == 1 else ""
            if (
                cluster_id is not None
                and target == "clusterd"
                and observed_id != cluster_id
            ):
                continue
            assert metadata.name is not None
            candidates.append(
                Victim(target, namespace, metadata.name, metadata.uid, observed_id)
            )
    return rng.choice(candidates) if candidates else None


def interleaving_disruption(
    kube: Kube, *, cluster_id: str, deadline: float
) -> Callable[[], None] | None:
    """Choose a bounded disruption for a staged transaction and its maintained cluster."""
    if quiet_period_active(kube.env.endpoints) or time.monotonic() >= deadline:
        return None
    rolling = templates.enabled_driver("rollouts") or templates.enabled_driver(
        "spec_phase"
    )
    snap = kube.try_snapshot() if rolling else None
    if (
        snap is not None
        and snap.reason == "ReadyToPromote"
        and snap.request is not None
        and snap.generation is not None
        and snap.observed_generation == snap.generation
        and rng.random() < 0.5
    ):

        def promote() -> None:
            if quiet_period_active(kube.env.endpoints) or time.monotonic() >= deadline:
                return
            try:
                current = kube.snapshot()
                if (
                    current.reason != "ReadyToPromote"
                    or current.request != snap.request
                    or current.generation != snap.generation
                    or current.observed_generation != current.generation
                ):
                    return
                kube.patch_spec({"forcePromote": snap.request})
                stop = min(deadline, time.monotonic() + 15.0)
                while time.monotonic() < stop:
                    started = time.monotonic()
                    observed = kube.snapshot()
                    if observed.reason == "Promoting":
                        telemetry.phase_observed(
                            observed.reason,
                            observed.active,
                            observed.request,
                            started=started,
                            force=True,
                        )
                    if observed.request != snap.request or observed.reason in (
                        "Promoting",
                        "Applied",
                    ):
                        return
                    time.sleep(min(0.25, max(0.0, stop - time.monotonic())))
            except TRANSIENT_ERRORS as error:
                log(f"interleaving promotion unavailable: {type(error).__name__}")

        return promote
    roles = ["environmentd_active", "clusterd"]
    if rolling:
        roles.append("environmentd_candidate")
    try:
        for role in rng.sample(roles, len(roles)):
            victim = pick_victim(kube, role, cluster_id=cluster_id)
            if victim is None:
                continue
            grace = rng.choice([0, 1])

            def delete(victim: Victim = victim, grace: int = grace) -> None:
                if (
                    quiet_period_active(kube.env.endpoints)
                    or time.monotonic() >= deadline
                ):
                    return
                if rolling:
                    started = time.monotonic()
                    observed = kube.try_snapshot()
                    if observed is not None:
                        telemetry.phase_observed(
                            observed.reason,
                            observed.active,
                            observed.request,
                            started=started,
                            force=True,
                        )
                if time.monotonic() >= deadline:
                    return
                delete_recorded(
                    kube,
                    origin="interleavings",
                    role=victim.role,
                    namespace=victim.namespace,
                    pod=victim.pod,
                    uid=victim.uid,
                    grace=grace,
                    reason="staged transaction",
                    cluster_id=victim.cluster_id,
                    victim_generation=victim.generation,
                    deadline=deadline,
                )

            return delete
    except TRANSIENT_ERRORS as error:
        log(f"interleaving target unavailable: {type(error).__name__}")
    return None


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
    rollouts_enabled = templates.enabled_driver("rollouts") or templates.enabled_driver(
        "spec_phase"
    )
    available_targets = [
        t for t in TARGETS if t != "environmentd_candidate" or rollouts_enabled
    ]
    available_triggers = ["now"]
    if templates.enabled_driver("lifecycle"):
        available_triggers.extend(("lifecycle_pending", "lifecycle_ddl"))
    if templates.enabled_driver("postgres_sources"):
        available_triggers.append("postgres_terminal")
    if templates.enabled_driver("schema_evolution"):
        available_triggers.append("schema_evo_alter")
    sources_enabled = templates.enabled_driver(
        "postgres_sources"
    ) or templates.enabled_driver("kafka_sources")
    if rollouts_enabled:
        available_triggers.extend(("rollout_applying", "rollout_promoting"))
    if sources_enabled:
        available_triggers.append("source_created")
        if rollouts_enabled:
            available_triggers.append("source_created_applying")
    while True:
        targets = {t: rng.choice([0, 1, 1, 4]) for t in available_targets}
        if any(targets.values()):
            break
    triggers = {
        t: rng.choice([0, 1, 1, 4]) for t in TRIGGERS if t in available_triggers
    }
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
    needs_cr = any(t.startswith("rollout_") or t.endswith("_applying") for t in weights)
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
    elif trigger == "schema_evo_alter":
        reachable(
            "Pod restart driver deleted a pod while ALTER TABLE ADD COLUMN was in flight or had just returned",
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
    elif trigger == "source_created":
        reachable(
            "Pod restart driver deleted a pod shortly after a source or source export was created",
            details,
        )
    elif trigger == "source_created_applying":
        reachable(
            "Pod restart driver deleted a pod shortly after a source or source export was created while a rollout was Applying",
            details,
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
        cluster_id=victim.cluster_id,
        victim_generation=victim.generation,
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

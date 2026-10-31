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
`unknown`, or `cancelled` before sending. An `attempted` or `unknown` delete
may still have happened.
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
    before_delete: Callable[[], None] | None = None,
    still_pending: Callable[[], bool] | None = None,
) -> bool:
    """Delete a pod, recording the attempt first. Returns whether the API acknowledged it.

    `uid`, when given, is sent as a delete precondition, so a pod recreated
    under the same name since it was listed is left alone.
    """
    if (deadline is not None and time.monotonic() >= deadline) or (
        still_pending is not None and not still_pending()
    ):
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
    if (deadline is not None and time.monotonic() >= deadline) or (
        still_pending is not None and not still_pending()
    ):
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
        if (deadline is not None and time.monotonic() >= deadline) or (
            still_pending is not None and not still_pending()
        ):
            with db:
                db.execute(
                    "UPDATE restarts SET outcome = 'cancelled' WHERE seq = ?", (seq,)
                )
            return False
        try:
            if before_delete is not None:
                before_delete()
            # Journal and callback work precede the actual fault request.
            started = time.monotonic()
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
        telemetry.interleaving_progress(
            "disruption",
            action="delete",
            outcome=outcome,
            role=observed_role,
            pod_uid=uid,
            cluster_id=cluster_id,
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


@dataclass(frozen=True)
class InterleavingDisruption:
    """Select a live target before optionally racing COMMIT with the effect."""

    run: Callable[[Callable[[], bool], Callable[[], None]], None]

    def __call__(self) -> None:
        self.run(lambda: True, lambda: None)


class _WriteCompleted(Exception):
    pass


def _owned_interleaving(
    kube: Kube, deadline: float
) -> tuple[bool, InterleavingDisruption | None]:
    from materialize.antithesis import rollout_steps
    from materialize.antithesis.drivers import rollouts

    db = rollouts.requests_db(kube.env)
    lease = rollouts.CrLease(kube.env)
    acquired = False
    try:
        acquired = lease.acquire()
        if not acquired:
            return False, None
        scenario = rollout_steps.load(db)
        if scenario is not None and scenario.owner != "interleavings":
            return False, None
        snap = kube.try_snapshot()
        if snap is None:
            return scenario is not None, None
        ctx = rollouts.Ctx(kube.env, kube, db, rollouts.timeline_params(db), lease)
        if scenario is None:
            if (
                snap.reason != "Applied"
                or snap.request != snap.last_completed
                or rng.random() >= 0.5
            ):
                return False, None
            changes = {**rollouts.new_request(), "rolloutStrategy": "ManuallyPromote"}
            scenario = rollout_steps.Scenario(
                rollouts.new_uuid(),
                rng.choice(["promote", "environmentd_candidate"]),
                snap.obj,
                time.monotonic(),
                False,
                owner="interleavings",
                data={"spec": {**snap.tracked_spec(), **changes}},
                request=changes["requestRollout"],
                deadline=time.monotonic() + rollouts.FOLLOW_TIMEOUT_SECONDS,
                intent=changes,
                from_request=snap.request,
            )
            rollout_steps.save(db, scenario)
        expected = (
            scenario.data["spec"]
            if snap.request == scenario.request
            else rollouts.Snapshot(scenario.prior).tracked_spec()
        )
        if scenario.stage == "cleanup":
            expected = dict(expected)
            if snap.spec.get("forcePromote") in (
                expected.get("forcePromote"),
                scenario.request,
            ):
                expected["forcePromote"] = snap.spec.get("forcePromote")
            prior_strategy = rollouts.Snapshot(scenario.prior).spec.get(
                "rolloutStrategy", rollouts.DEFAULT_ROLLOUT_STRATEGY
            )
            if snap.spec.get("rolloutStrategy") in ("ManuallyPromote", prior_strategy):
                expected["rolloutStrategy"] = snap.spec.get("rolloutStrategy")
        if (
            snap.request not in (scenario.from_request, scenario.request)
            or snap.tracked_spec() != expected
            or snap.resource_id != rollouts.Snapshot(scenario.prior).resource_id
            or (
                "generation" in scenario.data
                and snap.generation != scenario.data["generation"]
                and scenario.stage != "cleanup"
            )
        ):
            # A superseding request belongs to its writer, including its strategy.
            with db:
                db.execute("DELETE FROM rollout_scenario WHERE slot = 0")
            return True, None
        expired = time.monotonic() >= scenario.deadline
        if scenario.stage == "cleanup" or expired:
            if snap.request == scenario.request:
                _, stored = rollouts.submit_patch(
                    ctx,
                    "interleavings_restore",
                    {
                        "rolloutStrategy": rollouts.Snapshot(scenario.prior).spec.get(
                            "rolloutStrategy", rollouts.DEFAULT_ROLLOUT_STRATEGY
                        ),
                        "forcePromote": scenario.request,
                    },
                )
                if stored is None:
                    return True, None
            with db:
                db.execute("DELETE FROM rollout_scenario WHERE slot = 0")
            return True, None
        if snap.request != scenario.request:
            assert scenario.intent is not None
            rollouts.submit_patch(ctx, "interleavings_prepare", scenario.intent)
            return True, None
        if "generation" not in scenario.data:
            scenario.data["generation"] = snap.generation
            rollout_steps.save(db, scenario)
        if (
            snap.reason != "ReadyToPromote"
            or snap.generation is None
            or snap.observed_generation != snap.generation
        ):
            return True, None
        scenario_id, request, generation = (
            scenario.scenario_id,
            scenario.request,
            snap.generation,
        )
    except rollouts.LeaseLost:
        log("interleaving preparation lost its CR lease")
        return True, None
    finally:
        try:
            if acquired:
                lease.release()
        except sqlite3.Error as error:
            log(f"releasing interleaving lease failed: {error}")
        finally:
            lease.db.close()
            db.close()

    def act(pending: Callable[[], bool], release_commit: Callable[[], None]) -> None:
        db = rollouts.requests_db(kube.env)
        lease = rollouts.CrLease(kube.env)
        acquired = False
        try:
            acquired = lease.acquire()
            if not acquired or not pending() or quiet_period_active(kube.env.endpoints):
                return
            current = rollout_steps.load(db)
            snap = kube.try_snapshot()
            if (
                current is None
                or current.owner != "interleavings"
                or current.scenario_id != scenario_id
                or current.stage == "cleanup"
                or snap is None
                or snap.request != request
                or snap.generation != generation
                or snap.observed_generation != generation
                or snap.reason != "ReadyToPromote"
                or time.monotonic() >= deadline
                or not pending()
            ):
                return
            victim = (
                pick_victim(kube, "environmentd_candidate")
                if current.action == "environmentd_candidate"
                else None
            )
            if current.action == "environmentd_candidate" and victim is None:
                return
            # An interrupted action can restore this request but cannot repeat a kill.
            current.stage = "cleanup"
            rollout_steps.save(db, current)
            if not pending() or time.monotonic() >= deadline:
                return
            if victim is not None:
                _delete_interleaved(kube, victim, deadline, pending, release_commit)
                return
            ctx = rollouts.Ctx(kube.env, kube, db, rollouts.timeline_params(db), lease)

            def before_patch() -> None:
                if not pending() or time.monotonic() >= deadline:
                    raise _WriteCompleted
                release_commit()

            rollouts.submit_patch(
                ctx,
                "interleavings_promote",
                {"forcePromote": request},
                before_patch=before_patch,
            )
            telemetry.interleaving_progress(
                "disruption", action="promote", outcome="requested", request=request
            )
            stop = min(deadline, time.monotonic() + 15.0)
            while pending() and time.monotonic() < stop:
                lease.renew()
                started = time.monotonic()
                observed = kube.try_snapshot()
                if observed is None or observed.request != request or not pending():
                    return
                if (
                    observed.reason == "Promoting"
                    and observed.observed_generation == observed.generation
                ):
                    telemetry.phase_observed(
                        observed.reason,
                        observed.active,
                        observed.request,
                        started=started,
                        force=True,
                    )
                    telemetry.interleaving_progress(
                        "disruption",
                        action="promote",
                        outcome="observed",
                        phase=observed.reason,
                        request=observed.request,
                    )
                    return
                if observed.reason == "Applied":
                    return
                time.sleep(min(0.25, max(0.0, stop - time.monotonic())))
        except _WriteCompleted:
            return
        except TRANSIENT_ERRORS as error:
            log(f"interleaving action unavailable: {type(error).__name__}")
        except rollouts.LeaseLost:
            log("interleaving action lost its CR lease")
        finally:
            try:
                if acquired:
                    lease.release()
            except sqlite3.Error as error:
                log(f"releasing interleaving lease failed: {error}")
            finally:
                lease.db.close()
                db.close()

    return True, InterleavingDisruption(act)


def _delete_interleaved(
    kube: Kube,
    victim: Victim,
    deadline: float,
    pending: Callable[[], bool],
    release_commit: Callable[[], None],
) -> None:
    if time.monotonic() >= deadline or quiet_period_active(kube.env.endpoints):
        return
    delete_recorded(
        kube,
        origin="interleavings",
        role=victim.role,
        namespace=victim.namespace,
        pod=victim.pod,
        uid=victim.uid,
        grace=rng.choice([0, 1]),
        reason="staged transaction",
        cluster_id=victim.cluster_id,
        victim_generation=victim.generation,
        deadline=deadline,
        still_pending=pending,
        before_delete=release_commit,
    )


def interleaving_disruption(
    kube: Kube, *, cluster_id: str, deadline: float
) -> InterleavingDisruption | None:
    """Prepare an owned rollout or select a pod only inside unresolved SQL."""
    if quiet_period_active(kube.env.endpoints) or time.monotonic() >= deadline:
        return None
    rolling = templates.enabled_driver("rollouts") or templates.enabled_driver(
        "spec_phase"
    )
    try:
        if rolling:
            owned, action = _owned_interleaving(kube, deadline)
            if owned:
                return action
    except TRANSIENT_ERRORS as error:
        log(f"interleaving preparation unavailable: {type(error).__name__}")
        return None

    def delete(pending: Callable[[], bool], release_commit: Callable[[], None]) -> None:
        if quiet_period_active(kube.env.endpoints) or time.monotonic() >= deadline:
            return
        roles = ["environmentd_active", "clusterd"]
        if rolling:
            roles.append("environmentd_candidate")
        try:
            for role in rng.sample(roles, len(roles)):
                if not pending() or time.monotonic() >= deadline:
                    return
                victim = pick_victim(kube, role, cluster_id=cluster_id)
                if victim is None:
                    continue
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
                if not pending() or time.monotonic() >= deadline:
                    return
                _delete_interleaved(kube, victim, deadline, pending, release_commit)
                return
        except TRANSIENT_ERRORS as error:
            log(f"interleaving target unavailable: {type(error).__name__}")

    return InterleavingDisruption(delete)


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

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Start focused workloads on the target image before configuring or loading it."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from typing import Any

from kubernetes.client.rest import ApiException  # type: ignore

from materialize.antithesis import sql, state
from materialize.antithesis.drivers import rollouts
from materialize.antithesis.environment import Environment

BOOTSTRAP_SECONDS = rollouts.T_ROLLOUT_SECONDS


def _identity(snap: rollouts.Snapshot) -> tuple[object, ...]:
    return (
        snap.generation,
        snap.active,
        snap.resource_id,
        snap.request,
        snap.last_completed,
        snap.last_completed_image,
        snap.reason,
        snap.condition_status,
        snap.observed_generation,
        snap.spec.get("environmentdImageRef"),
    )


def _serving_target(
    kube: rollouts.Kube,
    env: Environment,
    snap: rollouts.Snapshot,
    target: str,
    details: dict[str, Any],
) -> bool:
    details["waiting_for"] = "cr_status"
    if not (
        snap.generation is not None
        and snap.observed_generation == snap.generation
        and snap.condition_status == "True"
        and snap.reason == "Applied"
        and snap.request is not None
        and snap.last_completed == snap.request
        and snap.spec.get("environmentdImageRef") == target
        and snap.last_completed_image == target
        and snap.active is not None
        and snap.resource_id is not None
    ):
        return False
    sets = kube.envd_statefulsets(snap.resource_id)
    details["statefulsets"] = [[s.generation, s.image, s.deleting] for s in sets]
    active_sets = [s for s in sets if s.generation == snap.active]
    details["waiting_for"] = "active_statefulset"
    if len(active_sets) != 1 or (
        active_sets[0].image != target or active_sets[0].deleting
    ):
        return False
    prefix = f"mz{snap.resource_id}-environmentd-"
    pods = [p for p in kube.envd_pods() if p.name.startswith(prefix)]
    details["pod_generations"] = [[p.generation, p.ready] for p in pods]
    active_pods = [p for p in pods if p.generation == snap.active]
    details["waiting_for"] = "active_pod"
    if len(active_pods) != 1:
        return False
    pod = active_pods[0]
    details["pod"] = {
        "generation": pod.generation,
        "image": pod.image,
        "ready": pod.ready,
    }
    if not (
        pod.generation == snap.active
        and pod.ready
        and pod.started_at is not None
        and pod.image == target
        and pod.ip is not None
    ):
        return False
    details["waiting_for"] = "active_leader"
    if rollouts.leader_statuses([pod]).get(pod.uid) != "IsLeader":
        return False
    versions = []
    details["waiting_for"] = "sql_serving"
    for host in (pod.ip, env.sql_host()):
        with sql.connection(
            host, connect_timeout=5, statement_timeout_ms=5_000
        ) as conn:
            row = conn.execute("SELECT mz_version()").fetchone()
            versions.append(str(row[0]) if row else "")
    details["serving_versions"] = versions
    if not versions[0] or versions[0] != versions[1]:
        return False
    expected = rollouts.expected_version(target)
    details["waiting_for"] = "target_version"
    if expected is not None and versions[0].split(" ")[0].split("+")[0] != expected:
        return False
    details["waiting_for"] = "stable_status"
    if _identity(kube.snapshot()) != _identity(snap):
        return False
    details["waiting_for"] = "ready"
    return True


def _record_progress(
    db: sqlite3.Connection, target: str, details: dict[str, Any]
) -> None:
    stable = dict(details)
    status = stable.get("last_status")
    if isinstance(status, dict):
        stable["last_status"] = {
            key: value for key, value in status.items() if key != "resource_version"
        }
    payload = json.dumps(stable, sort_keys=True)
    previous = db.execute(
        "SELECT details FROM bootstrap_progress WHERE target = ?", (target,)
    ).fetchone()
    if previous is not None and previous[0] == payload:
        return
    with db:
        db.execute(
            "INSERT OR REPLACE INTO bootstrap_progress VALUES (?, ?)",
            (target, payload),
        )
    print(f"focused bootstrap: {payload}", flush=True)


def _request_changes(request: str) -> dict[str, Any]:
    return {
        "requestRollout": request,
        "forceRollout": request,
        # Catch-up must be skipped during Applying, before ReadyToPromote.
        "forcePromote": request,
        "rolloutRequestTimeout": rollouts.DEFAULT_ROLLOUT_REQUEST_TIMEOUT,
    }


def ensure_target_image() -> None:
    """Require the active target image to serve SQL within one persistent bootstrap budget."""
    env = Environment()
    target = env.endpoints.environmentd_image
    if target is None:
        raise RuntimeError("focused workloads require MZ_ANTITHESIS_ENVIRONMENTD_IMAGE")
    db = state.open_db("focused_bootstrap", env.endpoints)
    try:
        with db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS target_image (target TEXT PRIMARY KEY, request TEXT NOT NULL, deadline REAL NOT NULL, completed INTEGER NOT NULL)"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS bootstrap_progress (target TEXT PRIMARY KEY, details TEXT NOT NULL)"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS bootstrap_retry (target TEXT PRIMARY KEY, previous_request TEXT NOT NULL, request TEXT NOT NULL)"
            )
            request = str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"antithesis-bootstrap:{env.endpoints.namespace}/{env.endpoints.environment}/{target}",
                )
            )
            db.execute(
                "INSERT OR IGNORE INTO target_image VALUES (?, ?, ?, 0)",
                (target, request, time.monotonic() + BOOTSTRAP_SECONDS),
            )
        row = db.execute(
            "SELECT request, deadline, completed FROM target_image WHERE target = ?",
            (target,),
        ).fetchone()
        assert row is not None
        request, deadline, completed = row
        kube = rollouts.Kube(env)
        revalidate = bool(completed)
        last_error = "target not ready"
        details: dict[str, Any] = {"target": target, "last_status": None}
        while revalidate or time.monotonic() < deadline:
            revalidate = False
            try:
                snap = kube.snapshot()
                details = {"target": target, "last_status": snap.summary()}
                if _serving_target(kube, env, snap, target, details):
                    if not completed and time.monotonic() >= deadline:
                        details["waiting_for"] = "deadline"
                        _record_progress(db, target, details)
                        break
                    with db:
                        db.execute(
                            "UPDATE target_image SET completed = 1 WHERE target = ?",
                            (target,),
                        )
                    _record_progress(db, target, details)
                    return
                if time.monotonic() >= deadline:
                    _record_progress(db, target, details)
                    break
                changes = {}
                if (
                    snap.spec.get("rolloutStrategy")
                    != rollouts.DEFAULT_ROLLOUT_STRATEGY
                ):
                    changes["rolloutStrategy"] = rollouts.DEFAULT_ROLLOUT_STRATEGY
                if snap.spec.get("environmentdImageRef") != target:
                    # Intent precedes the API call, so a lost response retries the same rollout.
                    changes.update(
                        environmentdImageRef=target,
                        **_request_changes(request),
                    )
                else:
                    retry = db.execute(
                        "SELECT previous_request, request FROM bootstrap_retry WHERE target = ?",
                        (target,),
                    ).fetchone()
                    if (
                        retry is not None
                        and snap.request == retry[0]
                        and request == retry[1]
                    ):
                        changes.update(_request_changes(request))
                    elif (
                        snap.generation is not None
                        and snap.observed_generation == snap.generation
                        and snap.request is not None
                    ):
                        if (
                            retry is None
                            and snap.request == request
                            and snap.condition_status == "False"
                            and snap.reason
                            in ("FailedDeploy", "RolloutTimeout", "WaitingForApproval")
                        ):
                            next_request = str(
                                uuid.uuid5(
                                    uuid.NAMESPACE_URL,
                                    f"antithesis-bootstrap-retry:{request}",
                                )
                            )
                            # A rejected rollout needs a fresh request. Persist one retry
                            # before sending it, retaining the original bootstrap deadline.
                            with db:
                                db.execute(
                                    "INSERT INTO bootstrap_retry VALUES (?, ?, ?)",
                                    (target, request, next_request),
                                )
                                db.execute(
                                    "UPDATE target_image SET request = ?, completed = 0 WHERE target = ?",
                                    (next_request, target),
                                )
                            request, completed = next_request, False
                            changes.update(_request_changes(request))
                        elif snap.spec.get("forcePromote") != snap.request and (
                            snap.reason == "ReadyToPromote"
                            or (snap.reason == "Applying" and snap.request == request)
                        ):
                            changes["forcePromote"] = snap.request
                if changes:
                    details["request_changes"] = sorted(changes)
                    kube.patch_spec(changes)
            except rollouts.TRANSIENT_ERRORS as error:
                status = getattr(error, "status", None)
                if (
                    isinstance(error, ApiException)
                    and status is not None
                    and 400 <= status < 500
                    and status not in (408, 409, 429)
                ):
                    details["api_error_status"] = status
                    _record_progress(db, target, details)
                    raise
                last_error = type(error).__name__
                details["last_error"] = last_error
            _record_progress(db, target, details)
            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(1.0, remaining))
        raise TimeoutError(
            f"focused workload target did not become ready within {BOOTSTRAP_SECONDS}s ({last_error}): {details}"
        )
    finally:
        db.close()

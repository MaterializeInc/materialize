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
    if len(sets) != 1 or (
        sets[0].generation != snap.active or sets[0].image != target or sets[0].deleting
    ):
        return False
    prefix = f"mz{snap.resource_id}-environmentd-"
    pods = [p for p in kube.envd_pods() if p.name.startswith(prefix)]
    if len(pods) != 1:
        return False
    pod = pods[0]
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
        and rollouts.leader_statuses([pod]).get(pod.uid) == "IsLeader"
    ):
        return False
    versions = []
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
    if expected is not None and versions[0].split(" ")[0].split("+")[0] != expected:
        return False
    return _identity(kube.snapshot()) == _identity(snap)


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
                        break
                    with db:
                        db.execute(
                            "UPDATE target_image SET completed = 1 WHERE target = ?",
                            (target,),
                        )
                    return
                if time.monotonic() >= deadline:
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
                        requestRollout=request,
                        forceRollout=request,
                        forcePromote=None,
                        rolloutRequestTimeout=rollouts.DEFAULT_ROLLOUT_REQUEST_TIMEOUT,
                    )
                elif (
                    snap.reason == "ReadyToPromote"
                    and snap.generation is not None
                    and snap.observed_generation == snap.generation
                    and snap.request is not None
                ):
                    changes["forcePromote"] = snap.request
                if changes:
                    kube.patch_spec(changes)
            except rollouts.TRANSIENT_ERRORS as error:
                status = getattr(error, "status", None)
                if (
                    isinstance(error, ApiException)
                    and status is not None
                    and 400 <= status < 500
                    and status not in (408, 409, 429)
                ):
                    raise
                last_error = type(error).__name__
            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(1.0, remaining))
        raise TimeoutError(
            f"focused workload target did not become ready within {BOOTSTRAP_SECONDS}s ({last_error}): {details}"
        )
    finally:
        db.close()

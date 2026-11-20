# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Disruptions the driver causes itself, beyond Antithesis's faults.

Antithesis faults are untargeted. The driver adds the ones that need a role
or a moment: deleting the active environmentd, a clusterd or an orchestratord
pod; changing a cluster's replicas; and 0dt rollouts, followed one step per
rule, which may kill a pod in a chosen rollout phase.

Each kind is paced by a cooldown drawn per timeline, not by rule weight: a
rule that swarm testing leaves as the only enabled one would otherwise
disrupt the system every round. Kubernetes requests run on one background
thread, so a slow API server stalls none of the rules.
"""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    reachable,
    sometimes,
)

from materialize.antithesis_driver.client import (
    Op,
    Outcome,
    Status,
    report_driver_error,
)
from materialize.antithesis_driver.context import RULE_WAIT_SECONDS, Context, log
from materialize.antithesis_driver.draw import Draw

CLUSTER = "driver"
OPERATOR_APP_NAME = "materialize-operator"
GRACE_MENU = (1, None)
"""Never 0: a zero grace period force-deletes the pod object, and the
StatefulSet then runs two incarnations of one pod at once."""
ROLLOUT_STRATEGIES = ("WaitUntilReady", "ManuallyPromote")
ROLLOUT_GIVE_UP_SECONDS = 20 * 60
KILL_PHASES = ("Applying", "ReadyToPromote", "Promoting")
ZERO_REPLICAS_SECONDS = 20.0


@dataclass
class Rollout:
    request: str
    strategy: str
    started: float
    kill_phase: str | None
    kill_target: str
    grace: int | None
    image: str | None
    killed: bool = False
    promoted: bool = False
    phases: tuple[str, ...] = ()


class Kube:
    """The Kubernetes side, used only when the driver runs next to orchestratord."""

    def __init__(self) -> None:
        from kubernetes import client  # type: ignore

        from materialize.antithesis.environment import Environment

        self.env = Environment()
        self.core = client.CoreV1Api()
        self.client = client

    def pods(self, target: str) -> list[Any]:
        from materialize.orchestratord import K8S_TIMEOUT_SECONDS

        endpoints = self.env.endpoints
        if target == "orchestratord":
            pods = self.core.list_namespaced_pod(
                endpoints.operator_namespace,
                label_selector=f"app.kubernetes.io/name={OPERATOR_APP_NAME}",
                _request_timeout=K8S_TIMEOUT_SECONDS,  # pyright: ignore[reportCallIssue]
            ).items
        else:
            pods = self.core.list_namespaced_pod(
                endpoints.namespace,
                _request_timeout=K8S_TIMEOUT_SECONDS,  # pyright: ignore[reportCallIssue]
            ).items
            if target == "environmentd":
                pods = [
                    p
                    for p in pods
                    if (p.metadata.labels or {}).get("materialize.cloud/app")
                    == "environmentd"
                ]
            else:
                pods = [
                    p
                    for p in pods
                    if any(k.endswith("/cluster-id") for k in p.metadata.labels or {})
                ]
        return [p for p in pods if p.metadata.deletion_timestamp is None]

    def delete(self, pod: Any, grace: int | None) -> None:
        from materialize.orchestratord import K8S_TIMEOUT_SECONDS

        body = self.client.V1DeleteOptions(
            grace_period_seconds=grace,
            preconditions=self.client.V1Preconditions(uid=pod.metadata.uid),
        )
        self.core.delete_namespaced_pod(
            pod.metadata.name,
            pod.metadata.namespace,
            body=body,
            grace_period_seconds=grace,
            _request_timeout=K8S_TIMEOUT_SECONDS,  # pyright: ignore[reportCallIssue]
        )


def generation(pod: Any) -> int | None:
    value = (pod.metadata.annotations or {}).get("materialize.cloud/generation")
    return int(value) if value is not None and value.isdigit() else None


class Disruptions:
    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        self._lock = threading.Lock()
        self.cooldown = 30.0
        self.last: dict[str, float] = {}
        self.replication_factor = 1
        self.zero_replicas_since: float | None = None
        self.kube = Kube() if ctx.config.kubernetes else None
        self._kube_worker = ThreadPoolExecutor(max_workers=1, thread_name_prefix="kube")
        self._kube_busy: Future[None] | None = None
        self.rollout: Rollout | None = None
        self.max_active_generation = -1
        self._requests = 0
        self.kill_probability = 0.0
        self.manual_share = 0.0

    def configure(self, d: Draw) -> None:
        self.cooldown = d.choice((15.0, 60.0, 180.0))
        self.kill_probability = d.choice((0.0, 0.3, 0.9))
        self.manual_share = d.choice((0.0, 0.5))

    def _due(self, name: str) -> bool:
        if self.ctx.quiesced.is_set():
            return False
        now = time.monotonic()
        with self._lock:
            if now - self.last.get(name, self.ctx.started) < self.cooldown:
                return False
            self.last[name] = now
            return True

    def _kube(self, name: str, action: Callable[[], None]) -> Status:
        """Run `action` on the Kubernetes thread, waiting briefly for it."""
        with self._lock:
            if self._kube_busy is not None and not self._kube_busy.done():
                return Status.NOT_SENT

            def run() -> None:
                # An action sent just before the eventually stopped faults
                # must not disrupt the system after.
                if self.ctx.quiesced.is_set():
                    return
                try:
                    action()
                except Exception as e:
                    if _is_api_error(e):
                        log(f"{name}: {type(e).__name__}: {e}")
                    else:
                        report_driver_error(name, e)

            future = self._kube_worker.submit(run)
            self._kube_busy = future
        try:
            future.result(timeout=RULE_WAIT_SECONDS)
            return Status.DONE
        except TimeoutError:
            return Status.PENDING

    def replication(self, d: Draw) -> Status:
        """Change the replication factor of the cluster that maintains the derived objects."""
        if not self._due("replication"):
            return Status.NOT_SENT
        # Zero replicas is rare and short: reads of an object maintained on a
        # cluster without replicas block until it has one again.
        factor = 0 if d.boolean(0.05) else d.integer(1, 2)
        statement = f"ALTER CLUSTER {CLUSTER} SET (REPLICATION FACTOR {factor})"

        def execute(conn: psycopg.Connection) -> None:
            conn.execute(statement.encode())

        def complete(outcome: Outcome[None]) -> None:
            if outcome.ok:
                with self._lock:
                    self.replication_factor = factor
                    self.zero_replicas_since = time.monotonic() if factor == 0 else None
                reachable(
                    "driver: a cluster replication factor change was acknowledged",
                    {"factor": factor},
                )

        return self.ctx.sessions.call(
            d.integer(0, len(self.ctx.sessions.sessions) - 1),
            Op("alter replication factor", execute, complete),
            RULE_WAIT_SECONDS,
        )

    def restore_if_overdue(self) -> None:
        """Give the derived cluster a replica again soon after it lost all.

        Reads of an object maintained on a cluster without replicas block,
        and swarm testing can disable the `replication` rule for a long
        stretch. Sends without waiting, so the invariant that calls it never
        stalls a round.
        """
        with self._lock:
            since = self.zero_replicas_since
        if since is None or time.monotonic() - since < ZERO_REPLICAS_SECONDS:
            return
        statement = f"ALTER CLUSTER {CLUSTER} SET (REPLICATION FACTOR 1)".encode()

        def execute(conn: psycopg.Connection) -> None:
            conn.execute(statement)

        def complete(outcome: Outcome[None]) -> None:
            if outcome.ok:
                with self._lock:
                    self.replication_factor = 1
                    self.zero_replicas_since = None

        for i in range(len(self.ctx.sessions.sessions)):
            op = Op("restore replication factor", execute, complete)
            if self.ctx.sessions.call(i, op, 0) != Status.NOT_SENT:
                return

    def resize(self, d: Draw) -> Status:
        """Change the replica size of the derived cluster, which replaces its replicas."""
        if len(self.ctx.config.replica_sizes) < 2 or not self._due("resize"):
            return Status.NOT_SENT
        size = d.choice(self.ctx.config.replica_sizes)
        statement = f"ALTER CLUSTER {CLUSTER} SET (SIZE '{size}')"

        def execute(conn: psycopg.Connection) -> None:
            conn.execute(statement.encode())

        def complete(outcome: Outcome[None]) -> None:
            if outcome.ok:
                reachable("driver: a cluster resize was acknowledged", {"size": size})

        return self.ctx.sessions.call(
            d.integer(0, len(self.ctx.sessions.sessions) - 1),
            Op("alter cluster size", execute, complete),
            RULE_WAIT_SECONDS,
        )

    def kill_pod(self, d: Draw) -> Status:
        kube = self.kube
        if kube is None or not self._due("kill_pod"):
            return Status.NOT_SENT
        target = d.choice(("environmentd", "clusterd", "clusterd", "orchestratord"))
        grace = d.choice(GRACE_MENU)
        # Draws favor small values, so an index into a short list is drawn
        # from a range of the list's size, not reduced modulo it.
        pick = d.integer(0, 7)

        def action() -> None:
            pods = kube.pods(target)
            if not pods:
                return
            pod = pods[pick % len(pods)]
            kube.delete(pod, grace)
            log(f"deleted {target} pod {pod.metadata.name} (grace {grace})")
            if target == "environmentd":
                reachable(
                    "driver: an environmentd pod was deleted",
                    {"pod": pod.metadata.name},
                )
            elif target == "clusterd":
                reachable(
                    "driver: a clusterd pod was deleted", {"pod": pod.metadata.name}
                )
            else:
                reachable(
                    "driver: an orchestratord pod was deleted",
                    {"pod": pod.metadata.name},
                )

        return self._kube("kill pod", action)

    def start_rollout(self, d: Draw) -> Status:
        kube = self.kube
        if kube is None or self.rollout is not None or not self._due("rollout"):
            return Status.NOT_SENT
        self._requests += 1
        # The request id must differ from every earlier one, or orchestratord
        # sees no new request.
        request = str(uuid.UUID(int=self._requests))
        strategy = (
            "ManuallyPromote" if d.boolean(self.manual_share) else "WaitUntilReady"
        )
        upgrade = kube.env.endpoints.upgrade_pending_image
        image = upgrade if upgrade is not None and d.boolean(0.5) else None
        rollout = Rollout(
            request=request,
            strategy=strategy,
            started=time.monotonic(),
            kill_phase=d.choice(KILL_PHASES)
            if d.boolean(self.kill_probability)
            else None,
            kill_target=d.choice(("active", "candidate", "orchestratord")),
            grace=d.choice(GRACE_MENU),
            image=image,
        )

        def action() -> None:
            spec: dict[str, Any] = {"rolloutStrategy": strategy}
            if image is not None:
                spec["environmentdImageRef"] = image
            kube.env.materialize.request_rollout(spec, request)
            with self._lock:
                self.rollout = rollout
            log(f"rollout {request} requested ({strategy}, image {image})")

        return self._kube("start rollout", action)

    def advance_rollout(self, d: Draw) -> Status:
        """Observe the rollout in progress, promote it, or kill a pod in its phase."""
        kube = self.kube
        rollout = self.rollout
        if kube is None or rollout is None:
            return Status.NOT_SENT

        def action() -> None:
            mz = kube.env.materialize.get()
            if mz is None:
                return
            status = mz.get("status") or {}
            active = status.get("activeGeneration")
            if isinstance(active, int):
                always(
                    active >= self.max_active_generation,
                    "Materialize status activeGeneration never decreases",
                    {"active": active, "max": self.max_active_generation},
                )
                self.max_active_generation = max(self.max_active_generation, active)
            condition = next(
                (
                    c
                    for c in status.get("conditions") or []
                    if c.get("type") == "UpToDate"
                ),
                {},
            )
            # Until orchestratord observes the patched spec, the condition
            # still describes the previous rollout.
            observed = condition.get("observedGeneration") == (
                mz.get("metadata") or {}
            ).get("generation")
            phase = condition.get("reason") if observed else None
            if phase is not None and phase not in rollout.phases:
                rollout.phases += (phase,)
                log(f"rollout {rollout.request}: {phase}, active generation {active}")
            if (
                status.get("lastCompletedRolloutRequest") == rollout.request
                and phase == "Applied"
            ):
                log(f"rollout {rollout.request} applied after {list(rollout.phases)}")
                reachable(
                    "driver: a requested rollout reached Applied",
                    {
                        "strategy": rollout.strategy,
                        "phases": list(rollout.phases),
                        "killed": rollout.killed,
                    },
                )
                sometimes(
                    rollout.killed,
                    "driver: a rollout reached Applied after a pod was killed in one of its phases",
                    {"phase": rollout.kill_phase, "target": rollout.kill_target},
                )
                self._finish(kube)
                return
            if rollout.kill_phase == phase and not rollout.killed:
                rollout.killed = True
                self._kill_for_rollout(kube, rollout, status)
            if (
                rollout.strategy == "ManuallyPromote"
                and phase == "ReadyToPromote"
                and not rollout.promoted
            ):
                kube.env.materialize.patch_spec({"forcePromote": rollout.request})
                rollout.promoted = True
                reachable("driver: a manually promoted rollout was promoted", {})
            if time.monotonic() - rollout.started > ROLLOUT_GIVE_UP_SECONDS:
                log(f"rollout {rollout.request} still {phase}, giving up following it")
                self._finish(kube)

        return self._kube("advance rollout", action)

    def _kill_for_rollout(
        self, kube: Kube, rollout: Rollout, status: dict[str, Any]
    ) -> None:
        active = status.get("activeGeneration")
        if rollout.kill_target == "orchestratord":
            pods = kube.pods("orchestratord")
        else:
            pods = [
                p
                for p in kube.pods("environmentd")
                if (generation(p) == active) == (rollout.kill_target == "active")
            ]
        if not pods:
            return
        pod = pods[0]
        kube.delete(pod, rollout.grace)
        log(
            f"rollout {rollout.request}: deleted {rollout.kill_target} pod {pod.metadata.name} in {rollout.kill_phase}"
        )
        reachable(
            "driver: a pod was deleted in a chosen rollout phase",
            {"phase": rollout.kill_phase, "target": rollout.kill_target},
        )

    def _finish(self, kube: Kube) -> None:
        rollout = self.rollout
        if rollout is not None and rollout.strategy != "WaitUntilReady":
            kube.env.materialize.patch_spec({"rolloutStrategy": "WaitUntilReady"})
        with self._lock:
            self.rollout = None

    def eventually(self, deadline: float) -> None:
        """Undo what disrupted the system, and wait for the CR to settle."""
        acked: list[bool] = []
        statement = f"ALTER CLUSTER {CLUSTER} SET (REPLICATION FACTOR 1)".encode()
        while not acked and time.monotonic() < deadline:
            for i in range(len(self.ctx.sessions.sessions)):
                status = self.ctx.sessions.call(
                    i,
                    Op(
                        "restore replication factor",
                        lambda conn: conn.execute(statement),
                        lambda outcome: acked.append(True) if outcome.ok else None,
                    ),
                    60.0,
                )
                if status != Status.NOT_SENT:
                    break
            if not acked:
                time.sleep(2)
        always(
            bool(acked),
            "driver: the derived cluster's replication factor is restored after faults stop",
            {},
        )
        kube = self.kube
        if kube is None:
            return
        up_to_date = False
        while time.monotonic() < deadline:
            try:
                mz = kube.env.materialize
                spec = (mz.get() or {}).get("spec") or {}
                if spec.get("rolloutStrategy") not in (None, "WaitUntilReady"):
                    mz.patch_spec({"rolloutStrategy": "WaitUntilReady"})
                if mz.phase() == "ReadyToPromote":
                    mz.patch_spec({"forcePromote": spec.get("requestRollout")})
                if mz.is_up_to_date():
                    up_to_date = True
                    break
            except Exception as e:
                if not _is_api_error(e):
                    raise
            time.sleep(5)
        always(
            up_to_date,
            "the Materialize CR is up to date after faults stop",
            {"phase": _safe(lambda: kube.env.materialize.phase())},
        )


def _safe(f: Callable[[], Any]) -> Any:
    try:
        return f()
    except Exception as e:
        return f"{type(e).__name__}: {e}"


def _is_api_error(e: BaseException) -> bool:
    """Errors the Kubernetes API or the network can return under faults."""
    import urllib3
    from kubernetes.client.rest import ApiException  # type: ignore

    return isinstance(e, (ApiException, urllib3.exceptions.HTTPError, OSError))

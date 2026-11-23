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
pod, also right after a DDL operation is sent; changing a cluster's replicas;
and 0dt rollouts, followed one step per rule, which may kill a pod in a chosen
rollout phase.

Each kind is paced by a cooldown drawn per timeline, not by rule weight: a
rule that swarm testing leaves as the only enabled one would otherwise
disrupt the system every round. Kubernetes requests run on one background
thread, so a slow API server stalls none of the rules.
"""

from __future__ import annotations

import random
import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    reachable,
    sometimes,
)

from materialize.antithesis import spec_model
from materialize.antithesis_driver.claims import STATUS_SETTLE_SECONDS, Claims
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
PATCH_PHASES = ("Applying", "ReadyToPromote", "Promoting", "Applied")
"""Phases in which the driver changes the spec of a rollout it follows.
`WaitingForApproval` is reached only through such a change, and the next
rollout request then acts in it."""
SETTLED_PHASES = ("Applied", "WaitingForApproval")
"""Phases that end a request once the status records it as completed."""
OBSERVE_SECONDS = 10.0
ZERO_REPLICAS_SECONDS = 20.0
DDL_KILL_DELAYS = (0.0, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0)
"""Seconds from sending a DDL operation to deleting the pod. Locally a DDL
statement takes 0.1 to 0.2 s, and the work that follows its catalog commit
starts about 0.1 s in."""


@dataclass(frozen=True)
class ArmedKill:
    target: str
    grace: int | None
    delay: float
    pick: int


@dataclass(frozen=True)
class SpecPatch:
    """A spec change the driver makes once the rollout shows the cell's phase."""

    cell: spec_model.Cell
    seed: int
    """Seeds the choices `spec_model.cell_changes` makes when the patch is
    built, which is after the rule that drew it returned."""
    serial: int
    """Makes the UUIDs of this patch differ from every other one."""


@dataclass
class Rollout:
    request: str
    strategy: str
    started: float
    kill_phase: str | None
    kill_target: str
    grace: int | None
    image: str | None
    patch: SpecPatch | None = None
    prior: dict[str, Any] = field(default_factory=dict)
    """The spec before the rollout was requested, which a `revert` restores."""
    killed: bool = False
    patched: bool = False
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
                    if p.metadata is not None
                    and (p.metadata.labels or {}).get("materialize.cloud/app")
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
        self.patch_probability = 0.0
        self.claims = Claims()
        self._observed = float("-inf")
        self._armed: ArmedKill | None = None
        self.ddl_kills = True

    def configure(self, d: Draw) -> None:
        self.cooldown = d.choice((15.0, 60.0, 180.0))
        self.kill_probability = d.choice((0.0, 0.3, 0.9))
        self.manual_share = d.choice((0.0, 0.5))
        self.patch_probability = d.choice((0.0, 0.3, 0.6))
        # One timeline in three: a rollout needs about two minutes in which
        # the driver deletes no environmentd pod. In a 4 h run 14% of the
        # rollouts without such a deletion reached Applied and 2% of those
        # with one, and with these kills in every timeline 11% of the
        # rollouts were without one after two minutes, down from 45%.
        self.ddl_kills = d.choice((False, False, True))

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
            Op("alter replication factor", execute, complete, ddl=True),
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
            Op("alter cluster size", execute, complete, ddl=True),
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

    def kill_after_ddl(self, d: Draw) -> Status:
        """Arm a pod deletion for right after the next DDL operation is sent.

        A catalog change is followed by work in other durable stores, such as
        registering a table's new schema with its shard. A process that dies
        in between leaves the two apart.
        """
        if self.kube is None or not self.ddl_kills or not self._due("kill_after_ddl"):
            return Status.NOT_SENT
        armed = ArmedKill(
            target=d.choice(
                ("environmentd", "environmentd", "environmentd", "clusterd")
            ),
            grace=d.choice(GRACE_MENU),
            delay=d.choice(DDL_KILL_DELAYS),
            pick=d.integer(0, 7),
        )
        with self._lock:
            self._armed = armed
        return Status.DONE

    def ddl_sent(self, name: str) -> None:
        """Runs on a session's thread, right before it sends a DDL operation."""
        kube = self.kube
        with self._lock:
            armed, self._armed = self._armed, None
        if armed is None or kube is None:
            return

        def run() -> None:
            time.sleep(armed.delay)
            if self.ctx.quiesced.is_set():
                return
            try:
                pods = kube.pods(armed.target)
                if not pods:
                    return
                pod = pods[armed.pick % len(pods)]
                kube.delete(pod, armed.grace)
            except Exception as e:
                if _is_api_error(e):
                    log(f"kill after ddl: {type(e).__name__}: {e}")
                else:
                    report_driver_error("kill after ddl", e)
                return
            log(
                f"deleted {armed.target} pod {pod.metadata.name} {armed.delay}s"
                f" after {name} was sent (grace {armed.grace})"
            )
            reachable(
                "driver: a pod was deleted right after a DDL operation was sent",
                {"op": name, "target": armed.target, "delay": armed.delay},
            )

        # Its own thread: the shared Kubernetes thread may be busy with a
        # rollout step, and the deletion has to land within the delay.
        threading.Thread(target=run, name="kill-after-ddl", daemon=True).start()

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
        patch = None
        if d.boolean(self.patch_probability):
            changed = d.choice(spec_model.FIELDS)
            self._requests += 1
            patch = SpecPatch(
                spec_model.Cell(
                    changed,
                    d.choice(PATCH_PHASES),
                    d.choice(spec_model.request_modes(changed)),
                    None,
                ),
                d.integer(0, 2**31 - 1),
                self._requests,
            )
            if patch.cell.phase == "ReadyToPromote":
                # The only strategy that stops there.
                strategy = "ManuallyPromote"
        upgrade = kube.env.endpoints.upgrade_pending_image
        # Not together with a patch: its `revert` restores the spec from
        # before this request, which would undo the upgrade after it
        # completed. The patch's own image changes never go back that far.
        upgrade_now = upgrade is not None and patch is None and d.boolean(0.5)
        image = upgrade if upgrade_now else None
        rollout = Rollout(
            request=request,
            strategy=strategy,
            started=time.monotonic(),
            kill_phase=(
                d.choice(KILL_PHASES) if d.boolean(self.kill_probability) else None
            ),
            kill_target=d.choice(("active", "candidate", "orchestratord")),
            grace=d.choice(GRACE_MENU),
            image=image,
            patch=patch,
        )

        def action() -> None:
            spec: dict[str, Any] = {"rolloutStrategy": strategy}
            if image is not None:
                spec["environmentdImageRef"] = image
            rollout.prior = dict((kube.env.materialize.get() or {}).get("spec") or {})
            kube.env.materialize.request_rollout(spec, request)
            with self._lock:
                self.rollout = rollout
            log(f"rollout {request} requested ({strategy}, image {image})")

        return self._kube("start rollout", action)

    def advance_rollout(self, d: Draw) -> Status:
        """Observe the CR and the rollout in progress: judge the status claim,
        promote the rollout, change its spec or kill a pod in a chosen phase."""
        kube = self.kube
        rollout = self.rollout
        if kube is None:
            return Status.NOT_SENT
        now = time.monotonic()
        with self._lock:
            # Without a rollout the CR changes rarely.
            if rollout is None and now - self._observed < OBSERVE_SECONDS:
                return Status.NOT_SENT
            self._observed = now

        def action() -> None:
            mz = kube.env.materialize.get()
            if mz is None:
                return
            status = mz.get("status") or {}
            spec = mz.get("spec") or {}
            active = status.get("activeGeneration")
            if isinstance(active, int):
                always(
                    active >= self.max_active_generation,
                    "Materialize status activeGeneration never decreases",
                    {"active": active, "max": self.max_active_generation},
                )
                self.max_active_generation = max(self.max_active_generation, active)
            self.claims.observe(kube.env.materialize, mz)
            if rollout is None:
                return
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
            completed = status.get("lastCompletedRolloutRequest") == rollout.request
            patch = rollout.patch
            if (
                patch is not None
                and not rollout.patched
                and phase == patch.cell.phase
                and (phase != "Applied" or completed)
            ):
                # A failed request leaves `patched` unset, and the retry sends
                # the same patch.
                self._patch_spec(kube, rollout, patch, mz)
                rollout.patched = True
                # The next observation follows the changed spec.
                return
            if completed and phase in SETTLED_PHASES:
                log(f"rollout {rollout.request} {phase} after {list(rollout.phases)}")
                if phase == "Applied":
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
                else:
                    reachable(
                        "driver: a rollout settled in WaitingForApproval after a spec change without a new request",
                        {"phases": list(rollout.phases)},
                    )
                self._finish(kube)
                return
            if rollout.kill_phase == phase and not rollout.killed:
                rollout.killed = True
                self._kill_for_rollout(kube, rollout, status)
            # Read from the spec: a patch may have changed the strategy or
            # the request since the rollout began.
            request = spec.get("requestRollout")
            if (
                spec.get("rolloutStrategy") == "ManuallyPromote"
                and phase == "ReadyToPromote"
                and spec.get("forcePromote") != request
            ):
                kube.env.materialize.patch_spec({"forcePromote": request})
                if not rollout.promoted:
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

    def _patch_spec(
        self, kube: Kube, rollout: Rollout, patch: SpecPatch, mz: dict[str, Any]
    ) -> None:
        """Change the spec of the rollout in progress, as `patch.cell` says."""
        endpoints = kube.env.endpoints
        status = mz.get("status") or {}
        spec = mz.get("spec") or {}
        rnd = random.Random(patch.seed)
        changes = spec_model.cell_changes(
            patch.cell,
            rnd,
            spec,
            rollout.prior,
            spec_model.Images(
                endpoints.initial_environmentd_image,
                endpoints.upgrade_pending_image,
                status.get("lastCompletedRolloutEnvironmentdImageRef"),
            ),
            lambda: str(uuid.UUID(int=(patch.serial << 64) | rnd.getrandbits(64))),
        )
        cell = patch.cell
        active = status.get("activeGeneration")
        # Before the request: one whose response is lost may still apply.
        if (
            cell.phase == "Promoting"
            and isinstance(active, int)
            and spec_model.changes_deployed_fields(spec, changes)
        ):
            self.claims.patched_while_promoting(active + 1, cell.request_mode)
        kube.env.materialize.patch_spec(changes)
        if "requestRollout" in changes:
            rollout.request = changes["requestRollout"]
        details = {"field": cell.field, "request_mode": cell.request_mode}
        log(f"rollout {rollout.request}: patched {cell.field} in {cell.phase}")
        if cell.phase == "Applying":
            reachable("driver: the spec was changed while the CR was Applying", details)
        elif cell.phase == "ReadyToPromote":
            reachable(
                "driver: the spec was changed while the CR was ReadyToPromote", details
            )
        elif cell.phase == "Promoting":
            reachable(
                "driver: the spec was changed while the CR was Promoting", details
            )
        else:
            reachable(
                "driver: the spec was changed after its rollout was Applied", details
            )

    def _finish(self, kube: Kube) -> None:
        spec = (kube.env.materialize.get() or {}).get("spec") or {}
        if spec.get("rolloutStrategy") not in (None, "WaitUntilReady"):
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
        settle_until: float | None = None
        while time.monotonic() < deadline:
            try:
                mz = kube.env.materialize
                cr = mz.get() or {}
                spec = cr.get("spec") or {}
                status = cr.get("status") or {}
                request = spec.get("requestRollout")
                if spec.get("rolloutStrategy") not in (None, "WaitUntilReady"):
                    mz.patch_spec({"rolloutStrategy": "WaitUntilReady"})
                phase = mz.phase()
                if phase == "ReadyToPromote":
                    mz.patch_spec({"forcePromote": request})
                # A spec change without a request waits for one.
                if (
                    phase == "WaitingForApproval"
                    and status.get("lastCompletedRolloutRequest") == request
                ):
                    self._requests += 1
                    mz.request_rollout(request_id=str(uuid.UUID(int=self._requests)))
                if cr:
                    self.claims.observe(mz, cr)
                if mz.is_up_to_date():
                    up_to_date = True
                    # A violated claim is asserted by a second observation.
                    if settle_until is None:
                        settle_until = time.monotonic() + 2 * STATUS_SETTLE_SECONDS
                    if not self.claims.pending or time.monotonic() >= settle_until:
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

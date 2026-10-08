# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Spec field by rollout phase, and what the status claims afterwards.

Two commands share this module:

- `driver_main` (`parallel_driver_spec_phase`) draws one cell
  (`spec_model.Cell`): a spec field, a rollout phase, a request mode, and an
  optional pod kill. It drives the CR into the phase with a setup patch,
  watches the CR so the patch lands on the version that shows the phase, then
  follows the CR until it settles and judges the status claim against the
  active StatefulSet. It writes the spec only while holding the rollouts
  driver's CR lease, so the workload stays the single spec writer.
- `eventually_main` (`eventually_spec_phase_settles`) checks that, once faults
  stop, the CR settles and its status claim holds.

The status-claim assertions live in `rollouts.assert_status_claim`, shared
with the observer. Each known condition gets its own message there, so a new
bug in any other cell is reported separately.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable
from typing import Any

from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    always_or_unreachable,
    sometimes,
)

from materialize import orchestratord
from materialize.antithesis import kube_watch, spec_model
from materialize.antithesis.drivers import rollouts
from materialize.antithesis.drivers.rollouts import (
    TRANSIENT_ERRORS,
    CrLease,
    Ctx,
    Kube,
    LeaseLost,
    Snapshot,
)
from materialize.antithesis.environment import Environment
from materialize.antithesis.rng import rng

# Server-side timeout of one phase-wait watch window. The window ends as soon
# as the awaited version arrives, so this only bounds how long the lease goes
# unrenewed while nothing changes. Must stay well below
# `rollouts.CR_LEASE_RENEW_SECONDS`.
PHASE_WATCH_WINDOW_SECONDS = 5
# How long the driver waits for the CR lease before giving up.
LEASE_WAIT_SECONDS = 60.0
# Delays between seeing an in-progress phase and patching. Zero patches on the
# watch event that showed the phase.
ACT_DELAY_MENU = (0, 0, 0, 1, 5)
# Reasons that end a request once they record it as completed.
# `FailedDeploy` ends it without recording it: orchestratord retries a failed
# apply, but a request refused by the upgrade window check stays refused until
# the spec changes.
SETTLED_REASONS = ("Applied", "WaitingForApproval", "RolloutTimeout")

MSG_LIVENESS = (
    "A requested rollout reaches Applied or a terminal failure after faults stop"
)
MSG_LIVENESS_PROMOTED_ACROSS = "A requested rollout reaches Applied or a terminal failure after faults stop, for a generation promoted across a spec change during Promoting (CLO-339)"
MSG_NOT_PROMOTING = "No rollout stays Promoting after faults stop"


def log(message: str) -> None:
    print(f"spec_phase: {message}", flush=True)


def settled(snap: Snapshot, min_generation: int | None) -> bool:
    """Whether a pass that saw the current spec, of generation `min_generation`
    or later, ended the requested rollout, by completing it or failing it."""
    observed = snap.observed_generation
    if observed is None or observed != snap.generation:
        return False
    if min_generation is not None and observed < min_generation:
        return False
    if snap.reason == "FailedDeploy":
        return True
    return snap.reason in SETTLED_REASONS and snap.last_completed == snap.request


def cr_watch(kube: Kube) -> kube_watch.ResourceWatch:
    return kube_watch.ResourceWatch(
        kube.custom.list_namespaced_custom_object,
        orchestratord.GROUP,
        orchestratord.VERSION,
        kube.namespace,
        orchestratord.PLURAL,
        field_selector=f"metadata.name={kube.name}",
        window_seconds=PHASE_WATCH_WINDOW_SECONDS,
        request_timeout=rollouts.K8S_TIMEOUT_SECONDS,
    )


def watch_until(
    ctx: Ctx, predicate: Callable[[Snapshot], bool], timeout: float
) -> Snapshot | None:
    """The first CR version, current or later, for which `predicate` holds, or
    None after `timeout`. Every stored version is seen, unless a relist skips
    some, so a phase that lasts a single version is still caught."""
    watch = cr_watch(ctx.kube)
    found: list[Snapshot] = []

    def until(item: kube_watch.Item) -> bool:
        if isinstance(item, kube_watch.Relist):
            objs = item.items[:1]
        else:
            objs = [] if item.type == "DELETED" else [item.obj]
        for obj in objs:
            snap = Snapshot(obj)
            if predicate(snap):
                found.append(snap)
                return True
        return False

    rv: str | None = None
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if ctx.lease is not None:
            ctx.lease.renew()
        window = watch.window(rv, until)
        if window.error is not None:
            log(f"CR watch: {window.error}")
        if found:
            return found[0]
        rv = window.resource_version
    return None


def phase_predicate(
    phase: str, request: str | None, min_generation: int | None
) -> Callable[[Snapshot], bool]:
    """Holds on the version that shows `phase` for the setup patch, or on one
    that shows the phase can no longer come."""

    def predicate(snap: Snapshot) -> bool:
        if phase in spec_model.IN_PROGRESS_PHASES:
            if snap.request != request:
                return True
            if snap.reason == phase:
                return True
        return settled(snap, min_generation)

    return predicate


def setup(ctx: Ctx, cell: spec_model.Cell) -> tuple[Snapshot, Snapshot] | None:
    """Drives the CR into the cell's phase. Returns the CR before the setup
    patch and the version where the phase was seen, which may show another
    phase if the setup missed it."""
    prior = ctx.kube.try_snapshot()
    if prior is None:
        return None
    if cell.phase == "Applied" and prior.reason == "Applied" and settled(prior, None):
        return prior, prior
    changes: dict[str, Any]
    if cell.phase == "WaitingForApproval":
        changes = {"forceRollout": rollouts.new_uuid()}
    elif cell.phase == "ReadyToPromote":
        changes = {**rollouts.new_request(), "rolloutStrategy": "ManuallyPromote"}
    else:
        changes = rollouts.new_request()
    request, stored = rollouts.submit_patch(
        ctx, f"spec_phase_setup_{cell.phase}", changes
    )
    if stored is None:
        return None
    reached = watch_until(
        ctx,
        phase_predicate(cell.phase, request, stored.generation),
        rollouts.FOLLOW_TIMEOUT_SECONDS,
    )
    if reached is None:
        return None
    return prior, reached


def follow_until_settled(
    ctx: Ctx, min_generation: int | None, timeout: float
) -> tuple[Snapshot | None, list[str]]:
    """Polls until `settled`, force-promoting a `ManuallyPromote` rollout that
    reaches `ReadyToPromote`. Returns the settled version, or None on
    timeout, and the reasons seen."""
    phases: list[str] = []
    promoted: set[str] = set()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        snap = ctx.poll()
        if snap is not None:
            if snap.reason and (not phases or phases[-1] != snap.reason):
                phases.append(snap.reason)
            if (
                snap.reason == "ReadyToPromote"
                and snap.spec.get("rolloutStrategy") == "ManuallyPromote"
                and snap.request is not None
                and snap.spec.get("forcePromote") != snap.request
                and snap.request not in promoted
            ):
                rollouts.submit(
                    ctx, "spec_phase_promote", {"forcePromote": snap.request}
                )
                promoted.add(snap.request)
            if settled(snap, min_generation):
                return snap, phases
        time.sleep(rollouts.POLL_SECONDS)
    return None, phases


def judge_claim(
    ctx: Ctx,
    observer: sqlite3.Connection,
    promoting: tuple[int, str] | None,
    details: dict[str, Any],
) -> None:
    """Judges the current status claim, asserting a violation only if it is
    still there under the same key `rollouts.STATUS_SETTLE_SECONDS` later.

    `promoting` is the active generation and request mode of a patch that
    changed a rendered field during `Promoting`. It names the cause only for
    the generation that promotion produced.
    """

    def cause(claim: rollouts.EvaluatedClaim) -> str:
        mode = None
        if promoting is not None and claim.snap.active == promoting[0] + 1:
            mode = promoting[1]
        return spec_model.claim_cause(
            claim.check,
            promoting_request_mode=mode,
            promoted_across=claim.snap.active
            in rollouts.promoted_across_spec_change(observer),
        )

    first = rollouts.read_claim(ctx.kube)
    if first is None:
        return
    if not first.check.violated:
        rollouts.assert_status_claim(cause(first), True, {**first.details(), **details})
        return
    log(f"status claim violated, re-checking: {first.check.mismatches}")
    ctx.sleep(rollouts.STATUS_SETTLE_SECONDS)
    second = rollouts.read_claim(ctx.kube)
    if second is None or second.check.key != first.check.key:
        return
    rollouts.assert_status_claim(
        cause(second),
        not second.check.violated,
        {**second.details(), **details, "first_mismatches": first.check.mismatches},
    )


def note_coverage(cell: spec_model.Cell, patched_in: str | None) -> None:
    details = {"cell": cell.key(), "patched_in": patched_in}
    for field, phase in spec_model.all_cells():
        sometimes(
            field == cell.field and phase == patched_in,
            f"Spec-phase driver patched {field} while the CR was {phase}",
            details,
        )
    sometimes(
        patched_in == cell.phase,
        "Spec-phase driver patched in the phase it aimed for",
        details,
    )


def run_cell(ctx: Ctx, cell: spec_model.Cell) -> None:
    started = setup(ctx, cell)
    if started is None:
        log(f"{cell}: setup did not reach a usable state")
        return
    prior, reached = started
    if cell.phase in spec_model.IN_PROGRESS_PHASES and reached.reason == cell.phase:
        ctx.sleep(rng.choice(ACT_DELAY_MENU))
    current = ctx.kube.try_snapshot() or reached
    endpoints = ctx.env.endpoints
    images = spec_model.Images(
        initial=endpoints.initial_environmentd_image,
        target=endpoints.upgrade_pending_image,
        last_completed=current.last_completed_image,
    )
    changes = spec_model.cell_changes(
        cell, rng, current.spec, prior.spec, images, rollouts.new_uuid
    )
    _, stored = rollouts.submit_patch(
        ctx, f"spec_phase_{cell.field}_{cell.request_mode}", changes
    )
    if stored is None:
        return
    # A spec patch leaves the status alone, so the stored status is the one
    # the patch landed on.
    patched_in = stored.reason
    note_coverage(cell, patched_in)
    promoting = None
    if (
        patched_in == "Promoting"
        and stored.active is not None
        and spec_model.changes_deployed_fields(current.spec, changes)
    ):
        promoting = (stored.active, cell.request_mode)
    if cell.kill is not None and patched_in is not None:
        plan = rollouts.KillPlan(patched_in, cell.kill, rng.choice(rollouts.GRACE_MENU))
        try:
            rollouts.execute_kill(ctx, plan, stored)
        except TRANSIENT_ERRORS as e:
            log(f"kill failed: {e}")
    final, phases = follow_until_settled(
        ctx, stored.generation, rollouts.FOLLOW_TIMEOUT_SECONDS
    )
    details = {
        "cell": cell.key(),
        "request_mode": cell.request_mode,
        "kill": cell.kill,
        "patched_in": patched_in,
        "changes": changes,
        "phases_after_patch": phases,
    }
    sometimes(
        final is not None and final.reason == "Applied",
        "Spec-phase cell settled at Applied",
        details,
    )
    if final is None:
        log(f"{cell}: did not settle; phases {phases}")
        return
    judge_claim(ctx, rollouts.observer_db(ctx.env), promoting, details)


def acquire_lease(lease: CrLease) -> bool:
    deadline = time.monotonic() + LEASE_WAIT_SECONDS
    while True:
        try:
            if lease.acquire():
                return True
        except sqlite3.OperationalError as e:
            log(f"taking the CR lease failed: {e}")
        if time.monotonic() >= deadline:
            return False
        time.sleep(5)


def driver_main() -> int:
    env = Environment()
    kube = Kube(env)
    db = rollouts.requests_db(env)
    ctx = Ctx(env, kube, db, rollouts.timeline_params(db))
    lease = CrLease(env)
    if not acquire_lease(lease):
        log("another invocation holds the CR lease")
        return 0
    ctx.lease = lease
    try:
        rollouts.normalize(ctx, "spec_phase_normalize")
        # A cell starts from a settled CR, so its setup patch is the only
        # rollout in flight.
        snap = kube.try_snapshot()
        if snap is not None and not settled(snap, None):
            snap, _ = follow_until_settled(
                ctx, snap.generation, rollouts.PHASE_WAIT_SECONDS
            )
            if snap is None:
                log("the CR did not settle; skipping")
                return 0
        cell = spec_model.choose_cell(rng)
        log(f"cell {cell}")
        try:
            run_cell(ctx, cell)
        finally:
            rollouts.normalize(ctx, "spec_phase_restore")
    except LeaseLost as e:
        log(f"stopping: {e}")
    except TRANSIENT_ERRORS as e:
        log(f"interrupted: {e}")
    finally:
        try:
            lease.release()
        except sqlite3.Error as e:
            log(f"releasing the CR lease failed: {e}")
    return 0


def eventually_main() -> int:
    env = Environment()
    ctx = rollouts.operator_ctx(env)
    kube = ctx.kube
    observer = rollouts.observer_db(env)
    watchers: list[rollouts.CrWatcher | rollouts.StatefulSetWatcher] = [
        rollouts.CrWatcher(kube, observer),
        rollouts.StatefulSetWatcher(kube, observer),
    ]
    start = time.monotonic()
    deadline = start + rollouts.CONVERGENCE_WEDGED_SECONDS
    normalized = False
    last: Snapshot | None = None
    final: Snapshot | None = None
    while time.monotonic() < deadline:
        try:
            if not normalized:
                # A strategy or timeout override left by a killed driver
                # would park the rollout legitimately.
                rollouts.normalize(ctx, "spec_phase_eventually_normalize")
                normalized = True
            last = kube.snapshot()
            if settled(last, None):
                final = last
                break
        except TRANSIENT_ERRORS as e:
            log(f"reading the CR failed: {e}")
        # Waits by watching, so the observer checks every version on the way.
        retry_at = time.monotonic() + 5
        while time.monotonic() < retry_at:
            rollouts.run_watches(watchers)
    promoted_across = last is not None and last.active in (
        rollouts.promoted_across_spec_change(observer)
    )
    details = {
        "elapsed_seconds": time.monotonic() - start,
        "wedged_bound_seconds": rollouts.CONVERGENCE_WEDGED_SECONDS,
        "last": last.summary() if last else None,
        "spec": last.tracked_spec() if last else None,
        "promoted_across_spec_change": promoted_across,
    }
    if promoted_across:
        always_or_unreachable(final is not None, MSG_LIVENESS_PROMOTED_ACROSS, details)
    else:
        always(final is not None, MSG_LIVENESS, details)
    always(last is None or last.reason != "Promoting", MSG_NOT_PROMOTING, details)
    sometimes(
        final is not None and final.reason == "Applied",
        "Spec-phase eventually check found the CR Applied after faults stop",
        details,
    )
    if final is not None:
        try:
            judge_claim(ctx, observer, None, {"origin": "eventually"})
        except TRANSIENT_ERRORS as e:
            log(f"judging the status claim failed: {e}")
    return 0

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Resumable rollout scenarios, with one bounded step per command invocation."""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any

from materialize.antithesis import ddl_hold
from materialize.antithesis.drivers import rollouts as r

OBSERVE_SECONDS = 5.0
SCENARIO_SECONDS = 6 * r.FOLLOW_TIMEOUT_SECONDS


@dataclass
class Scenario:
    """The active action's replayable intent and absolute observation deadlines."""

    scenario_id: str
    action: str
    prior: dict[str, Any]
    created: float
    hold: bool
    owner: str = "rollouts"
    data: dict[str, Any] = field(default_factory=dict)
    stage: str = "start"
    request: str | None = None
    deadline: float = 0.0
    intent: dict[str, Any] | None = None
    intent_action: str = ""
    intent_attempted: bool = False
    intent_request: str | None = None
    from_request: str | None = None
    after_patch: str = ""
    mode: str = ""
    variant: str = ""
    wait_for: list[str] = field(default_factory=list)
    delay_until: float = 0.0
    parked: bool = False
    retry: bool = False
    kill: dict[str, Any] | None = None
    kill_attempted: bool = False
    killed_phase: str | None = None
    phases: list[str] = field(default_factory=list)
    outcome: str | None = None


def initialize(db: sqlite3.Connection) -> None:
    """Create the bounded scenario journal and aggregate outcome counters."""
    db.execute(
        "CREATE TABLE IF NOT EXISTS rollout_scenario ("
        " slot INTEGER PRIMARY KEY CHECK (slot = 0), state TEXT NOT NULL)"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS rollout_outcomes ("
        " action TEXT NOT NULL, outcome TEXT NOT NULL, count INTEGER NOT NULL,"
        " PRIMARY KEY (action, outcome))"
    )
    db.commit()


def load(db: sqlite3.Connection) -> Scenario | None:
    """Read the active scenario, which callers serialize with the CR lease."""
    initialize(db)
    row = db.execute("SELECT state FROM rollout_scenario WHERE slot = 0").fetchone()
    return Scenario(**json.loads(row[0])) if row is not None else None


def save(db: sqlite3.Connection, s: Scenario) -> None:
    """Commit the active scenario before issuing its next external effect."""
    with db:
        db.execute(
            "INSERT INTO rollout_scenario VALUES (0, ?)"
            " ON CONFLICT (slot) DO UPDATE SET state = excluded.state",
            (json.dumps(asdict(s)),),
        )


def begin(ctx: r.Ctx, action: str | None = None) -> Scenario | None:
    """Record one selected action without changing the Materialize spec."""
    initialize(ctx.db)
    prior = ctx.kube.try_snapshot()
    if prior is None:
        return None
    if action is None:
        names = [a for a in r.ACTIONS if ctx.params.weights.get(a)]
        action = r.rng.choices(names, weights=[ctx.params.weights[a] for a in names])[0]
    s = Scenario(
        r.new_uuid(),
        action,
        prior.obj,
        time.monotonic(),
        r.rng.random() < ctx.params.ddl_hold_probability,
    )
    if action == "paced_ddl":
        s.stage = "paced"
        s.hold = False
    save(ctx.db, s)
    _hold(s)
    return s


def _hold(s: Scenario) -> None:
    if s.hold:
        ddl_hold.request_until(
            f"rollouts:{s.scenario_id}", s.created + SCENARIO_SECONDS
        )


def _queue(
    ctx: r.Ctx,
    s: Scenario,
    snap: r.Snapshot,
    label: str,
    changes: dict[str, Any],
    after: str,
) -> None:
    s.stage = "patch"
    s.intent = changes
    s.intent_action = label
    s.intent_attempted = False
    s.from_request = snap.request
    s.intent_request = changes.get("requestRollout", snap.request)
    s.after_patch = after
    s.deadline = time.monotonic() + r.FOLLOW_TIMEOUT_SECONDS
    # Persist the exact intent before the API call. A command killed after
    # the call cannot draw a different request when its response was lost.
    save(ctx.db, s)


def _patch(
    ctx: r.Ctx,
    s: Scenario,
    on_stored: Callable[[r.Snapshot, bool], None] | None = None,
) -> str:
    assert s.intent is not None
    snap = ctx.poll()
    if snap is None:
        return "unavailable"
    if snap.request not in (s.from_request, s.intent_request):
        s.outcome = "Superseded"
        _complete(ctx, s)
        return "superseded"
    matches = snap.request == s.intent_request and all(
        snap.spec.get(key) == value for key, value in s.intent.items()
    )
    acknowledged = False
    if not matches or not s.intent_attempted:
        if time.monotonic() >= s.deadline:
            s.outcome = "Timeout"
            s.stage = "cleanup" if s.after_patch != "done" else "done"
            save(ctx.db, s)
            return "timeout"
        s.intent_attempted = True
        save(ctx.db, s)
        _, stored = r.submit_patch(ctx, s.intent_action, s.intent)
        if stored is None:
            return "unacknowledged"
        snap = stored
        acknowledged = True
        _note_patch_phase(s.intent_action, stored)
    s.request = snap.request
    s.intent = None
    s.stage = s.after_patch
    s.deadline = time.monotonic() + (
        r.PHASE_WAIT_SECONDS
        if s.wait_for == ["Applying"] and s.stage == "wait"
        else r.FOLLOW_TIMEOUT_SECONDS
    )
    if s.stage == "follow":
        plan = r.maybe_kill_plan(ctx)
        s.kill = asdict(plan) if plan is not None else None
        s.kill_attempted = False
        s.killed_phase = None
        s.phases = []
    if on_stored is not None:
        on_stored(snap, acknowledged)
    save(ctx.db, s)
    return "patched"


def _note_patch_phase(action: str, stored: r.Snapshot) -> None:
    # A spec patch leaves status unchanged. Only its returned version proves
    # the phase at which it landed, including when the planning read was stale.
    if action == "supersede_second":
        r.sometimes(
            stored.reason == "Applying",
            "Rollout driver superseded a rollout while it was Applying",
            stored.summary(),
        )
    elif action.startswith("revert_"):
        r.sometimes(
            stored.reason == "Applying",
            "Rollout driver reverted a rollout while it was Applying",
            stored.summary(),
        )
    elif action.startswith("promoting_"):
        r.sometimes(
            stored.reason == "Promoting",
            "Rollout driver patched the spec while the rollout was Promoting",
            stored.summary(),
        )


def _start(ctx: r.Ctx, s: Scenario) -> str:
    prior = r.Snapshot(s.prior)
    changes = r.new_request()
    after = "follow"
    label = s.action
    if s.action == "forced_migration":
        changes["environmentdExtraArgs"] = r.toggled_migration_args(prior.spec)
    elif s.action in ("supersede", "revert", "patch_during_promoting"):
        changes.update(r.maybe_toggle(prior))
        s.wait_for = [
            "Promoting" if s.action == "patch_during_promoting" else "Applying"
        ]
        after = "wait"
    elif s.action == "force_promote":
        if r.rng.random() < 0.5:
            changes["forcePromote"] = changes["requestRollout"]
            label = "force_promote_same_patch"
        else:
            s.wait_for = ["Applying"]
            after = "wait"
            label = "force_promote_forward"
    elif s.action == "manual_promote":
        changes["rolloutStrategy"] = "ManuallyPromote"
        s.wait_for = ["ReadyToPromote"]
        after = "wait"
    elif s.action == "short_timeout":
        s.mode = r.rng.choice(r.timeout_menu())
        changes["rolloutRequestTimeout"] = s.mode
    elif s.action == "upgrade":
        target = ctx.env.endpoints.upgrade_pending_image
        if target is None or prior.spec.get("environmentdImageRef") == target:
            s.mode = "same_version"
        else:
            changes["environmentdImageRef"] = target
            s.mode = "cancel" if r.rng.random() < 0.3 else "upgrade"
            if s.mode == "cancel":
                changes["rolloutStrategy"] = "ManuallyPromote"
                s.wait_for = ["ReadyToPromote"]
                after = "wait"
                label = "upgrade_manual"
    _queue(ctx, s, prior, label, changes, after)
    return "planned"


def _wait(ctx: r.Ctx, s: Scenario) -> str:
    snap = r.wait_for_phase(
        ctx,
        s.request,
        tuple(s.wait_for),
        min(OBSERVE_SECONDS, max(0.0, s.deadline - time.monotonic())),
    )
    if snap is None:
        return "unavailable"
    reached = snap.request == s.request and snap.reason in s.wait_for
    terminal = (
        snap.last_completed == s.request and snap.reason in r.TERMINAL_FOR_REQUEST
    )
    if not (
        reached
        or terminal
        or snap.request != s.request
        or time.monotonic() >= s.deadline
    ):
        return "waiting"
    if snap.request != s.request:
        s.outcome = "Superseded"
        s.stage = "check"
        save(ctx.db, s)
        return "superseded"
    s.parked = reached
    if s.action in ("manual_promote", "upgrade"):
        delay = r.rng.choice(r.MANUAL_WAIT_MENU) if reached else 0
    elif s.action == "patch_during_promoting":
        delay = 0
    else:
        delay = r.rng.choice(r.ACT_DELAY_MENU)
    s.delay_until = time.monotonic() + delay
    s.stage = "act"
    save(ctx.db, s)
    # A zero-delay phase-targeted patch must use the phase just observed.
    # Returning to the scheduler first can lose a short Promoting window.
    return _act(ctx, s, snap) if delay == 0 else "delayed"


def _act(ctx: r.Ctx, s: Scenario, snap: r.Snapshot | None = None) -> str:
    if time.monotonic() < s.delay_until:
        return "delayed"
    snap = snap or ctx.poll()
    if snap is None:
        return "unavailable"
    if snap.request != s.request:
        s.outcome = "Superseded"
        s.stage = "check"
        save(ctx.db, s)
        return "superseded"
    prior = r.Snapshot(s.prior)
    changes: dict[str, Any]
    if s.action == "supersede":
        changes = {**r.new_request(), **r.maybe_toggle(snap)}
        label = "supersede_second"
    elif s.action == "revert":
        changes = {
            "forceRollout": prior.spec.get("forceRollout"),
            "environmentdExtraArgs": prior.spec.get("environmentdExtraArgs"),
        }
        s.variant = r.rng.choice(["restore_request", "keep_request", "fresh_request"])
        if s.variant == "restore_request" and prior.request is not None:
            changes["requestRollout"] = prior.request
        elif s.variant == "fresh_request":
            changes["requestRollout"] = r.new_uuid()
        label = f"revert_{s.variant}"
    elif s.action == "patch_during_promoting":
        s.variant = r.rng.choice(["new_request", "same_request", "revert"])
        if s.variant == "new_request":
            changes = {
                **r.new_request(),
                "environmentdExtraArgs": r.toggled_migration_args(snap.spec),
            }
        elif s.variant == "same_request":
            changes = r.rng.choice(
                [
                    {"forceRollout": r.new_uuid()},
                    {"environmentdExtraArgs": r.toggled_migration_args(snap.spec)},
                ]
            )
        else:
            changes = {
                "forceRollout": prior.spec.get("forceRollout"),
                "environmentdExtraArgs": prior.spec.get("environmentdExtraArgs"),
            }
        label = f"promoting_{s.variant}"
    elif s.action == "upgrade":
        if s.parked:
            r.reachable(
                "Cross-version upgrade candidate reached ReadyToPromote",
                {"from": prior.spec.get("environmentdImageRef")},
            )
        s.variant = r.rng.choice(["restore_request", "fresh_request"])
        changes = {
            "environmentdImageRef": prior.spec.get("environmentdImageRef"),
            "forceRollout": prior.spec.get("forceRollout"),
            "rolloutStrategy": r.DEFAULT_ROLLOUT_STRATEGY,
            "requestRollout": (
                prior.request
                if s.variant == "restore_request" and prior.request is not None
                else r.new_uuid()
            ),
        }
        label = f"upgrade_cancel_{s.variant}"
    else:
        changes = {"forcePromote": s.request}
        label = (
            "manual_promote_approve"
            if s.action == "manual_promote"
            else "force_promote_later"
        )
    _queue(ctx, s, snap, label, changes, "follow")
    return _patch(ctx, s)


def _follow(ctx: r.Ctx, s: Scenario) -> str:
    end = min(s.deadline, time.monotonic() + OBSERVE_SECONDS)
    while time.monotonic() < end:
        snap = ctx.poll()
        if snap is not None:
            if snap.request != s.request:
                s.outcome = "Superseded"
            elif (
                snap.last_completed == s.request
                and snap.reason in r.TERMINAL_FOR_REQUEST
            ):
                s.outcome = snap.reason
            if snap.reason and (not s.phases or s.phases[-1] != snap.reason):
                s.phases.append(snap.reason)
            if s.outcome is not None:
                s.stage = "check"
                save(ctx.db, s)
                return "settled"
            if s.kill is not None and not s.kill_attempted:
                plan = r.KillPlan(**s.kill)
                if r.kill_due(ctx, plan, snap):

                    def before_delete() -> None:
                        s.kill_attempted = True
                        save(ctx.db, s)

                    if r.execute_kill(ctx, plan, snap, before_delete=before_delete):
                        s.killed_phase = snap.reason
                    save(ctx.db, s)
        time.sleep(min(r.POLL_SECONDS, max(0.0, end - time.monotonic())))
    if time.monotonic() >= s.deadline:
        s.outcome = "Timeout"
        s.stage = "check"
    save(ctx.db, s)
    return "observed"


def _check(ctx: r.Ctx, s: Scenario) -> str:
    observer = r.observer_db(ctx.env)
    try:
        r.observe_once(observer, ctx.kube)
    finally:
        observer.close()
    result = r.Followed(s.phases, s.outcome, s.killed_phase)
    r.note_kill_outcome(result)
    details = {"phases": s.phases, "outcome": s.outcome}
    if s.outcome == "Applied":
        if s.action == "same_version" or s.mode == "same_version":
            r.reachable("Same-version deploy reached Applied", details)
        elif s.action == "forced_migration":
            args = ctx.kube.snapshot().spec.get("environmentdExtraArgs")
            if args:
                r.reachable(
                    "Deploy with forced builtin schema migration reached Applied",
                    {**details, "args": args},
                )
        elif s.action == "force_promote":
            r.reachable("Force-promoted rollout reached Applied", details)
        elif s.action == "manual_promote" and s.parked:
            r.reachable(
                "ManuallyPromote rollout reached Applied after a force promote from ReadyToPromote",
                details,
            )
        elif s.action == "upgrade" and s.mode == "upgrade":
            r.reachable(
                "Cross-version upgrade reached Applied",
                {
                    **details,
                    "from": r.Snapshot(s.prior).spec.get("environmentdImageRef"),
                },
            )
    if s.action == "upgrade" and s.mode == "cancel":
        after = ctx.kube.try_snapshot()
        prior = r.Snapshot(s.prior)
        if (
            after is not None
            and after.request == s.request
            and after.last_completed == s.request
            and after.last_completed_image is not None
        ):
            r.always(
                after.last_completed_image == prior.spec.get("environmentdImageRef")
                and (s.variant == "fresh_request" or after.active == prior.active),
                "A cancelled cross-version upgrade leaves the old release serving",
                {
                    **details,
                    "prior_image": prior.spec.get("environmentdImageRef"),
                    "prior_active": prior.active,
                    "variant": s.variant,
                    "parked": s.parked,
                    **after.summary(),
                },
            )
            r.reachable(
                "Cross-version upgrade cancelled before promotion",
                {"variant": s.variant, "parked": s.parked},
            )
    if s.action == "short_timeout" and s.retry:
        r.sometimes(
            s.outcome == "Applied",
            "Rollout reached Applied after a RolloutTimeout and a fresh request",
            details,
        )
    s.stage = "cleanup"
    save(ctx.db, s)
    return "checked"


def _cleanup(ctx: r.Ctx, s: Scenario) -> str:
    snap = ctx.poll()
    if snap is None:
        return "unavailable"
    changes = {}
    if s.action == "manual_promote" or (s.action == "upgrade" and s.mode == "cancel"):
        changes["rolloutStrategy"] = r.DEFAULT_ROLLOUT_STRATEGY
    if s.action == "short_timeout":
        changes["rolloutRequestTimeout"] = r.DEFAULT_ROLLOUT_REQUEST_TIMEOUT
    retry = (
        s.action == "short_timeout"
        and not s.retry
        and s.outcome in ("RolloutTimeout", "WaitingForApproval")
    )
    if retry:
        r.reachable(
            "Short rolloutRequestTimeout cancelled a rollout",
            {"timeout": s.mode, "phases": s.phases},
        )
    after = "retry" if retry else "done"
    if changes:
        _queue(ctx, s, snap, f"{s.action}_restore", changes, after)
        return "restoring"
    s.stage = after
    save(ctx.db, s)
    return "restored"


def _complete(ctx: r.Ctx, s: Scenario) -> None:
    if s.hold:
        # Release before forgetting the owner. A killed completion can retry
        # this release instead of leaving an undiscoverable long hold.
        ddl_hold.release(f"rollouts:{s.scenario_id}")
    with ctx.db:
        ctx.db.execute(
            "INSERT INTO rollout_outcomes VALUES (?, ?, 1)"
            " ON CONFLICT (action, outcome) DO UPDATE SET count = count + 1",
            (s.action, s.outcome or "Timeout"),
        )
        ctx.db.execute("DELETE FROM rollout_scenario WHERE slot = 0")


def finish_paced(ctx: r.Ctx, s: Scenario) -> None:
    """Complete the paced trial claimed by its dedicated command."""
    assert s.action == "paced_ddl" and s.stage == "paced"
    s.outcome = "Completed"
    _complete(ctx, s)


def step(ctx: r.Ctx) -> str:
    """Advance one persisted intent or a bounded observation window."""
    s = load(ctx.db)
    if s is not None and s.owner != "rollouts":
        return "other_command"
    if s is None:
        if not r.normalize(ctx, "normalize"):
            return "unavailable"
        s = begin(ctx)
        if s is None:
            return "unavailable"
    if time.monotonic() - s.created >= SCENARIO_SECONDS:
        s.outcome = "Timeout"
        _complete(ctx, s)
        return "expired"
    if s.stage == "paced":
        return "paced_pending"
    _hold(s)
    r.log(f"step {s.action}:{s.stage}")
    if s.stage == "start":
        return _start(ctx, s)
    if s.stage == "patch":
        return _patch(ctx, s)
    if s.stage == "wait":
        return _wait(ctx, s)
    if s.stage == "act":
        return _act(ctx, s)
    if s.stage == "follow":
        return _follow(ctx, s)
    if s.stage == "check":
        return _check(ctx, s)
    if s.stage == "cleanup":
        return _cleanup(ctx, s)
    if s.stage == "retry":
        snap = ctx.poll()
        if snap is None:
            return "unavailable"
        s.retry = True
        s.outcome = None
        _queue(ctx, s, snap, "short_timeout_retry", r.new_request(), "follow")
        return "retrying"
    if s.stage == "done":
        _complete(ctx, s)
        return "done"
    raise ValueError(f"unrecognized rollout step {s.stage}")

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Durable spec-phase trials sharing the ordinary rollout scenario journal."""

from __future__ import annotations

import time
from dataclasses import asdict
from typing import Any

from materialize.antithesis import kube_watch, spec_model
from materialize.antithesis import rollout_steps as journal
from materialize.antithesis.drivers import rollouts as r
from materialize.antithesis.drivers import spec_phase as sp


def _cell(s: journal.Scenario) -> spec_model.Cell:
    return spec_model.Cell(**s.data["cell"])


def _queue(
    ctx: r.Ctx,
    s: journal.Scenario,
    snap: r.Snapshot,
    label: str,
    changes: dict[str, Any],
    after: str,
    role: str,
) -> str:
    s.data["patch_role"] = role
    journal._queue(ctx, s, snap, label, changes, after)
    return _patch(ctx, s)


def _patch(ctx: r.Ctx, s: journal.Scenario) -> str:
    role = s.data["patch_role"]

    def stored(snap: r.Snapshot, acknowledged: bool) -> None:
        if role == "setup":
            s.data["setup_generation"] = snap.generation
            s.data["resource_version"] = snap.resource_version
        elif role == "cell":
            s.data["generation"] = snap.generation
            s.data["follow_deadline"] = s.deadline
            s.phases = []
            if acknowledged:
                cell = _cell(s)
                s.data["patched_in"] = snap.reason
                s.data["stored"] = snap.obj
                sp.note_coverage(cell, snap.reason)
                if (
                    snap.reason == "Promoting"
                    and snap.active is not None
                    and spec_model.changes_deployed_fields(
                        s.data["before_cell_spec"], s.data["changes"]
                    )
                ):
                    s.data["promoting"] = [snap.active, cell.request_mode]
                if cell.kill is not None and snap.reason is not None:
                    s.kill = asdict(
                        r.KillPlan(snap.reason, cell.kill, r.rng.choice(r.GRACE_MENU))
                    )
        elif role == "promote":
            s.data["generation"] = snap.generation
            s.deadline = s.data["follow_deadline"]

    result = journal._patch(ctx, s, on_stored=stored)
    if result == "patched" and role == "cell":
        _kill(ctx, s)
    return result


def _begin(ctx: r.Ctx) -> journal.Scenario | None:
    snap = ctx.poll()
    if snap is None:
        return None
    s = journal.Scenario(
        r.new_uuid(),
        "spec_phase",
        snap.obj,
        time.monotonic(),
        False,
        owner="spec_phase",
        stage="normalize",
    )
    journal.save(ctx.db, s)
    return s


def _before(ctx: r.Ctx, s: journal.Scenario) -> str:
    snap = ctx.poll()
    if snap is None:
        return "unavailable"
    if not sp.settled(snap, None):
        if time.monotonic() >= s.deadline:
            s.outcome = "SetupTimeout"
            s.stage = "cleanup"
            journal.save(ctx.db, s)
        return "settling"
    s.prior = snap.obj
    s.data["cell"] = asdict(spec_model.choose_cell(r.rng))
    s.stage = "setup"
    journal.save(ctx.db, s)
    return "selected"


def _setup(ctx: r.Ctx, s: journal.Scenario) -> str:
    prior = r.Snapshot(s.prior)
    cell = _cell(s)
    if (
        cell.phase == "Applied"
        and prior.reason == "Applied"
        and sp.settled(prior, None)
    ):
        s.stage = "act_cell"
        s.data["reached"] = prior.obj
        journal.save(ctx.db, s)
        return _act(ctx, s)
    if cell.phase == "WaitingForApproval":
        changes = {"forceRollout": r.new_uuid()}
    elif cell.phase == "ReadyToPromote":
        changes = {**r.new_request(), "rolloutStrategy": "ManuallyPromote"}
    else:
        changes = r.new_request()
    return _queue(
        ctx, s, prior, f"spec_phase_setup_{cell.phase}", changes, "seek_phase", "setup"
    )


def _seek(ctx: r.Ctx, s: journal.Scenario) -> str:
    cell = _cell(s)
    found: list[r.Snapshot] = []
    predicate = sp.phase_predicate(
        cell.phase, s.request, s.data.get("setup_generation")
    )

    def until(item: kube_watch.Item) -> bool:
        objects = (
            item.items[:1]
            if isinstance(item, kube_watch.Relist)
            else ([] if item.type == "DELETED" else [item.obj])
        )
        for obj in objects:
            snap = r.Snapshot(obj)
            if predicate(snap):
                found.append(snap)
                return True
        return False

    if time.monotonic() >= s.deadline:
        s.outcome = "SetupTimeout"
        s.stage = "cleanup"
        journal.save(ctx.db, s)
        return "timeout"
    window = sp.cr_watch(ctx.kube).window(s.data.get("resource_version"), until)
    s.data["resource_version"] = window.resource_version
    if window.error is not None:
        sp.log(f"CR watch: {window.error}")
    if not found:
        journal.save(ctx.db, s)
        return "waiting"
    reached = found[0]
    s.data["reached"] = reached.obj
    delay = (
        r.rng.choice(sp.ACT_DELAY_MENU)
        if cell.phase in spec_model.IN_PROGRESS_PHASES and reached.reason == cell.phase
        else 0
    )
    s.delay_until = time.monotonic() + delay
    s.stage = "act_cell"
    journal.save(ctx.db, s)
    return _act(ctx, s) if delay == 0 else "delayed"


def _act(ctx: r.Ctx, s: journal.Scenario) -> str:
    if time.monotonic() < s.delay_until:
        return "delayed"
    current = ctx.poll()
    if current is None:
        return "unavailable"
    cell = _cell(s)
    endpoints = ctx.env.endpoints
    images = spec_model.Images(
        endpoints.initial_environmentd_image,
        endpoints.upgrade_pending_image,
        current.last_completed_image,
    )
    changes = spec_model.cell_changes(
        cell, r.rng, current.spec, r.Snapshot(s.prior).spec, images, r.new_uuid
    )
    s.data["before_cell_spec"] = current.spec
    s.data["changes"] = changes
    return _queue(
        ctx,
        s,
        current,
        f"spec_phase_{cell.field}_{cell.request_mode}",
        changes,
        "follow_cell",
        "cell",
    )


def _kill(ctx: r.Ctx, s: journal.Scenario) -> None:
    if s.kill is None or s.kill_attempted:
        return
    snap = ctx.poll()
    stored = r.Snapshot(s.data["stored"])
    if snap is None or (snap.request, snap.reason, snap.active) != (
        s.request,
        stored.reason,
        stored.active,
    ):
        return

    def before_delete() -> None:
        s.kill_attempted = True
        journal.save(ctx.db, s)

    if r.execute_kill(ctx, r.KillPlan(**s.kill), snap, before_delete=before_delete):
        s.killed_phase = snap.reason
    journal.save(ctx.db, s)


def _details(s: journal.Scenario) -> dict[str, Any]:
    cell = _cell(s)
    return {
        "cell": cell.key(),
        "request_mode": cell.request_mode,
        "kill": cell.kill,
        "patched_in": s.data.get("patched_in"),
        "changes": s.data.get("changes"),
        "phases_after_patch": s.phases,
    }


def _follow(ctx: r.Ctx, s: journal.Scenario) -> str:
    _kill(ctx, s)
    end = min(s.deadline, time.monotonic() + journal.OBSERVE_SECONDS)
    while time.monotonic() < end:
        snap = ctx.poll()
        if snap is not None:
            if snap.reason and (not s.phases or s.phases[-1] != snap.reason):
                s.phases.append(snap.reason)
            if (
                snap.reason == "ReadyToPromote"
                and snap.spec.get("rolloutStrategy") == "ManuallyPromote"
                and snap.request is not None
                and snap.spec.get("forcePromote") != snap.request
            ):
                return _queue(
                    ctx,
                    s,
                    snap,
                    "spec_phase_promote",
                    {"forcePromote": snap.request},
                    "follow_cell",
                    "promote",
                )
            if sp.settled(snap, s.data.get("generation")):
                s.outcome = snap.reason
                s.data["final"] = snap.obj
                s.stage = "judge_first"
                sp.sometimes(
                    snap.reason == "Applied",
                    "Spec-phase cell settled at Applied",
                    _details(s),
                )
                journal.save(ctx.db, s)
                return "settled"
        time.sleep(min(r.POLL_SECONDS, max(0.0, end - time.monotonic())))
    if time.monotonic() >= s.deadline:
        s.outcome = "Timeout"
        s.stage = "cleanup"
        sp.sometimes(False, "Spec-phase cell settled at Applied", _details(s))
    journal.save(ctx.db, s)
    return "observed"


def _judge(ctx: r.Ctx, s: journal.Scenario) -> str:
    if s.stage == "judge_second" and time.monotonic() < s.delay_until:
        return "delayed"
    observer = r.observer_db(ctx.env)
    try:
        promoting = s.data.get("promoting")
        final = r.Snapshot(s.data["final"])
        if promoting is not None and final.active == promoting[0] + 1:
            r.mark_promoted_across_spec_change(observer, promoting[0] + 1)
        claim = r.read_claim(ctx.kube)
        if claim is None:
            return "unavailable"
        first = s.data.get("first_claim")
        if s.stage == "judge_first" and claim.check.violated:
            s.data["first_claim"] = {
                "key": list(claim.check.key),
                "mismatches": claim.check.mismatches,
            }
            s.delay_until = time.monotonic() + r.STATUS_SETTLE_SECONDS
            s.stage = "judge_second"
        else:
            if first is None or list(claim.check.key) == first["key"]:
                r.assert_status_claim(
                    sp.claim_cause(observer, promoting, claim),
                    not claim.check.violated,
                    {
                        **claim.details(),
                        **_details(s),
                        "first_mismatches": first["mismatches"] if first else None,
                    },
                )
            s.stage = "cleanup"
        journal.save(ctx.db, s)
        return "judged"
    finally:
        observer.close()


def step(ctx: r.Ctx) -> str:
    """Advance a spec-phase trial without retaining the CR lease between commands."""
    s = journal.load(ctx.db)
    if s is not None and s.owner != "spec_phase":
        return "other_command"
    if s is None:
        s = _begin(ctx)
        if s is None:
            return "unavailable"
    if time.monotonic() - s.created >= journal.SCENARIO_SECONDS:
        s.outcome = "Timeout"
        s.stage = "cleanup"
    sp.log(f"step {s.stage}")
    if s.stage == "normalize":
        if not r.normalize(ctx, "spec_phase_normalize"):
            return "unavailable"
        s.stage = "settle_before"
        s.deadline = time.monotonic() + r.PHASE_WAIT_SECONDS
        journal.save(ctx.db, s)
        return "normalized"
    if s.stage == "settle_before":
        return _before(ctx, s)
    if s.stage == "setup":
        return _setup(ctx, s)
    if s.stage == "patch":
        return _patch(ctx, s)
    if s.stage == "seek_phase":
        return _seek(ctx, s)
    if s.stage == "act_cell":
        return _act(ctx, s)
    if s.stage == "follow_cell":
        return _follow(ctx, s)
    if s.stage in ("judge_first", "judge_second"):
        return _judge(ctx, s)
    if s.stage == "cleanup":
        if not r.normalize(ctx, "spec_phase_restore"):
            return "unavailable"
        journal._complete(ctx, s)
        return "done"
    if s.stage == "done":
        journal._complete(ctx, s)
        return "done"
    raise ValueError(f"unrecognized spec-phase step {s.stage}")

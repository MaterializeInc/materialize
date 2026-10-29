# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import copy
import sqlite3
from dataclasses import dataclass, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import ddl_hold, spec_model
from materialize.antithesis import rollout_steps as steps
from materialize.antithesis.drivers import rollouts as r
from materialize.antithesis.drivers import spec_phase


class Kube:
    def __init__(self) -> None:
        self.obj: dict[str, Any] = {
            "metadata": {"generation": 1},
            "spec": {
                "requestRollout": "r0",
                "forceRollout": "r0",
                "environmentdImageRef": "old",
                "rolloutStrategy": r.DEFAULT_ROLLOUT_STRATEGY,
                "rolloutRequestTimeout": r.DEFAULT_ROLLOUT_REQUEST_TIMEOUT,
            },
            "status": {
                "activeGeneration": 5,
                "lastCompletedRolloutRequest": "r0",
                "lastCompletedRolloutEnvironmentdImageRef": "old",
                "conditions": [
                    {"type": "UpToDate", "reason": "Applied", "status": "True"}
                ],
            },
        }
        self.calls: list[dict[str, Any]] = []
        self.failure: str | None = None

    def snapshot(self) -> r.Snapshot:
        return r.Snapshot(copy.deepcopy(self.obj))

    def try_snapshot(self) -> r.Snapshot:
        return self.snapshot()

    def phase(self, reason: str) -> None:
        self.obj["status"]["conditions"][0]["reason"] = reason
        self.obj["status"]["observedGeneration"] = self.obj["metadata"]["generation"]
        self.obj["status"]["conditions"][0]["observedGeneration"] = self.obj[
            "metadata"
        ]["generation"]
        if reason in r.TERMINAL_FOR_REQUEST:
            self.obj["status"]["lastCompletedRolloutRequest"] = self.obj["spec"][
                "requestRollout"
            ]
            self.obj["status"]["lastCompletedRolloutEnvironmentdImageRef"] = self.obj[
                "spec"
            ]["environmentdImageRef"]

    def patch_spec(self, changes: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(copy.deepcopy(changes))
        failure, self.failure = self.failure, None
        if failure != "before":
            after = spec_model.merge_patch(self.obj["spec"], changes)
            if after != self.obj["spec"]:
                self.obj["metadata"]["generation"] += 1
            self.obj["spec"] = after
        if failure is not None:
            raise OSError("response lost")
        return copy.deepcopy(self.obj)


@dataclass
class Fleet:
    db: sqlite3.Connection
    kube: Kube
    env: Any
    params: r.TimelineParams
    now: list[float]
    choices: dict[str, Any]

    def ctx(self) -> r.Ctx:
        return r.Ctx(self.env, self.kube, self.db, self.params)  # type: ignore[arg-type]


@pytest.fixture
def fleet(monkeypatch: pytest.MonkeyPatch) -> Fleet:
    db = sqlite3.connect(":memory:")
    env = Mock(endpoints=SimpleNamespace(upgrade_pending_image="new"))
    monkeypatch.setattr(r, "open_db", lambda *args: db)
    r.requests_db(env)
    choices: dict[str, Any] = {"random": 0.8}
    serial = iter(range(100))
    monkeypatch.setattr(r, "new_uuid", lambda: f"r{next(serial) + 1}")
    monkeypatch.setattr(r.rng, "random", lambda: choices["random"])
    monkeypatch.setattr(
        r.rng,
        "choice",
        lambda seq: (
            choices["variant"]
            if "variant" in choices and choices["variant"] in seq
            else seq[0]
        ),
    )
    monkeypatch.setattr(r, "maybe_kill_plan", lambda ctx: None)
    monkeypatch.setattr(r, "observer_db", lambda env: Mock())
    monkeypatch.setattr(r, "observe_once", Mock())
    now = [0.0]
    monkeypatch.setattr(r.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(
        r.time, "sleep", lambda seconds: now.__setitem__(0, now[0] + seconds)
    )
    return Fleet(
        db, Kube(), env, r.TimelineParams({a: 1 for a in r.ACTIONS}, 0, 0), now, choices
    )


@pytest.mark.parametrize("lost", ["before", "after"])
def test_interrupted_patch_reuses_its_durable_request(fleet: Fleet, lost: str) -> None:
    steps.begin(fleet.ctx(), "same_version")
    assert steps.step(fleet.ctx()) == "planned"
    intent = steps.load(fleet.db)
    assert intent is not None and intent.intent is not None
    request = intent.intent["requestRollout"]
    fleet.kube.failure = lost
    assert steps.step(fleet.ctx()) == "unacknowledged"
    assert steps.step(fleet.ctx()) == "patched"
    resumed = steps.load(fleet.db)
    assert (
        resumed is not None and resumed.request == request and resumed.stage == "follow"
    )
    assert len(fleet.kube.calls) == (2 if lost == "before" else 1)
    assert all(c["requestRollout"] == request for c in fleet.kube.calls)


def test_follow_yields_after_one_observation_window(fleet: Fleet) -> None:
    steps.begin(fleet.ctx(), "same_version")
    steps.step(fleet.ctx())
    steps.step(fleet.ctx())
    initial = steps.load(fleet.db)
    assert initial is not None
    deadline = initial.deadline
    assert steps.step(fleet.ctx()) == "observed"
    assert fleet.now[0] == steps.OBSERVE_SECONDS
    resumed = steps.load(fleet.db)
    assert (
        resumed is not None
        and resumed.stage == "follow"
        and resumed.deadline == deadline
    )


def test_manual_delay_survives_process_exit_without_normalizing(
    fleet: Fleet, monkeypatch: pytest.MonkeyPatch
) -> None:
    steps.begin(fleet.ctx(), "manual_promote")
    steps.step(fleet.ctx())
    steps.step(fleet.ctx())
    fleet.kube.phase("ReadyToPromote")
    fleet.choices["variant"] = 120
    assert steps.step(fleet.ctx()) == "delayed"
    normalize = Mock(
        side_effect=AssertionError("temporary strategy belongs to the active scenario")
    )
    monkeypatch.setattr(r, "normalize", normalize)
    assert steps.step(fleet.ctx()) == "delayed"
    assert fleet.now[0] == 0
    fleet.now[0] = 120
    assert steps.step(fleet.ctx()) == "patched"
    assert fleet.kube.calls[-1] == {
        "forcePromote": fleet.kube.obj["spec"]["requestRollout"]
    }
    normalize.assert_not_called()


@pytest.mark.parametrize(
    "action,variant,draw",
    [
        ("same_version", None, 0.8),
        ("forced_migration", None, 0.8),
        ("supersede", None, 0.8),
        *(
            ("revert", v, 0.8)
            for v in ("restore_request", "keep_request", "fresh_request")
        ),
        *(
            ("patch_during_promoting", v, 0.8)
            for v in ("new_request", "same_request", "revert")
        ),
        ("force_promote", None, 0.1),
        ("force_promote", None, 0.8),
        ("manual_promote", 0, 0.8),
        ("short_timeout", None, 0.8),
        ("upgrade", None, 0.8),
        ("upgrade", "restore_request", 0.1),
        ("upgrade", "fresh_request", 0.1),
    ],
)
def test_scenarios_resume_through_actions_checks_and_cleanup(
    fleet: Fleet,
    action: str,
    variant: Any,
    draw: float,
) -> None:
    fleet.choices.update(variant=variant, random=draw)
    steps.begin(fleet.ctx(), action)
    for _ in range(30):
        s = steps.load(fleet.db)
        if s is None:
            break
        if s.stage == "wait":
            fleet.kube.phase(s.wait_for[0])
        if s.stage == "follow":
            fleet.kube.phase(
                "RolloutTimeout"
                if action == "short_timeout" and not s.retry
                else "Applied"
            )
        if s.stage == "act":
            fleet.now[0] = max(fleet.now[0], s.delay_until)
        steps.step(fleet.ctx())
    assert (
        steps.load(fleet.db) is None
    ), "every action must reach a terminal journal transition"
    assert fleet.db.execute(
        "SELECT action, outcome, count FROM rollout_outcomes"
    ).fetchall() == [(action, "Applied", 1)]
    assert fleet.kube.obj["spec"]["rolloutStrategy"] == r.DEFAULT_ROLLOUT_STRATEGY
    assert (
        fleet.kube.obj["spec"]["rolloutRequestTimeout"]
        == r.DEFAULT_ROLLOUT_REQUEST_TIMEOUT
    )
    assert r.observe_once.called
    if action == "short_timeout":
        assert (
            len(
                {c["requestRollout"] for c in fleet.kube.calls if "requestRollout" in c}
            )
            == 2
        )
    if action == "upgrade":
        assert fleet.kube.obj["spec"]["environmentdImageRef"] == (
            "old" if draw < 0.3 else "new"
        )


def test_interrupted_delete_does_not_kill_a_replacement_again(
    fleet: Fleet, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        r, "maybe_kill_plan", lambda _: r.KillPlan("Applying", "candidate", 1)
    )
    monkeypatch.setattr(r, "kill_due", lambda *args: True)
    steps.begin(fleet.ctx(), "same_version")
    steps.step(fleet.ctx())
    steps.step(fleet.ctx())

    def deleted(*args: Any, before_delete: Any) -> None:
        before_delete()
        raise KeyboardInterrupt("driver died after sending its delete")

    delete = Mock(side_effect=deleted)
    monkeypatch.setattr(r, "execute_kill", delete)
    with pytest.raises(KeyboardInterrupt):
        steps.step(fleet.ctx())
    resumed = steps.load(fleet.db)
    assert resumed is not None and resumed.kill_attempted
    assert steps.step(fleet.ctx()) == "observed"
    delete.assert_called_once()


def test_driver_runs_one_step_and_releases_its_lease(
    fleet: Fleet, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(r, "Environment", lambda: fleet.env)
    monkeypatch.setattr(r, "Kube", lambda env: fleet.kube)
    monkeypatch.setattr(r, "timeline_params", lambda db: fleet.params)
    lease = Mock()
    lease.acquire.return_value = True
    monkeypatch.setattr(r, "CrLease", lambda env: lease)
    monkeypatch.setattr(r, "ACTION_FNS", {a: Mock() for a in r.ACTIONS}, raising=False)
    monkeypatch.setattr(r.rng, "choices", lambda *args, **kwargs: ["same_version"])
    advance = Mock(return_value="observed")
    monkeypatch.setattr(steps, "step", advance)
    assert r.driver_main() == 0
    advance.assert_called_once()
    lease.release.assert_called_once()


def test_spec_phase_does_not_normalize_an_active_scenario(
    fleet: Fleet, monkeypatch: pytest.MonkeyPatch
) -> None:
    steps.begin(fleet.ctx(), "manual_promote")
    monkeypatch.setattr(spec_phase, "Environment", lambda: fleet.env)
    monkeypatch.setattr(spec_phase, "Kube", lambda env: fleet.kube)
    monkeypatch.setattr(r, "timeline_params", lambda db: fleet.params)
    monkeypatch.setattr(r, "CrLease", lambda env: Mock())
    normalize = Mock(side_effect=AssertionError("must not steal the scenario"))
    monkeypatch.setattr(r, "normalize", normalize)
    assert spec_phase.driver_main() == 0
    normalize.assert_not_called()


def test_paced_trial_is_selected_once_and_claimed_by_its_command(
    fleet: Fleet, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(r.rng, "choices", lambda *args, **kwargs: ["paced_ddl"])
    assert steps.step(fleet.ctx()) == "paced_pending"
    scenario = steps.load(fleet.db)
    assert scenario is not None and scenario.action == "paced_ddl"
    for _ in range(3):
        assert steps.step(fleet.ctx()) == "paced_pending"
        assert steps.load(fleet.db) == scenario
    monkeypatch.setattr(r, "Environment", lambda: fleet.env)
    monkeypatch.setattr(r, "Kube", lambda env: fleet.kube)
    monkeypatch.setattr(r, "timeline_params", lambda db: fleet.params)
    monkeypatch.setattr(r, "CrLease", lambda env: Mock())
    from contextlib import nullcontext

    monkeypatch.setattr(steps.ddl_hold, "held", lambda *args, **kwargs: nullcontext())
    trial = Mock(side_effect=[False, True])
    monkeypatch.setattr(r, "action_paced_ddl", trial)
    assert r.paced_ddl_main() == 0
    assert steps.load(fleet.db) == scenario, "a skipped trial must remain schedulable"
    assert r.paced_ddl_main() == 0
    assert steps.load(fleet.db) is None
    assert trial.call_count == 2
    assert fleet.db.execute(
        "SELECT action, outcome FROM rollout_outcomes"
    ).fetchall() == [("paced_ddl", "Completed")]


def test_cleanup_reconciles_an_indeterminate_restore(fleet: Fleet) -> None:
    s = steps.begin(fleet.ctx(), "manual_promote")
    assert s is not None
    s.stage = "cleanup"
    s.request = "r0"
    s.outcome = "Applied"
    fleet.kube.obj["spec"]["rolloutStrategy"] = "ManuallyPromote"
    steps.save(fleet.db, s)
    assert steps.step(fleet.ctx()) == "restoring"
    fleet.kube.failure = "after"
    assert steps.step(fleet.ctx()) == "unacknowledged"
    assert steps.step(fleet.ctx()) == "patched"
    assert steps.step(fleet.ctx()) == "done"
    assert len(fleet.kube.calls) == 1
    assert fleet.kube.obj["spec"]["rolloutStrategy"] == r.DEFAULT_ROLLOUT_STRATEGY


def hold_state(fleet: Fleet, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        ddl_hold.state,
        "open_db",
        lambda name: sqlite3.connect(tmp_path / "holds.sqlite"),
    )
    fleet.params = replace(fleet.params, ddl_hold_probability=1)


def test_scenario_hold_survives_a_long_gap_and_releases_on_completion(
    fleet: Fleet,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    hold_state(fleet, monkeypatch, tmp_path)
    s = steps.begin(fleet.ctx(), "same_version")
    assert s is not None
    assert ddl_hold.active()
    assert not ddl_hold.strict_active()
    fleet.now[0] = 120
    assert ddl_hold.active(), "a scheduler gap must not release a held scenario's DDL"
    s.stage = "done"
    steps.save(fleet.db, s)
    assert steps.step(fleet.ctx()) == "done"
    assert not ddl_hold.active()


def test_scenario_repairs_a_hold_interrupted_after_journaling(
    fleet: Fleet,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    hold_state(fleet, monkeypatch, tmp_path)
    with monkeypatch.context() as attempt:
        attempt.setattr(
            ddl_hold, "request_until", Mock(side_effect=KeyboardInterrupt())
        )
        with pytest.raises(KeyboardInterrupt):
            steps.begin(fleet.ctx(), "same_version")
    assert steps.load(fleet.db) is not None
    assert not ddl_hold.active()
    assert steps.step(fleet.ctx()) == "planned"
    assert ddl_hold.active()
    assert not fleet.kube.calls


def test_interrupted_completion_cannot_orphan_a_hold(
    fleet: Fleet,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    hold_state(fleet, monkeypatch, tmp_path)
    s = steps.begin(fleet.ctx(), "same_version")
    assert s is not None
    s.stage = "done"
    steps.save(fleet.db, s)
    fleet.db.execute(
        "CREATE TRIGGER stop_delete BEFORE DELETE ON rollout_scenario BEGIN SELECT RAISE(ABORT, 'interrupted'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="interrupted"):
        steps.step(fleet.ctx())
    assert steps.load(fleet.db) is not None
    assert (
        not ddl_hold.active()
    ), "the journal cannot forget an owner whose hold survives"
    fleet.db.execute("DROP TRIGGER stop_delete")
    assert steps.step(fleet.ctx()) == "done"
    assert not ddl_hold.active()
    assert fleet.db.execute("SELECT count FROM rollout_outcomes").fetchone()[0] == 1


def test_abandoned_scenario_hold_expires_and_does_not_release_other_owners(
    fleet: Fleet,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    hold_state(fleet, monkeypatch, tmp_path)
    steps.begin(fleet.ctx(), "same_version")
    ddl_hold.request_until("another", steps.SCENARIO_SECONDS + 100, strict=True)
    fleet.now[0] = steps.SCENARIO_SECONDS + 1
    assert steps.step(fleet.ctx()) == "expired"
    assert ddl_hold.strict_active()
    ddl_hold.release("another")
    assert not ddl_hold.active()


def test_expiration_does_not_credit_an_unchecked_result(fleet: Fleet) -> None:
    s = steps.begin(fleet.ctx(), "same_version")
    assert s is not None
    s.stage = "check"
    s.outcome = "Applied"
    steps.save(fleet.db, s)
    fleet.now[0] = steps.SCENARIO_SECONDS + 1
    assert steps.step(fleet.ctx()) == "expired"
    assert (
        fleet.db.execute("SELECT outcome FROM rollout_outcomes").fetchone()[0]
        == "Timeout"
    )


def test_cr_phase_observation_retains_request_start_across_delayed_response(
    fleet: Fleet,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = object.__new__(r.Kube)
    api.namespace = "mz"
    api.name = "environment"

    def get(*args: Any, **kwargs: Any) -> dict[str, Any]:
        fleet.now[0] = 3.0
        return fleet.kube.obj

    api.custom = Mock()
    api.custom.get_namespaced_custom_object.side_effect = get
    phase = Mock()
    monkeypatch.setattr(r.telemetry, "phase_observed", phase)
    api.snapshot()
    assert phase.call_args.kwargs["started"] == 0.0
    assert fleet.now[0] == 3.0


def test_unacknowledged_intent_timeout_is_never_a_success(
    fleet: Fleet,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    steps.begin(fleet.ctx(), "same_version")
    steps.step(fleet.ctx())
    fleet.kube.failure = "before"
    assert steps.step(fleet.ctx()) == "unacknowledged"
    s = steps.load(fleet.db)
    assert s is not None
    fleet.now[0] = s.deadline + 1
    reached = Mock()
    monkeypatch.setattr(r, "reachable", reached)
    assert steps.step(fleet.ctx()) == "timeout"
    assert steps.step(fleet.ctx()) == "restored"
    assert steps.step(fleet.ctx()) == "done"
    assert (
        fleet.db.execute("SELECT outcome FROM rollout_outcomes").fetchone()[0]
        == "Timeout"
    )
    reached.assert_not_called()


def test_a_superseded_intent_does_not_overwrite_the_new_request(fleet: Fleet) -> None:
    steps.begin(fleet.ctx(), "same_version")
    steps.step(fleet.ctx())
    fleet.kube.obj["spec"]["requestRollout"] = "external"
    assert steps.step(fleet.ctx()) == "superseded"
    assert not fleet.kube.calls
    assert steps.load(fleet.db) is None
    assert (
        fleet.db.execute("SELECT outcome FROM rollout_outcomes").fetchone()[0]
        == "Superseded"
    )


@pytest.mark.parametrize(
    "action,phase",
    [
        ("supersede", "Applying"),
        ("revert", "Applying"),
        ("patch_during_promoting", "Promoting"),
    ],
)
@pytest.mark.parametrize("advance", [False, True])
def test_phase_coverage_uses_the_acknowledged_patch_version(
    fleet: Fleet,
    monkeypatch: pytest.MonkeyPatch,
    action: str,
    phase: str,
    advance: bool,
) -> None:
    steps.begin(fleet.ctx(), action)
    steps.step(fleet.ctx())
    steps.step(fleet.ctx())
    s = steps.load(fleet.db)
    assert s is not None
    s.stage = "act"
    steps.save(fleet.db, s)
    fleet.kube.phase(phase)
    original = steps._queue

    def queued(*args: Any) -> None:
        original(*args)
        if advance:
            fleet.kube.phase("Applied")

    monkeypatch.setattr(steps, "_queue", queued)
    coverage = Mock()
    monkeypatch.setattr(r, "sometimes", coverage)
    assert steps.step(fleet.ctx()) == "patched"
    coverage.assert_called_once()
    assert coverage.call_args.args[0] is not advance


def test_reconciling_a_lost_response_does_not_invent_patch_phase_coverage(
    fleet: Fleet,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    steps.begin(fleet.ctx(), "supersede")
    steps.step(fleet.ctx())
    steps.step(fleet.ctx())
    s = steps.load(fleet.db)
    assert s is not None
    s.stage = "act"
    steps.save(fleet.db, s)
    fleet.kube.phase("Applying")
    fleet.kube.failure = "after"
    coverage = Mock()
    monkeypatch.setattr(r, "sometimes", coverage)
    assert steps.step(fleet.ctx()) == "unacknowledged"
    assert steps.step(fleet.ctx()) == "patched"
    coverage.assert_not_called()

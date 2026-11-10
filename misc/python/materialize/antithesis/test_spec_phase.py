# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

from dataclasses import asdict
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import kube_watch, spec_model
from materialize.antithesis import rollout_steps as journal
from materialize.antithesis import spec_phase_steps as steps
from materialize.antithesis.drivers import rollouts as r
from materialize.antithesis.drivers import spec_phase
from materialize.antithesis.test_rollout_steps import Fleet
from materialize.antithesis.test_rollout_steps import fleet as fleet


@pytest.fixture
def phase_fleet(fleet: Fleet, monkeypatch: pytest.MonkeyPatch) -> Fleet:
    fleet.env.endpoints.initial_environmentd_image = "old"
    fleet.kube.phase("Applied")
    monkeypatch.setattr(r, "promoted_across_spec_change", lambda _: [])
    monkeypatch.setattr(r, "mark_promoted_across_spec_change", Mock())
    monkeypatch.setattr(r, "assert_status_claim", Mock())

    def claim(*args: Any) -> Any:
        return SimpleNamespace(
            snap=fleet.kube.snapshot(),
            check=spec_model.StatusCheck("applied", ("applied", 5, 1, "r0"), {}, False),
            details=lambda: {},
        )

    monkeypatch.setattr(r, "read_claim", claim)
    return fleet


def start_cell(
    fleet: Fleet, monkeypatch: pytest.MonkeyPatch, cell: spec_model.Cell
) -> None:
    monkeypatch.setattr(spec_model, "choose_cell", lambda _: cell)
    assert steps.step(fleet.ctx()) == "normalized"
    assert steps.step(fleet.ctx()) == "selected"


def phase_watch(
    fleet: Fleet, monkeypatch: pytest.MonkeyPatch, phase: str
) -> list[str | None]:
    versions: list[str | None] = []

    def window(rv: str | None, until: Any) -> Any:
        versions.append(rv)
        fleet.kube.phase(phase)
        version = f"version-{len(versions)}"
        until(kube_watch.Relist([fleet.kube.obj], version))
        return SimpleNamespace(resource_version=version, error=None)

    monkeypatch.setattr(
        spec_phase, "cr_watch", lambda _: SimpleNamespace(window=window)
    )
    return versions


def test_manual_promotion_retries_an_unacknowledged_patch() -> None:
    snap = SimpleNamespace(
        reason="ReadyToPromote",
        request="r",
        spec={"rolloutStrategy": "ManuallyPromote"},
    )
    ctx = MagicMock()
    ctx.poll.side_effect = [snap, snap]
    with (
        patch.object(spec_phase, "settled", side_effect=[False, True]),
        patch.object(
            spec_phase.rollouts, "submit_patch", side_effect=[("r", None), ("r", snap)]
        ) as submit,
        patch.object(spec_phase.time, "sleep"),
    ):
        final, _ = spec_phase.follow_until_settled(ctx, None, 10)
    assert final is snap
    assert submit.call_count == 2


def test_eventual_settle_retries_unacknowledged_normalization() -> None:
    ctx = MagicMock()
    snap = SimpleNamespace(
        active=1, reason="Applied", summary=lambda: {}, tracked_spec=lambda: {}
    )
    ctx.kube.snapshot.return_value = snap
    now = [0.0]

    def watch(*args: object) -> None:
        now[0] += 5

    with (
        patch.object(spec_phase, "Environment"),
        patch.object(spec_phase.rollouts, "operator_ctx", return_value=ctx),
        patch.object(spec_phase.rollouts, "observer_db"),
        patch.object(spec_phase.rollouts, "CrWatcher"),
        patch.object(spec_phase.rollouts, "StatefulSetWatcher"),
        patch.object(spec_phase.rollouts, "run_watches", side_effect=watch),
        patch.object(
            spec_phase.rollouts, "promoted_across_spec_change", return_value=set()
        ),
        patch.object(
            spec_phase.rollouts, "normalize", side_effect=[False, True]
        ) as normalize,
        patch.object(spec_phase, "settled", side_effect=[False, True]),
        patch.object(spec_phase.time, "monotonic", side_effect=lambda: now[0]),
        patch.object(spec_phase, "judge_claim"),
    ):
        spec_phase.eventually_main()
    assert normalize.call_count == 2


@pytest.mark.parametrize("field,phase", spec_model.all_cells())
def test_every_dynamic_cell_resumes_to_a_checked_outcome(
    phase_fleet: Fleet,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    phase: str,
) -> None:
    cell = spec_model.Cell(field, phase, spec_model.request_modes(field)[0], None)
    start_cell(phase_fleet, monkeypatch, cell)
    phase_watch(phase_fleet, monkeypatch, phase)
    coverage = Mock()
    monkeypatch.setattr(spec_phase, "note_coverage", coverage)
    for _ in range(20):
        s = journal.load(phase_fleet.db)
        if s is None:
            break
        if s.stage == "follow_cell":
            phase_fleet.kube.phase("Applied")
        steps.step(phase_fleet.ctx())
    assert journal.load(phase_fleet.db) is None
    coverage.assert_called_once_with(cell, phase)
    r.assert_status_claim.assert_called_once()
    assert phase_fleet.db.execute(
        "SELECT action, outcome FROM rollout_outcomes"
    ).fetchall() == [("spec_phase", "Applied")]


def test_watch_resumes_its_version_and_patches_inline_when_phase_arrives(
    phase_fleet: Fleet,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cell = spec_model.Cell("force_rollout", "Promoting", "keep", None)
    start_cell(phase_fleet, monkeypatch, cell)
    assert steps.step(phase_fleet.ctx()) == "patched"
    versions: list[str | None] = []

    def window(rv: str | None, until: Any) -> Any:
        versions.append(rv)
        if len(versions) == 2:
            phase_fleet.kube.phase("Promoting")
            until(kube_watch.Relist([phase_fleet.kube.obj], "v2"))
        return SimpleNamespace(resource_version=f"v{len(versions)}", error=None)

    monkeypatch.setattr(
        spec_phase, "cr_watch", lambda _: SimpleNamespace(window=window)
    )
    assert steps.step(phase_fleet.ctx()) == "waiting"
    before = len(phase_fleet.kube.calls)
    assert steps.step(phase_fleet.ctx()) == "patched"
    assert versions == [None, "v1"]
    assert len(phase_fleet.kube.calls) == before + 1
    s = journal.load(phase_fleet.db)
    assert s is not None and s.data["promoting"] == [5, "keep"]
    phase_fleet.kube.obj["status"]["activeGeneration"] = 6
    phase_fleet.kube.phase("Applied")
    assert steps.step(phase_fleet.ctx()) == "settled"
    assert steps.step(phase_fleet.ctx()) == "judged"
    r.mark_promoted_across_spec_change.assert_called_once()


def test_lost_cell_response_reconciles_without_claiming_its_phase(
    phase_fleet: Fleet,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cell = spec_model.Cell("force_rollout", "Applied", "fresh", None)
    start_cell(phase_fleet, monkeypatch, cell)
    phase_fleet.kube.failure = "after"
    coverage = Mock()
    monkeypatch.setattr(spec_phase, "note_coverage", coverage)
    assert steps.step(phase_fleet.ctx()) == "unacknowledged"
    assert steps.step(phase_fleet.ctx()) == "patched"
    assert len(phase_fleet.kube.calls) == 1
    s = journal.load(phase_fleet.db)
    assert s is not None and s.stage == "follow_cell"
    assert s.data["generation"] == phase_fleet.kube.obj["metadata"]["generation"]
    coverage.assert_not_called()


def test_cell_phase_delay_and_follow_yield_without_holding_the_lease(
    phase_fleet: Fleet,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cell = spec_model.Cell("force_rollout", "Applying", "keep", None)
    start_cell(phase_fleet, monkeypatch, cell)
    assert steps.step(phase_fleet.ctx()) == "patched"
    phase_watch(phase_fleet, monkeypatch, "Applying")
    phase_fleet.choices["variant"] = 5
    assert steps.step(phase_fleet.ctx()) == "delayed"
    assert steps.step(phase_fleet.ctx()) == "delayed"
    assert phase_fleet.now[0] == 0
    phase_fleet.now[0] = 5
    assert steps.step(phase_fleet.ctx()) == "patched"
    s = journal.load(phase_fleet.db)
    assert s is not None
    deadline = s.deadline
    assert steps.step(phase_fleet.ctx()) == "observed"
    assert phase_fleet.now[0] == 5 + journal.OBSERVE_SECONDS
    resumed = journal.load(phase_fleet.db)
    assert resumed is not None and resumed.deadline == deadline


def test_ordinary_command_does_not_steal_a_spec_trial(
    phase_fleet: Fleet,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    start_cell(
        phase_fleet,
        monkeypatch,
        spec_model.Cell("force_rollout", "Applied", "keep", None),
    )
    prior = journal.load(phase_fleet.db)
    assert journal.step(phase_fleet.ctx()) == "other_command"
    assert journal.load(phase_fleet.db) == prior


@pytest.mark.parametrize("same_key", [False, True])
def test_claim_recheck_delay_and_key_survive_invocation_exit(
    phase_fleet: Fleet,
    monkeypatch: pytest.MonkeyPatch,
    same_key: bool,
) -> None:
    s = journal.Scenario(
        "spec",
        "spec_phase",
        phase_fleet.kube.obj,
        0,
        False,
        owner="spec_phase",
        stage="judge_first",
        outcome="Applied",
        data={
            "cell": asdict(spec_model.Cell("force_rollout", "Promoting", "keep", None)),
            "final": phase_fleet.kube.obj,
        },
    )
    journal.initialize(phase_fleet.db)
    journal.save(phase_fleet.db, s)

    def claim(key: str) -> Any:
        return SimpleNamespace(
            snap=phase_fleet.kube.snapshot(),
            check=spec_model.StatusCheck("applied", (key,), {"changed": 1}, False),
            details=lambda: {},
        )

    monkeypatch.setattr(
        r,
        "read_claim",
        Mock(side_effect=[claim("one"), claim("one" if same_key else "two")]),
    )
    assert steps.step(phase_fleet.ctx()) == "judged"
    assert steps.step(phase_fleet.ctx()) == "delayed"
    r.assert_status_claim.assert_not_called()
    phase_fleet.now[0] = r.STATUS_SETTLE_SECONDS
    assert steps.step(phase_fleet.ctx()) == "judged"
    assert r.assert_status_claim.called is same_key
    if same_key:
        assert r.assert_status_claim.call_args.args[1] is False


def test_spec_driver_advances_one_step_and_releases_its_lease(
    phase_fleet: Fleet,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(spec_phase, "Environment", lambda: phase_fleet.env)
    monkeypatch.setattr(spec_phase, "Kube", lambda _: phase_fleet.kube)
    monkeypatch.setattr(r, "timeline_params", lambda _: phase_fleet.params)
    lease = Mock()
    monkeypatch.setattr(spec_phase, "CrLease", lambda _: lease)
    advance = Mock()
    monkeypatch.setattr(steps, "step", advance)
    monkeypatch.setattr(spec_phase, "run_cell", Mock(), raising=False)
    assert spec_phase.driver_main() == 0
    advance.assert_called_once()
    lease.release.assert_called_once()


def test_setup_request_survives_an_indeterminate_response(
    phase_fleet: Fleet,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cell = spec_model.Cell("force_rollout", "Promoting", "fresh", None)
    start_cell(phase_fleet, monkeypatch, cell)
    phase_fleet.kube.failure = "after"
    assert steps.step(phase_fleet.ctx()) == "unacknowledged"
    assert steps.step(phase_fleet.ctx()) == "patched"
    assert len(phase_fleet.kube.calls) == 1
    s = journal.load(phase_fleet.db)
    assert s is not None and s.stage == "seek_phase"
    assert s.data["setup_generation"] == phase_fleet.kube.obj["metadata"]["generation"]


def test_manual_approval_retry_keeps_the_original_follow_deadline(
    phase_fleet: Fleet,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cell = spec_model.Cell("force_rollout", "ReadyToPromote", "keep", None)
    start_cell(phase_fleet, monkeypatch, cell)
    phase_watch(phase_fleet, monkeypatch, "ReadyToPromote")
    assert steps.step(phase_fleet.ctx()) == "patched"
    assert steps.step(phase_fleet.ctx()) == "patched"
    s = journal.load(phase_fleet.db)
    assert s is not None
    deadline = s.deadline
    phase_fleet.now[0] = 100
    phase_fleet.kube.failure = "after"
    assert steps.step(phase_fleet.ctx()) == "unacknowledged"
    assert steps.step(phase_fleet.ctx()) == "patched"
    resumed = journal.load(phase_fleet.db)
    assert resumed is not None and resumed.deadline == deadline
    assert len(phase_fleet.kube.calls) == 3
    assert phase_fleet.kube.calls[-1] == {
        "forcePromote": phase_fleet.kube.obj["spec"]["requestRollout"]
    }


def test_interrupted_cell_kill_does_not_delete_a_replacement_again(
    phase_fleet: Fleet,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cell = spec_model.Cell("force_rollout", "Applied", "keep", "orchestratord")
    start_cell(phase_fleet, monkeypatch, cell)

    def deleted(*args: Any, before_delete: Any) -> None:
        before_delete()
        raise KeyboardInterrupt("driver died after its delete")

    delete = Mock(side_effect=deleted)
    monkeypatch.setattr(r, "execute_kill", delete)
    with pytest.raises(KeyboardInterrupt):
        steps.step(phase_fleet.ctx())
    s = journal.load(phase_fleet.db)
    assert s is not None and s.kill_attempted and s.stage == "follow_cell"
    phase_fleet.kube.phase("Applied")
    assert steps.step(phase_fleet.ctx()) == "settled"
    delete.assert_called_once()

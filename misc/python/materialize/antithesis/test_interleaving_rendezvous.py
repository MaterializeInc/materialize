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
import json
import sqlite3
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import rollout_steps, spec_model
from materialize.antithesis.drivers import history, pod_restarts, rollouts


@pytest.fixture
def rendezvous(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    class Kube:
        def __init__(self):
            self.env = SimpleNamespace(endpoints=Mock())
            self.namespace = "ns"
            self.obj = {
                "metadata": {"generation": 1},
                "spec": {
                    "requestRollout": "initial",
                    "rolloutStrategy": "WaitUntilReady",
                },
                "status": {
                    "activeGeneration": 5,
                    "resourceId": "environment",
                    "lastCompletedRolloutRequest": "initial",
                    "conditions": [{"type": "UpToDate", "reason": "Applied"}],
                },
            }
            self.calls = []
            self.lose_response = False

        def snapshot(self):
            return rollouts.Snapshot(copy.deepcopy(self.obj))

        try_snapshot = snapshot

        def patch_spec(self, changes):
            self.calls.append(copy.deepcopy(changes))
            self.obj["spec"] = spec_model.merge_patch(self.obj["spec"], changes)
            self.obj["metadata"]["generation"] += 1
            if self.lose_response:
                self.lose_response = False
                raise OSError("response lost after patch")
            return copy.deepcopy(self.obj)

        def phase(self, reason):
            self.obj["status"]["conditions"] = [
                {
                    "type": "UpToDate",
                    "reason": reason,
                    "observedGeneration": self.obj["metadata"]["generation"],
                }
            ]

    monkeypatch.setattr(
        rollouts, "open_db", lambda *args: sqlite3.connect(tmp_path / "requests.db")
    )
    monkeypatch.setattr(pod_restarts.templates, "enabled_driver", lambda *args: True)
    monkeypatch.setattr(pod_restarts, "quiet_period_active", lambda *args: False)
    monkeypatch.setattr(pod_restarts.rng, "random", lambda: 0.0)
    monkeypatch.setattr(pod_restarts.rng, "choice", lambda items: items[0])
    return Kube()


def test_victim_is_selected_inside_the_staged_write(monkeypatch):
    monkeypatch.setattr(pod_restarts.templates, "enabled_driver", lambda *args: False)
    monkeypatch.setattr(pod_restarts, "quiet_period_active", lambda *args: False)
    monkeypatch.setattr(pod_restarts.rng, "sample", lambda items, n: ["clusterd"])
    pick = Mock(
        return_value=pod_restarts.Victim("clusterd", "ns", "new-pod", "new-uid", "u2")
    )
    delete = Mock()
    monkeypatch.setattr(pod_restarts, "pick_victim", pick)
    monkeypatch.setattr(pod_restarts, "delete_recorded", delete)
    action = pod_restarts.interleaving_disruption(
        Mock(), cluster_id="u2", deadline=time.monotonic() + 5
    )
    assert action is not None
    pick.assert_not_called()
    action()
    assert delete.call_args.kwargs["uid"] == "new-uid"


def test_owned_rollout_resumes_the_exact_request_after_a_lost_patch_response(
    rendezvous,
):
    rendezvous.lose_response = True
    deadline = time.monotonic() + 5
    assert (
        pod_restarts.interleaving_disruption(
            rendezvous, cluster_id="u2", deadline=deadline
        )
        is None
    )
    requested = rendezvous.obj["spec"]["requestRollout"]
    assert requested != "initial"
    assert rendezvous.obj["spec"]["rolloutStrategy"] == "ManuallyPromote"
    rendezvous.phase("Applying")
    assert (
        pod_restarts.interleaving_disruption(
            rendezvous, cluster_id="u2", deadline=deadline
        )
        is None
    )
    rendezvous.phase("ReadyToPromote")
    assert (
        pod_restarts.interleaving_disruption(
            rendezvous, cluster_id="u2", deadline=deadline
        )
        is not None
    )
    assert [p["requestRollout"] for p in rendezvous.calls if "requestRollout" in p] == [
        requested
    ]


def test_foreign_scenario_is_never_promoted_or_replaced(rendezvous, monkeypatch):
    db = rollouts.requests_db(rendezvous.env)
    rollout_steps.initialize(db)
    foreign = rollout_steps.Scenario(
        "foreign", "manual_promote", rendezvous.obj, time.monotonic(), False
    )
    rollout_steps.save(db, foreign)
    db.close()
    rendezvous.phase("ReadyToPromote")
    monkeypatch.setattr(pod_restarts, "pick_victim", Mock(return_value=None))
    action = pod_restarts.interleaving_disruption(
        rendezvous, cluster_id="u2", deadline=time.monotonic() + 5
    )
    if action is not None:
        action()
    assert rendezvous.calls == []
    db = rollouts.requests_db(rendezvous.env)
    retained = rollout_steps.load(db)
    assert retained is not None and retained.scenario_id == "foreign"
    db.close()


def test_commit_race_begins_after_target_selection():
    committed = threading.Event()
    sequence = []

    def write(before_commit):
        sequence.append("staged")
        before_commit()
        committed.set()

    def effect(pending, release_commit):
        assert pending()
        assert not committed.is_set()
        sequence.append("selected")
        release_commit()
        assert committed.wait(1)
        sequence.append("delete")

    action = pod_restarts.InterleavingDisruption(effect)
    assert history._overlap_write(
        write, action, race_commit=True, deadline=time.monotonic() + 2
    )
    assert sequence == ["staged", "selected", "delete"]


@pytest.mark.parametrize("completed_during_capture", [False, True])
def test_delete_commit_race_waits_for_evidence_and_abandons_finished_sql(
    monkeypatch, completed_during_capture
):
    events = []
    pending = [True]
    db = sqlite3.connect(":memory:")
    monkeypatch.setattr(pod_restarts, "open_db", lambda *args: db)

    def capture(*args, **kwargs):
        events.append("capture")
        if completed_during_capture:
            pending[0] = False

    monkeypatch.setattr(pod_restarts.recovery, "preserve_pod_exit", capture)
    kube = Mock()
    kube.core.delete_namespaced_pod.side_effect = lambda *args, **kwargs: events.append(
        "delete"
    )
    deleted = pod_restarts.delete_recorded(
        kube,
        origin="test",
        role="clusterd",
        namespace="ns",
        pod="pod",
        uid="uid",
        grace=0,
        reason="race",
        before_delete=lambda: events.append("release"),
        still_pending=lambda: pending[0],
    )
    assert deleted is not completed_during_capture
    assert events == (
        ["capture"] if completed_during_capture else ["capture", "release", "delete"]
    )


@pytest.mark.parametrize("expires", [False, True])
def test_delete_abandons_sql_that_finishes_during_restart_journaling(
    monkeypatch, expires
):
    pending, now = [True], [1.0]

    class Journal(sqlite3.Connection):
        def execute(self, statement, *args, **kwargs):
            result = super().execute(statement, *args, **kwargs)
            if statement.startswith("INSERT INTO restarts"):
                if expires:
                    now[0] = 10.0
                else:
                    pending[0] = False
            return result

        def close(self):
            pass

    db = sqlite3.connect(":memory:", factory=Journal)
    monkeypatch.setattr(pod_restarts, "open_db", lambda *args: db)
    monkeypatch.setattr(pod_restarts.time, "monotonic", lambda: now[0])
    observed = Mock()
    monkeypatch.setattr(pod_restarts.telemetry, "fault_observed", observed)
    release, kube = Mock(), Mock()
    assert not pod_restarts.delete_recorded(
        kube,
        origin="test",
        role="clusterd",
        namespace="ns",
        pod="pod",
        uid=None,
        grace=0,
        reason="race",
        deadline=10.0,
        before_delete=release,
        still_pending=lambda: pending[0],
    )
    release.assert_not_called()
    kube.core.delete_namespaced_pod.assert_not_called()
    observed.assert_not_called()
    assert db.execute("SELECT outcome FROM restarts").fetchall() == [("cancelled",)]
    sqlite3.Connection.close(db)


def test_ready_callback_rejects_a_changed_request_generation(rendezvous):
    deadline = time.monotonic() + 5
    assert (
        pod_restarts.interleaving_disruption(
            rendezvous, cluster_id="u2", deadline=deadline
        )
        is None
    )
    rendezvous.phase("ReadyToPromote")
    action = pod_restarts.interleaving_disruption(
        rendezvous, cluster_id="u2", deadline=deadline
    )
    assert action is not None
    rendezvous.obj["metadata"]["generation"] += 1
    action()
    assert len(rendezvous.calls) == 1


def test_candidate_delete_is_journaled_once_and_uses_the_staged_uid(
    rendezvous, monkeypatch
):
    monkeypatch.setattr(
        pod_restarts.rng,
        "choice",
        lambda items: (
            "environmentd_candidate" if "environmentd_candidate" in items else items[0]
        ),
    )
    deadline = time.monotonic() + 5
    assert (
        pod_restarts.interleaving_disruption(
            rendezvous, cluster_id="u2", deadline=deadline
        )
        is None
    )
    rendezvous.phase("ReadyToPromote")
    action = pod_restarts.interleaving_disruption(
        rendezvous, cluster_id="u2", deadline=deadline
    )
    assert action is not None
    pick = Mock(
        return_value=pod_restarts.Victim(
            "environmentd_candidate", "ns", "pod", "live-uid", generation=6
        )
    )
    delete = Mock()
    monkeypatch.setattr(pod_restarts, "pick_victim", pick)
    monkeypatch.setattr(pod_restarts, "delete_recorded", delete)
    pick.assert_not_called()
    action()
    action()
    assert delete.call_count == 1
    assert delete.call_args.kwargs["uid"] == "live-uid"
    assert delete.call_args.kwargs["victim_generation"] == 6


def test_completed_sql_does_not_trigger_a_late_owned_promotion(rendezvous):
    deadline = time.monotonic() + 5
    pod_restarts.interleaving_disruption(rendezvous, cluster_id="u2", deadline=deadline)
    rendezvous.phase("ReadyToPromote")
    action = pod_restarts.interleaving_disruption(
        rendezvous, cluster_id="u2", deadline=deadline
    )
    assert action is not None
    action.run(lambda: False, Mock())
    assert len(rendezvous.calls) == 1


def test_sql_completion_during_patch_journaling_cancels_the_promotion(rendezvous):
    deadline = time.monotonic() + 5
    pod_restarts.interleaving_disruption(rendezvous, cluster_id="u2", deadline=deadline)
    rendezvous.phase("ReadyToPromote")
    action = pod_restarts.interleaving_disruption(
        rendezvous, cluster_id="u2", deadline=deadline
    )
    assert action is not None
    checks = []

    def pending():
        checks.append(True)
        return len(checks) < 4

    release = Mock()
    action.run(pending, release)
    release.assert_not_called()
    assert len(rendezvous.calls) == 1


def test_resumption_does_not_adopt_a_changed_spec_under_the_same_request(rendezvous):
    deadline = time.monotonic() + 5
    pod_restarts.interleaving_disruption(rendezvous, cluster_id="u2", deadline=deadline)
    rendezvous.obj["spec"]["environmentdImageRef"] = "foreign-image"
    rendezvous.phase("ReadyToPromote")
    assert (
        pod_restarts.interleaving_disruption(
            rendezvous, cluster_id="u2", deadline=deadline
        )
        is None
    )
    assert len(rendezvous.calls) == 1


def test_cleanup_restores_only_the_owned_manual_request(rendezvous, monkeypatch):
    deadline = time.monotonic() + 5
    pod_restarts.interleaving_disruption(rendezvous, cluster_id="u2", deadline=deadline)
    rendezvous.phase("ReadyToPromote")
    action = pod_restarts.interleaving_disruption(
        rendezvous, cluster_id="u2", deadline=deadline
    )
    assert action is not None
    original = rendezvous.patch_spec

    def patch(changes):
        result = original(changes)
        if "forcePromote" in changes:
            rendezvous.phase("Promoting")
        return result

    rendezvous.patch_spec = patch
    phase = Mock()
    monkeypatch.setattr(pod_restarts.telemetry, "phase_observed", phase)
    action()
    assert phase.call_count == 1
    assert (
        pod_restarts.interleaving_disruption(
            rendezvous, cluster_id="u2", deadline=deadline
        )
        is None
    )
    assert rendezvous.obj["spec"]["rolloutStrategy"] == "WaitUntilReady"
    assert (
        rendezvous.calls[-1]["forcePromote"] == rendezvous.obj["spec"]["requestRollout"]
    )


def test_disrupted_history_write_retries_the_same_operation_after_a_failed_read(
    monkeypatch,
):
    db = sqlite3.connect(":memory:")
    monkeypatch.setattr(history.state, "open_db", lambda *args: db)
    history.open_state()
    with db:
        db.execute(
            "INSERT INTO ops (epoch, kind, session, isolation, endpoint, invoke_rt, complete_rt, outcome, wkey) VALUES (7, 'w', 'writer', 'strict serializable', 'service', 1, 2, 'indeterminate', 1)"
        )
        db.execute("INSERT INTO interleaving_followups (write_id) VALUES (1)")
    selected = []

    def read(db_arg, session, epoch, txn_prob, *, paths):
        selected.append((epoch, {p.key for p in paths}))
        session.name = "reader"
        outcome = "indeterminate" if len(selected) == 1 else "ok"
        with db_arg:
            db_arg.execute(
                "INSERT INTO ops (epoch, kind, session, isolation, endpoint, invoke_rt, complete_rt, outcome, observed) VALUES (?, 'r', 'reader', 'strict serializable', 'service', 3, 4, ?, ?)",
                (epoch, outcome, json.dumps([[1, "materialized_view", [1], []]])),
            )

    monkeypatch.setattr(history, "_read", read)
    check = Mock(return_value=[])
    monkeypatch.setattr(history, "_check_epoch", check)
    monkeypatch.setattr(history, "_assert_timestamp_order", Mock())
    monkeypatch.setattr(history, "_coverage_cluster_id", lambda *args: "u2")
    assert history._follow_up_interleaving(db, "host", 1, time.monotonic() + 2)
    assert selected == [(7, {1}), (7, {1})]
    assert db.execute("SELECT * FROM interleaving_followups").fetchall() == []
    assert check.call_args.args[:2] == (db, 7)
    db.close()


def test_an_absent_indeterminate_write_stays_pending_until_a_later_read_observes_it(
    monkeypatch,
):
    db = sqlite3.connect(":memory:")
    monkeypatch.setattr(history.state, "open_db", lambda *args: db)
    history.open_state()
    with db:
        db.execute(
            "INSERT INTO ops (epoch, kind, session, isolation, endpoint, invoke_rt, complete_rt, outcome, wkey) VALUES (7, 'w', 'writer', 'strict serializable', 'service', 1, 2, 'indeterminate', 1)"
        )
        db.execute("INSERT INTO interleaving_followups (write_id) VALUES (1)")
    seen = []

    def read(db_arg, session, epoch, txn_prob, *, paths):
        session.name = "reader"
        values = [] if not seen else [1]
        seen.append((epoch, {path.key for path in paths}))
        with db_arg:
            db_arg.execute(
                "INSERT INTO ops (epoch, kind, session, isolation, endpoint, invoke_rt, complete_rt, outcome, observed) VALUES (?, 'r', 'reader', 'strict serializable', 'service', 3, 4, 'ok', ?)",
                (epoch, json.dumps([[1, "materialized_view", values, []]])),
            )

    monkeypatch.setattr(history, "_read", read)
    monkeypatch.setattr(history, "_check_epoch", Mock(return_value=[]))
    monkeypatch.setattr(history, "_assert_timestamp_order", Mock())
    monkeypatch.setattr(history, "_coverage_cluster_id", lambda *args: "u2")
    deadline = time.monotonic() + 2
    assert history._follow_up_interleaving(db, "host", 1, deadline)
    assert db.execute("SELECT * FROM interleaving_followups").fetchall() == [(1, 1)]
    assert history._follow_up_interleaving(db, "host", 1, deadline)
    assert db.execute("SELECT * FROM interleaving_followups").fetchall() == []
    assert seen == [(7, {1}), (7, {1})]
    db.close()


@pytest.mark.parametrize("attempts", [0, history.INTERLEAVING_FOLLOWUP_ATTEMPTS])
def test_pending_identity_has_a_bounded_priority_over_new_disruptions(
    monkeypatch, attempts
):
    db = sqlite3.connect(":memory:")
    monkeypatch.setattr(history.state, "open_db", lambda *args: db)
    history.open_state()
    with db:
        db.execute("INSERT INTO interleaving_followups VALUES (1, ?)", (attempts,))
    env = Mock()
    env.sql_host.return_value = "host"
    monkeypatch.setattr(history, "Environment", lambda: env)
    monkeypatch.setattr(history, "start_watchdog", Mock())
    monkeypatch.setattr(history.quiet, "active_quiet_period", lambda: False)
    monkeypatch.setattr(history, "_epoch_for_op", Mock(return_value=1))
    monkeypatch.setattr(
        history.telemetry, "resolve_cluster_id", Mock(return_value="u2")
    )
    follow_up = Mock(return_value=True)
    selection = Mock(return_value=None)
    monkeypatch.setattr(history, "_follow_up_interleaving", follow_up)
    monkeypatch.setattr(pod_restarts, "interleaving_disruption", selection)
    monkeypatch.setattr(rollouts, "Kube", Mock())
    assert history.interleaving_driver() == 0
    if attempts == 0:
        follow_up.assert_called_once()
        selection.assert_not_called()
    else:
        follow_up.assert_not_called()
        selection.assert_called_once()


@pytest.mark.parametrize("observed", [False, True])
def test_resumed_followup_emits_its_own_operation_identity(monkeypatch, observed):
    db = sqlite3.connect(":memory:")
    monkeypatch.setattr(history.state, "open_db", lambda *args: db)
    history.open_state()
    with db:
        db.execute(
            "INSERT INTO ops (epoch, kind, session, isolation, endpoint, invoke_rt, complete_rt, outcome, wkey) VALUES (7, 'w', 'writer', 'strict serializable', 'service', 1, 2, 'indeterminate', 1)"
        )
        db.execute("INSERT INTO interleaving_followups (write_id) VALUES (1)")
    env = Mock()
    env.sql_host.return_value = "host"
    monkeypatch.setattr(history, "Environment", lambda: env)
    monkeypatch.setattr(history, "start_watchdog", Mock())
    monkeypatch.setattr(history.quiet, "active_quiet_period", lambda: False)
    monkeypatch.setattr(history, "_epoch_for_op", Mock(return_value=None))
    monkeypatch.setattr(history, "_check_epoch", Mock(return_value=[]))
    monkeypatch.setattr(history, "_assert_timestamp_order", Mock())
    monkeypatch.setattr(history, "_coverage_cluster_id", lambda *args: "u2")
    monkeypatch.setattr(
        history.telemetry,
        "_recorder",
        history.telemetry.Recorder(
            "transactions", "parallel_driver_history_interleavings.py"
        ),
    )
    send = Mock()
    monkeypatch.setattr(history.telemetry.observations, "send_event", send)

    def read(db_arg, session, epoch, txn_prob, *, paths):
        session.name = "reader"
        with db_arg:
            db_arg.execute(
                "INSERT INTO ops (epoch, kind, session, isolation, endpoint, invoke_rt, complete_rt, outcome, observed) VALUES (?, 'r', 'reader', 'strict serializable', 'service', 3, 4, 'ok', ?)",
                (
                    epoch,
                    json.dumps([[1, "materialized_view", [1] if observed else [], []]]),
                ),
            )

    monkeypatch.setattr(history, "_read", read)
    assert history.interleaving_driver() == 0
    events = [
        call.args[1]
        for call in send.call_args_list
        if call.args[0] == "antithesis_interleaving"
    ]
    assert [event["stage"] for event in events] == ["start", "checked", "end"]
    assert len({event["round"] for event in events}) == 1
    for event in events:
        assert (event["mode"], event["write_id"], event["epoch"], event["key"]) == (
            "resume_history",
            1,
            7,
            1,
        )
        assert event["profile"] == "transactions"
        assert event["command"] == "parallel_driver_history_interleavings.py"
    assert events[1]["observed_write"] is observed
    assert events[1]["successful_reads"] == 1 and events[1]["read_id"] == 2
    assert events[2]["outcome"] == "completed"

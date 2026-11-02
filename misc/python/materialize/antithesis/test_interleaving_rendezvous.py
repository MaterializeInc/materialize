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
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import rollout_steps, spec_model
from materialize.antithesis.drivers import history, pod_restarts, rollouts


@pytest.fixture
def active_replica(monkeypatch, tmp_path):
    obj = {
        "metadata": {"uid": "cr-uid", "generation": 13},
        "spec": {"requestRollout": "owned-request"},
        "status": {
            "resourceId": "environment",
            "activeGeneration": 6,
            "conditions": [
                {"type": "UpToDate", "observedGeneration": 13, "reason": "Applying"}
            ],
        },
    }

    def pod(
        name, generation="6", cluster="u2", resource="environment", uid="active-uid"
    ):
        return SimpleNamespace(
            metadata=SimpleNamespace(
                name=name,
                uid=uid,
                deletion_timestamp=None,
                labels={
                    "cluster.environmentd.materialize.cloud/cluster-id": cluster,
                    "cluster.environmentd.materialize.cloud/generation": generation,
                    "materialize.cloud/mz-resource-id": resource,
                },
            )
        )

    core = Mock()
    active = pod("active")
    core.list_namespaced_pod.return_value.items = [
        pod("candidate", "7"),
        pod("foreign", cluster="u9"),
        pod("other-environment", resource="other"),
        active,
    ]
    core.read_namespaced_pod.return_value = copy.deepcopy(active)
    kube = SimpleNamespace(
        env=SimpleNamespace(endpoints=Mock()),
        namespace="ns",
        operator_namespace="operator",
        core=core,
        try_snapshot=lambda: rollouts.Snapshot(copy.deepcopy(obj)),
        patch_spec=Mock(),
    )
    monkeypatch.setattr(pod_restarts, "quiet_period_active", lambda _: False)
    monkeypatch.setattr(pod_restarts.time, "monotonic", lambda: 10.0)
    monkeypatch.setattr(
        pod_restarts.rng,
        "choice",
        lambda items: (
            "active_replica"
            if items == ("rollout_or_pod", "active_replica")
            else items[0]
        ),
    )
    monkeypatch.setattr(
        pod_restarts,
        "_owned_interleaving",
        Mock(side_effect=AssertionError("independent arm touched rollout ownership")),
    )
    monkeypatch.setattr(
        pod_restarts, "open_db", lambda *args: sqlite3.connect(tmp_path / "faults.db")
    )
    monkeypatch.setattr(pod_restarts.recovery, "preserve_pod_exit", Mock())
    monkeypatch.setattr(pod_restarts.telemetry, "interleaving_progress", Mock())
    monkeypatch.setattr(pod_restarts.telemetry, "fault_observed", Mock())
    return SimpleNamespace(kube=kube, obj=obj, active=active, pod=pod)


def test_active_replica_arm_ignores_owned_preparation_and_only_deletes_scoped_active_uid(
    active_replica,
):
    f = active_replica
    action = pod_restarts.interleaving_disruption(f.kube, cluster_id="u2", deadline=30)
    assert action is not None
    release = Mock()
    action.run(lambda: True, release)
    call = f.kube.core.delete_namespaced_pod.call_args
    assert call.args == ("active", "ns")
    assert call.kwargs["body"].preconditions.uid == "active-uid"
    assert call.kwargs["_request_timeout"] > 0
    release.assert_called_once()
    f.kube.patch_spec.assert_not_called()
    pod_restarts._owned_interleaving.assert_not_called()


@pytest.mark.parametrize(
    "missing", ["active", "resource", "uid", "generation", "request", "observed"]
)
def test_active_replica_arm_does_not_start_sql_without_current_generation_identity(
    active_replica, missing
):
    f = active_replica
    if missing == "active":
        f.obj["status"].pop("activeGeneration")
    elif missing == "resource":
        f.obj["status"].pop("resourceId")
    elif missing in ("uid", "generation"):
        f.obj["metadata"].pop(missing)
    elif missing == "request":
        f.obj["spec"].pop("requestRollout")
    else:
        f.obj["status"]["conditions"][0]["observedGeneration"] = 12
    assert (
        pod_restarts.interleaving_disruption(f.kube, cluster_id="u2", deadline=30)
        is None
    )
    f.kube.core.list_namespaced_pod.assert_not_called()
    f.kube.core.delete_namespaced_pod.assert_not_called()


@pytest.mark.parametrize(
    "changed", ["active", "resource", "uid", "generation", "request", "observed"]
)
def test_active_replica_arm_revalidates_cr_identity_inside_unresolved_sql(
    active_replica, changed
):
    f = active_replica
    action = pod_restarts.interleaving_disruption(f.kube, cluster_id="u2", deadline=30)
    assert action is not None
    if changed == "active":
        f.obj["status"]["activeGeneration"] = 7
    elif changed == "resource":
        f.obj["status"]["resourceId"] = "other"
    elif changed == "uid":
        f.obj["metadata"]["uid"] = "replacement-cr"
    elif changed == "generation":
        f.obj["metadata"]["generation"] = 14
        f.obj["status"]["conditions"][0]["observedGeneration"] = 14
    elif changed == "request":
        f.obj["spec"]["requestRollout"] = "superseding-request"
    else:
        f.obj["status"]["conditions"][0]["observedGeneration"] = 12
    action()
    f.kube.core.delete_namespaced_pod.assert_not_called()


@pytest.mark.parametrize(
    "kind", ["candidate", "cluster", "resource", "uid", "deleting", "generation"]
)
def test_active_replica_arm_never_selects_unproven_pods(active_replica, kind):
    f = active_replica
    pod = copy.deepcopy(f.active)
    if kind == "candidate":
        pod.metadata.labels["cluster.environmentd.materialize.cloud/generation"] = "7"
    elif kind == "cluster":
        pod.metadata.labels["cluster.environmentd.materialize.cloud/cluster-id"] = "u9"
    elif kind == "resource":
        pod.metadata.labels["materialize.cloud/mz-resource-id"] = "other"
    elif kind == "uid":
        pod.metadata.uid = None
    elif kind == "deleting":
        pod.metadata.deletion_timestamp = "now"
    else:
        pod.metadata.labels.pop("cluster.environmentd.materialize.cloud/generation")
    f.kube.core.list_namespaced_pod.return_value.items = [pod]
    action = pod_restarts.interleaving_disruption(f.kube, cluster_id="u2", deadline=30)
    assert action is not None
    action()
    f.kube.core.read_namespaced_pod.assert_not_called()
    f.kube.core.delete_namespaced_pod.assert_not_called()


def test_active_replica_arm_revalidates_listed_uid_before_delete(active_replica):
    f = active_replica
    f.kube.core.read_namespaced_pod.return_value.metadata.uid = "replacement-uid"
    action = pod_restarts.interleaving_disruption(f.kube, cluster_id="u2", deadline=30)
    assert action is not None
    action()
    f.kube.core.delete_namespaced_pod.assert_not_called()


def test_active_replica_arm_keeps_generation_guard_through_delete_preflight(
    active_replica, monkeypatch
):
    f = active_replica

    def promote(*args, **kwargs):
        f.obj["status"]["activeGeneration"] = 7

    monkeypatch.setattr(pod_restarts.recovery, "preserve_pod_exit", promote)
    action = pod_restarts.interleaving_disruption(f.kube, cluster_id="u2", deadline=30)
    assert action is not None
    action()
    f.kube.core.delete_namespaced_pod.assert_not_called()


@pytest.mark.parametrize("guard", ["pending", "quiet", "deadline"])
def test_active_replica_arm_preserves_pending_quiet_and_deadline_guards(
    active_replica, monkeypatch, guard
):
    f = active_replica
    action = pod_restarts.interleaving_disruption(f.kube, cluster_id="u2", deadline=30)
    assert action is not None
    if guard == "quiet":
        monkeypatch.setattr(pod_restarts, "quiet_period_active", lambda _: True)
    elif guard == "deadline":
        monkeypatch.setattr(pod_restarts.time, "monotonic", lambda: 31)
    release = Mock()
    action.run(lambda: guard != "pending", release)
    f.kube.core.delete_namespaced_pod.assert_not_called()
    release.assert_not_called()


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
            self.now = [10.0]
            self.pauses: list[float] = []
            self.on_pause: Callable[[], None] = lambda: None

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
    kube = Kube()

    def sleep(seconds):
        kube.now[0] += seconds
        kube.pauses.append(seconds)
        kube.on_pause()

    monkeypatch.setattr(pod_restarts.time, "monotonic", lambda: kube.now[0])
    monkeypatch.setattr(pod_restarts.time, "sleep", sleep)
    return kube


def prepare_rendezvous(kube):
    assert (
        pod_restarts.interleaving_disruption(
            kube, cluster_id="u2", deadline=time.monotonic() + 1
        )
        is None
    )
    return time.monotonic() + 5


def test_owned_preparation_waits_for_ready_without_releasing_its_lease(
    rendezvous, monkeypatch
):
    competing = []

    def advance():
        lease = rollouts.CrLease(rendezvous.env)
        competing.append(lease.acquire())
        lease.db.close()
        rendezvous.phase("ReadyToPromote")

    rendezvous.on_pause = advance
    monkeypatch.setattr(
        history,
        "Session",
        Mock(side_effect=AssertionError("SQL opened while preparing rollout")),
    )
    action = pod_restarts.interleaving_disruption(
        rendezvous, cluster_id="u2", deadline=time.monotonic() + 5
    )
    assert action is not None
    assert competing == [False]
    assert len(rendezvous.calls) == 1
    assert rendezvous.obj["spec"]["rolloutStrategy"] == "ManuallyPromote"


def test_competing_interleaver_does_not_delete_a_candidate_during_owned_preparation(
    rendezvous, monkeypatch
):
    delete = Mock(return_value=True)
    monkeypatch.setattr(pod_restarts, "delete_recorded", delete)
    monkeypatch.setattr(pod_restarts.telemetry, "phase_observed", Mock())
    monkeypatch.setattr(
        pod_restarts.rng, "sample", lambda items, n: ["environmentd_candidate"]
    )
    monkeypatch.setattr(
        pod_restarts,
        "pick_victim",
        Mock(
            return_value=pod_restarts.Victim(
                "environmentd_candidate",
                "ns",
                "candidate",
                "candidate-uid",
                generation=6,
            )
        ),
    )
    competing = []

    def advance():
        other = pod_restarts.interleaving_disruption(
            rendezvous, cluster_id="u2", deadline=time.monotonic() + 5
        )
        competing.append(other)
        if other is not None:
            other()
        rendezvous.phase("ReadyToPromote")

    rendezvous.on_pause = advance
    action = pod_restarts.interleaving_disruption(
        rendezvous, cluster_id="u2", deadline=time.monotonic() + 5
    )
    assert action is not None
    delete.assert_not_called()
    assert competing == [None]
    assert len(rendezvous.calls) == 1


@pytest.mark.parametrize("owner", [None, "rollouts", "expired_interleavings"])
def test_contended_lease_keeps_fallback_for_unrelated_or_expired_trials(
    rendezvous, monkeypatch, owner
):
    db = rollouts.requests_db(rendezvous.env)
    rollout_steps.initialize(db)
    if owner is not None:
        scenario = rollout_steps.Scenario(
            "other",
            "manual_promote",
            rendezvous.obj,
            time.monotonic(),
            False,
            owner="interleavings" if owner == "expired_interleavings" else owner,
            deadline=(
                time.monotonic() - 1
                if owner == "expired_interleavings"
                else time.monotonic() + 10
            ),
        )
        rollout_steps.save(db, scenario)
    lease = rollouts.CrLease(rendezvous.env)
    assert lease.acquire()
    try:
        delete = Mock(return_value=True)
        monkeypatch.setattr(pod_restarts, "delete_recorded", delete)
        monkeypatch.setattr(pod_restarts.telemetry, "phase_observed", Mock())
        monkeypatch.setattr(pod_restarts.rng, "sample", lambda items, n: ["clusterd"])
        monkeypatch.setattr(
            pod_restarts,
            "pick_victim",
            Mock(
                return_value=pod_restarts.Victim(
                    "clusterd", "ns", "replica", "uid", "u2"
                )
            ),
        )
        action = pod_restarts.interleaving_disruption(
            rendezvous, cluster_id="u2", deadline=time.monotonic() + 5
        )
        assert action is not None
        action()
        delete.assert_called_once()
        retained = rollout_steps.load(db)
        assert (retained is None) == (owner is None)
        if retained is not None:
            assert retained.scenario_id == "other"
    finally:
        lease.release()
        lease.db.close()
        db.close()


def test_owned_preparation_stops_for_quiet_and_resumes_the_same_request(
    rendezvous, monkeypatch
):
    quiet = [False]
    monkeypatch.setattr(pod_restarts, "quiet_period_active", lambda *args: quiet[0])
    rendezvous.on_pause = lambda: quiet.__setitem__(0, True)
    deadline = time.monotonic() + 5
    assert (
        pod_restarts.interleaving_disruption(
            rendezvous, cluster_id="u2", deadline=deadline
        )
        is None
    )
    assert quiet[0]
    requested = rendezvous.obj["spec"]["requestRollout"]
    assert requested != "initial"
    quiet[0] = False
    rendezvous.phase("ReadyToPromote")
    assert (
        pod_restarts.interleaving_disruption(
            rendezvous, cluster_id="u2", deadline=deadline
        )
        is not None
    )
    assert rendezvous.obj["spec"]["requestRollout"] == requested
    assert len(rendezvous.calls) == 1


@pytest.mark.parametrize("changed", ["request", "spec", "resource", "generation"])
def test_owned_preparation_does_not_adopt_a_superseding_request(rendezvous, changed):
    def supersede():
        if changed == "request":
            rendezvous.obj["spec"]["requestRollout"] = "foreign"
        elif changed == "spec":
            rendezvous.obj["spec"]["environmentdImageRef"] = "foreign-image"
        elif changed == "resource":
            rendezvous.obj["status"]["resourceId"] = "foreign-environment"
        else:
            rendezvous.obj["metadata"]["generation"] += 1
        rendezvous.phase("ReadyToPromote")

    rendezvous.on_pause = supersede
    assert (
        pod_restarts.interleaving_disruption(
            rendezvous, cluster_id="u2", deadline=time.monotonic() + 5
        )
        is None
    )
    if changed == "request":
        assert rendezvous.obj["spec"]["requestRollout"] == "foreign"
    assert len(rendezvous.calls) == 1
    db = rollouts.requests_db(rendezvous.env)
    assert rollout_steps.load(db) is None
    db.close()


def test_owned_preparation_fences_itself_after_losing_the_lease(
    rendezvous, monkeypatch
):
    lost = [False]
    original = rollouts.CrLease.renew

    def renew(lease):
        if lost[0]:
            raise rollouts.LeaseLost("another invocation owns this rollout")
        return original(lease)

    monkeypatch.setattr(rollouts.CrLease, "renew", renew)
    rendezvous.on_pause = lambda: lost.__setitem__(0, True)
    assert (
        pod_restarts.interleaving_disruption(
            rendezvous, cluster_id="u2", deadline=time.monotonic() + 5
        )
        is None
    )
    assert lost[0]
    assert len(rendezvous.calls) == 1
    db = rollouts.requests_db(rendezvous.env)
    scenario = rollout_steps.load(db)
    assert scenario is not None and scenario.owner == "interleavings"
    db.close()


def test_victim_is_selected_inside_the_staged_write(monkeypatch):
    monkeypatch.setattr(pod_restarts.rng, "choice", lambda items: items[0])
    monkeypatch.setattr(pod_restarts.templates, "enabled_driver", lambda *args: False)
    monkeypatch.setattr(pod_restarts, "quiet_period_active", lambda *args: False)
    monkeypatch.setattr(pod_restarts.rng, "sample", lambda items, n: ["clusterd"])
    pick = Mock(
        return_value=pod_restarts.Victim("clusterd", "ns", "new-pod", "new-uid", "u2")
    )

    def acknowledged(*args, **kwargs):
        kwargs["before_delete"]()
        return True

    delete = Mock(side_effect=acknowledged)
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
    prepare_rendezvous(rendezvous)
    requested = rendezvous.obj["spec"]["requestRollout"]
    assert requested != "initial"
    assert rendezvous.obj["spec"]["rolloutStrategy"] == "ManuallyPromote"
    rendezvous.phase("Applying")
    assert (
        pod_restarts.interleaving_disruption(
            rendezvous, cluster_id="u2", deadline=time.monotonic() + 1
        )
        is None
    )
    rendezvous.phase("ReadyToPromote")
    assert (
        pod_restarts.interleaving_disruption(
            rendezvous, cluster_id="u2", deadline=time.monotonic() + 5
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


@pytest.mark.parametrize(
    "generation,observed_generation,current",
    [(1, 1, True), (1, 0, False), (1, None, False), (None, None, False)],
)
def test_fallback_delete_records_only_current_forced_phase_samples(
    rendezvous, monkeypatch, generation, observed_generation, current
):
    rendezvous.phase("Promoting")
    rendezvous.obj["metadata"]["generation"] = generation
    rendezvous.obj["status"]["conditions"][0][
        "observedGeneration"
    ] = observed_generation
    monkeypatch.setattr(
        pod_restarts, "_owned_interleaving", lambda *args: (False, None)
    )
    monkeypatch.setattr(pod_restarts.rng, "sample", lambda roles, count: ["clusterd"])
    monkeypatch.setattr(
        pod_restarts,
        "pick_victim",
        Mock(
            return_value=pod_restarts.Victim(
                "clusterd", "namespace", "pod", "uid", "u2"
            )
        ),
    )
    phase = Mock()
    deleted = Mock(return_value=True)
    monkeypatch.setattr(pod_restarts.telemetry, "phase_observed", phase)
    monkeypatch.setattr(pod_restarts, "delete_recorded", deleted)
    action = pod_restarts.interleaving_disruption(
        rendezvous, cluster_id="u2", deadline=time.monotonic() + 5
    )
    assert action is not None
    action.run(lambda: True, Mock())
    deleted.assert_called_once()
    if current:
        phase.assert_called_once_with(
            "Promoting", 5, "initial", started=10.0, force=True
        )
    else:
        phase.assert_not_called()


def test_owned_promotion_cannot_credit_a_snapshot_with_no_generation(
    rendezvous, monkeypatch
):
    deadline = prepare_rendezvous(rendezvous)
    rendezvous.phase("ReadyToPromote")
    action = pod_restarts.interleaving_disruption(
        rendezvous, cluster_id="u2", deadline=deadline
    )
    assert action is not None
    patch_spec = rendezvous.patch_spec

    def patch(changes):
        result = patch_spec(changes)
        if "forcePromote" in changes:
            rendezvous.phase("Promoting")
            rendezvous.obj["metadata"]["generation"] = None
            rendezvous.obj["status"]["conditions"][0]["observedGeneration"] = None
        return result

    rendezvous.patch_spec = patch
    phase = Mock()
    monkeypatch.setattr(pod_restarts.telemetry, "phase_observed", phase)
    action()
    assert len(rendezvous.calls) == 2
    phase.assert_not_called()


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


def test_delete_exposure_starts_after_preparation_and_commit_release(monkeypatch):
    now = [1.0]
    db = sqlite3.connect(":memory:")
    monkeypatch.setattr(pod_restarts, "open_db", lambda *args: db)
    monkeypatch.setattr(pod_restarts.time, "monotonic", lambda: now[0])
    observed = Mock()
    monkeypatch.setattr(pod_restarts.telemetry, "fault_observed", observed)
    kube = Mock()

    def release():
        now[0] = 5.0

    def delete(*args, **kwargs):
        now[0] = 6.0

    kube.core.delete_namespaced_pod.side_effect = delete
    assert pod_restarts.delete_recorded(
        kube,
        origin="test",
        role="clusterd",
        namespace="ns",
        pod="pod",
        uid=None,
        grace=0,
        reason="race",
        cluster_id="u2",
        before_delete=release,
    )
    observed.assert_called_once_with(
        "clusterd", 5.0, 6.0, key="pod_restarts:1", cluster_id="u2"
    )


def test_ready_callback_rejects_a_changed_request_generation(rendezvous):
    deadline = prepare_rendezvous(rendezvous)
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
    deadline = prepare_rendezvous(rendezvous)
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

    def acknowledged(*args, **kwargs):
        kwargs["before_delete"]()
        return True

    delete = Mock(side_effect=acknowledged)
    monkeypatch.setattr(pod_restarts, "pick_victim", pick)
    monkeypatch.setattr(pod_restarts, "delete_recorded", delete)
    pick.assert_not_called()
    action()
    action()
    assert delete.call_count == 1
    assert delete.call_args.kwargs["uid"] == "live-uid"
    assert delete.call_args.kwargs["victim_generation"] == 6


def test_completed_sql_does_not_trigger_a_late_owned_promotion(rendezvous):
    deadline = prepare_rendezvous(rendezvous)
    rendezvous.phase("ReadyToPromote")
    action = pod_restarts.interleaving_disruption(
        rendezvous, cluster_id="u2", deadline=deadline
    )
    assert action is not None
    action.run(lambda: False, Mock())
    assert len(rendezvous.calls) == 1


def test_sql_completion_during_patch_journaling_cancels_the_promotion(rendezvous):
    deadline = prepare_rendezvous(rendezvous)
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
    retry = pod_restarts.interleaving_disruption(
        rendezvous, cluster_id="u2", deadline=deadline
    )
    assert retry is not None
    assert len(rendezvous.calls) == 1


@pytest.mark.parametrize("sent,acked", [(False, False), (True, True), (True, False)])
def test_candidate_rendezvous_retries_only_a_positively_unsent_delete(
    rendezvous, monkeypatch, sent, acked
):
    monkeypatch.setattr(
        pod_restarts.rng,
        "choice",
        lambda items: (
            "environmentd_candidate" if "environmentd_candidate" in items else items[0]
        ),
    )
    deadline = prepare_rendezvous(rendezvous)
    rendezvous.phase("ReadyToPromote")
    action = pod_restarts.interleaving_disruption(
        rendezvous, cluster_id="u2", deadline=deadline
    )
    assert action is not None
    monkeypatch.setattr(
        pod_restarts,
        "pick_victim",
        Mock(
            return_value=pod_restarts.Victim(
                "environmentd_candidate", "ns", "pod", "uid", generation=6
            )
        ),
    )

    def delete(*args, **kwargs):
        if sent:
            kwargs["before_delete"]()
        return acked

    monkeypatch.setattr(pod_restarts, "delete_recorded", delete)
    action()
    retry = pod_restarts.interleaving_disruption(
        rendezvous, cluster_id="u2", deadline=deadline
    )
    if sent:
        assert retry is None
        assert rendezvous.obj["spec"]["rolloutStrategy"] == "WaitUntilReady"
    else:
        assert retry is not None
        assert len(rendezvous.calls) == 1
        assert rendezvous.obj["spec"]["rolloutStrategy"] == "ManuallyPromote"


def test_unsent_retry_cannot_rewrite_the_scenario_after_losing_its_lease(
    rendezvous, monkeypatch
):
    deadline = prepare_rendezvous(rendezvous)
    rendezvous.phase("ReadyToPromote")
    action = pod_restarts.interleaving_disruption(
        rendezvous, cluster_id="u2", deadline=deadline
    )
    assert action is not None
    lost = [False]
    original = rollouts.CrLease.renew

    def renew(lease):
        if lost[0]:
            raise rollouts.LeaseLost("another invocation holds the lease")
        return original(lease)

    monkeypatch.setattr(rollouts.CrLease, "renew", renew)
    checks = []

    def pending():
        checks.append(True)
        if len(checks) >= 4:
            lost[0] = True
            return False
        return True

    action.run(pending, Mock())
    db = rollouts.requests_db(rendezvous.env)
    scenario = rollout_steps.load(db)
    assert scenario is not None and scenario.stage == "cleanup"
    assert len(rendezvous.calls) == 1
    db.close()


def test_an_unavailable_final_promotion_snapshot_preserves_the_ready_trial(
    rendezvous,
):
    deadline = prepare_rendezvous(rendezvous)
    rendezvous.phase("ReadyToPromote")
    action = pod_restarts.interleaving_disruption(
        rendezvous, cluster_id="u2", deadline=deadline
    )
    assert action is not None
    original = rendezvous.try_snapshot
    rendezvous.try_snapshot = Mock(side_effect=[original(), None, None])
    action()
    rendezvous.try_snapshot = original
    assert len(rendezvous.calls) == 1
    retry = pod_restarts.interleaving_disruption(
        rendezvous, cluster_id="u2", deadline=deadline
    )
    assert retry is not None
    assert len(rendezvous.calls) == 1


def test_resumption_does_not_adopt_a_changed_spec_under_the_same_request(rendezvous):
    deadline = prepare_rendezvous(rendezvous)
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
    deadline = prepare_rendezvous(rendezvous)
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


def test_an_earlier_followup_cannot_spend_the_next_ids_budget_without_a_read(
    monkeypatch,
):
    class State(sqlite3.Connection):
        def close(self):
            pass

    db = sqlite3.connect(":memory:", factory=State)
    monkeypatch.setattr(history.state, "open_db", lambda *args: db)
    history.open_state()
    with db:
        for key in (0, 1):
            db.execute(
                "INSERT INTO ops (epoch, kind, session, isolation, endpoint, invoke_rt, complete_rt, outcome, wkey) VALUES (7, 'w', 'writer', 'strict serializable', 'service', 0, 1, 'indeterminate', ?)",
                (key,),
            )
            db.execute(
                "INSERT INTO interleaving_followups (write_id) VALUES (?)", (key + 1,)
            )
    env, now = Mock(), [1.0]
    env.sql_host.return_value = "host"
    monkeypatch.setattr(history, "Environment", lambda: env)
    monkeypatch.setattr(history, "start_watchdog", Mock())
    monkeypatch.setattr(history.quiet, "active_quiet_period", lambda: False)
    monkeypatch.setattr(history.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(history.telemetry, "_recorder", None)
    monkeypatch.setattr(history, "_coverage_cluster_id", lambda *args: "u2")
    monkeypatch.setattr(history, "_assert_timestamp_order", Mock())
    reads = []

    def read(db_arg, session, epoch, txn_prob, *, paths):
        reads.append({path.key for path in paths})
        session.name = "reader"
        with db_arg:
            db_arg.execute(
                "INSERT INTO ops (epoch, kind, session, isolation, endpoint, invoke_rt, complete_rt, outcome, observed) VALUES (?, 'r', 'reader', 'strict serializable', 'service', 1, 2, 'ok', ?)",
                (epoch, json.dumps([[0, "index", [1], []]])),
            )

    def checked(*args, **kwargs):
        now[0] += history.DRIVER_BUDGET_S[1]
        return []

    monkeypatch.setattr(history, "_read", read)
    monkeypatch.setattr(history, "_check_epoch", checked)
    assert history.interleaving_driver() == 0
    assert reads == [{0}]
    assert db.execute("SELECT * FROM interleaving_followups").fetchall() == [(2, 0)]
    sqlite3.Connection.close(db)

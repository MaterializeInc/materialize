# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import quiet
from materialize.antithesis.drivers import canaries, recovery, rollouts


def snapshot(reason: str, request: str = "r0") -> rollouts.Snapshot:
    return rollouts.Snapshot(
        {
            "spec": {"requestRollout": request},
            "status": {
                "conditions": [
                    {"type": "UpToDate", "status": "False", "reason": reason}
                ]
            },
        }
    )


def test_recovery_retries_unacknowledged_normalization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    kube = Mock()
    kube.snapshot.return_value = snapshot("Applying")
    normalize = Mock(side_effect=[False, True])
    monkeypatch.setattr(rollouts, "normalize", normalize)
    unstick = rollouts.OperatorUnstick(Mock(kube=kube), "test")
    unstick.step()
    assert not unstick.normalized
    unstick.step()
    assert unstick.normalized
    assert normalize.call_count == 2


def test_recovery_retries_unacknowledged_fresh_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    kube = Mock()
    kube.snapshot.return_value = snapshot("WaitingForApproval")
    submit = Mock(side_effect=[("r1", None), ("r1", snapshot("Applying", "r1"))])
    monkeypatch.setattr(rollouts, "submit_patch", submit)
    monkeypatch.setattr(
        rollouts, "new_request", lambda: {"requestRollout": "r1", "forceRollout": "r1"}
    )
    unstick = rollouts.OperatorUnstick(Mock(kube=kube), "test", normalized=True)
    assert unstick.step()[1] is False
    assert not unstick.fresh_requested
    assert unstick.step()[1] is True
    assert unstick.fresh_requested
    assert submit.call_count == 2
    assert submit.call_args_list[0] == submit.call_args_list[1]


def test_recovery_reconciles_indeterminate_fresh_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    kube = Mock()
    kube.snapshot.side_effect = [
        snapshot("WaitingForApproval"),
        snapshot("Applying", "r1"),
    ]
    submit = Mock(return_value=("r1", None))
    monkeypatch.setattr(rollouts, "submit_patch", submit)
    monkeypatch.setattr(
        rollouts, "new_request", lambda: {"requestRollout": "r1", "forceRollout": "r1"}
    )
    unstick = rollouts.OperatorUnstick(Mock(kube=kube), "test", normalized=True)
    unstick.step()
    unstick.step()
    assert unstick.fresh_requested
    assert submit.call_count == 1


def test_canary_paths_start_without_waiting_for_a_stalled_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = threading.Barrier(len(canaries.PROBES))

    def probe(host: str, name: str) -> canaries.ProbeResult:
        started.wait(timeout=2)
        return canaries.ProbeResult(name, "ok", True, 0.0)

    monkeypatch.setattr(canaries, "run_probe", probe)
    assert [r.name for r in canaries._run_probes("host")] == list(canaries.PROBES)


def test_current_termination_is_observed_and_not_counted_twice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    term = SimpleNamespace(exit_code=42, reason="Error", signal=0, finished_at="now")
    previous = SimpleNamespace(
        exit_code=166, reason="Error", signal=0, finished_at="before"
    )
    status = SimpleNamespace(
        name="environmentd",
        restart_count=1,
        last_state=SimpleNamespace(terminated=previous),
        state=SimpleNamespace(terminated=term, waiting=None),
    )
    pod = SimpleNamespace(
        metadata=SimpleNamespace(
            name="envd",
            uid="uid",
            labels={"materialize.cloud/app": "environmentd"},
            deletion_timestamp=None,
        ),
        status=SimpleNamespace(container_statuses=[status]),
    )
    core = Mock()
    core.list_namespaced_pod.side_effect = [
        SimpleNamespace(items=[pod]),
        SimpleNamespace(items=[]),
    ] * 2
    monkeypatch.setattr(recovery.client, "CoreV1Api", lambda: core)
    env = SimpleNamespace(
        endpoints=SimpleNamespace(namespace="mz", operator_namespace="operator")
    )
    observed = recovery.observe_pods(env)  # type: ignore[arg-type]
    assert observed[0].exit_code == 42
    assert core.list_namespaced_pod.call_args.kwargs["_request_timeout"] > 0
    db = sqlite3.connect(":memory:")
    db.execute(
        "CREATE TABLE terminations (pod_uid TEXT, container TEXT, restart_count INTEGER, role TEXT, pod TEXT, exit_code INTEGER, reason TEXT, signal INTEGER, finished_at TEXT, classification TEXT, observed_at REAL, PRIMARY KEY (pod_uid, container, restart_count))"
    )
    verdict = Mock()
    monkeypatch.setattr(recovery, "always_or_unreachable", verdict)
    assert recovery.record_terminations(db, observed) == 1
    assert verdict.call_args.args[0] is False
    status.restart_count = 2
    status.last_state.terminated = term
    status.state.terminated = None
    observed = recovery.observe_pods(env)  # type: ignore[arg-type]
    assert recovery.record_terminations(db, observed) == 0


def test_recovery_comparison_records_and_guards_its_pod_deletes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from materialize.antithesis.drivers import pod_restarts

    envd = SimpleNamespace(
        name="envd",
        namespace="mz",
        uid="envd-uid",
        labels={"materialize.cloud/app": "environmentd"},
        deletion_timestamp=None,
    )
    replica = SimpleNamespace(
        name="replica",
        namespace="mz",
        uid="replica-uid",
        labels={"materialize.cloud/cluster-id": "u1"},
        deletion_timestamp=None,
    )
    core = Mock()
    core.list_namespaced_pod.return_value.items = [
        SimpleNamespace(metadata=m) for m in (envd, replica)
    ]
    monkeypatch.setattr(recovery.client, "CoreV1Api", lambda: core)
    env = SimpleNamespace(
        endpoints=SimpleNamespace(namespace="mz", operator_namespace="operator")
    )
    kube = Mock(core=core)
    monkeypatch.setattr(rollouts, "Kube", lambda _: kube)
    deleted = Mock(return_value=True)
    monkeypatch.setattr(pod_restarts, "delete_recorded", deleted)
    monkeypatch.setattr(recovery.rng, "choice", Mock(side_effect=["both", replica]))
    monkeypatch.setattr(recovery.lifecycle, "Session", Mock())
    monkeypatch.setattr(recovery.lifecycle, "open_state", Mock())
    monkeypatch.setattr(
        recovery.lifecycle,
        "quiet_projection",
        Mock(side_effect=[(1, "before"), (1, "after")]),
    )
    monkeypatch.setattr(recovery.lifecycle, "compare_projections", Mock())
    monkeypatch.setattr(recovery.lifecycle, "check_readable", Mock())
    monkeypatch.setattr(recovery, "poll", Mock(return_value=(True, {}, 0.0)))
    check = recovery.Recovery.__new__(recovery.Recovery)
    check.env = env  # type: ignore[assignment]
    check.mode = "test"
    check.timings = {}
    check.restart_and_compare()
    assert deleted.call_count == 2
    assert [c.kwargs["uid"] for c in deleted.call_args_list] == [
        "envd-uid",
        "replica-uid",
    ]
    assert all(c.kwargs["origin"] == "recovery" for c in deleted.call_args_list)
    core.delete_namespaced_pod.assert_not_called()
    assert core.list_namespaced_pod.call_args.kwargs["_request_timeout"] > 0


def test_rollout_pod_kills_respect_quiet_periods(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from materialize.antithesis.drivers import pod_restarts

    monkeypatch.setattr(pod_restarts, "quiet_period_active", lambda _: True)
    deleted = Mock(return_value=True)
    monkeypatch.setattr(pod_restarts, "delete_recorded", deleted)
    ctx = Mock()
    ctx.kube.lease_holder.return_value = "operator"
    assert not rollouts.execute_kill(
        ctx, rollouts.KillPlan("Applying", "orchestratord", 1), snapshot("Applying")
    )
    deleted.assert_not_called()


@pytest.mark.parametrize(
    "before,after,checked",
    [
        (
            {"pods": [["u1", 0]], "active_generation": 1, "phase": "Applying"},
            {"pods": [["u1", 1]], "active_generation": 1, "phase": "Applying"},
            True,
        ),
        (
            {"active_generation": 1, "phase": "Applying"},
            {"active_generation": 1, "phase": "ReadyToPromote"},
            True,
        ),
        (
            {"active_generation": 1, "phase": "Applying"},
            {"active_generation": 1, "phase": "Promoting"},
            False,
        ),
        (
            {"active_generation": 1, "phase": "Applying"},
            {"active_generation": 2, "phase": "Applied"},
            False,
        ),
    ],
)
def test_only_promotion_exempts_under_load_canary_deadlines(
    monkeypatch: pytest.MonkeyPatch, before: dict, after: dict, checked: bool
) -> None:
    monkeypatch.setattr(canaries, "_wait_for_sql", lambda *args: 0.0)
    monkeypatch.setattr(canaries.time, "sleep", lambda _: None)
    monkeypatch.setattr(canaries, "_ensure_objects", lambda _: None)
    monkeypatch.setattr(
        canaries, "_deployment_signature", Mock(side_effect=[before, after])
    )
    monkeypatch.setattr(canaries.history, "recent_activity", lambda: 1)
    monkeypatch.setattr(canaries.counters, "recent_activity", lambda: 1)
    result = canaries.ProbeResult("P1", "unanswered", True, 181)
    monkeypatch.setattr(canaries, "_run_probes", lambda _: [result])
    assertion = Mock()
    monkeypatch.setattr(canaries, "_assert_under_load", assertion)
    canaries._probe_under_load(Mock(), "host", True)
    assert assertion.called is checked


def test_temporary_probe_stall_keeps_the_latency_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def attempt(
        host: str, name: str, start: float, end: float, result: canaries.ProbeResult
    ) -> None:
        result.status = "ok"
        result.accepted = True

    monkeypatch.setattr(canaries, "_attempt", attempt)
    monkeypatch.setattr(
        canaries.time,
        "monotonic",
        Mock(side_effect=[0.0, canaries.PROBE_DEADLINE_S + 1]),
    )
    result = canaries.run_probe("host", "P1")
    assert result.answered
    assert not result.answered_in_time
    verdict = Mock()
    latency = Mock()
    monkeypatch.setattr(canaries, "always", verdict)
    monkeypatch.setattr(canaries, "sometimes", latency)
    canaries._assert_under_load(result)
    assert verdict.call_args.args[0] is True
    assert latency.call_args.args[0] is False


def test_probe_that_never_answers_fails_the_wedge_property(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = threading.Event()

    def attempt(
        host: str, name: str, start: float, end: float, result: canaries.ProbeResult
    ) -> None:
        result.accepted = True
        release.wait(5)
        result.status = "ok"

    monkeypatch.setattr(canaries, "_attempt", attempt)
    monkeypatch.setattr(canaries, "_wedge_bound", lambda _: 0.01)
    try:
        result = canaries.run_probe("host", "P1")
        assert result.status == "unanswered"
        assert result.accepted
        verdict = Mock()
        monkeypatch.setattr(canaries, "always", verdict)
        canaries._assert_under_load(result)
        assert verdict.call_args.args[0] is False
    finally:
        release.set()


def test_interrupted_paced_ddl_releases_renewable_quiet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = []

    @contextmanager
    def held(requester: str, chunk_s: int, exclusive: bool):
        assert chunk_s <= 60
        assert exclusive
        events.append("held")
        try:
            yield True
        finally:
            events.append("released")

    monkeypatch.setattr(quiet, "held", held)
    monkeypatch.setattr(
        rollouts,
        "ddl_check_interval_seconds",
        Mock(side_effect=RuntimeError("interrupted")),
    )
    with pytest.raises(RuntimeError, match="interrupted"):
        rollouts.action_paced_ddl(Mock())
    assert events == ["held", "released"]

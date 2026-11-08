# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import threading
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

from materialize.antithesis.crash_loops import Sample, Tracker
from materialize.antithesis.drivers import pod_restarts, recovery

EXIT_CODES = {"halt": 166, "panic": 134, "killed": 137}


def sample(
    restarts: int,
    cls: str | None = None,
    uid: str = "u1",
    deleting: bool = False,
    pod: str | None = None,
) -> Sample:
    return Sample(
        key=(uid, "c"),
        name=f"clusterd/{pod or f'pod-{uid}'}/c",
        restart_count=restarts,
        exit_class=cls,
        exit_code=EXIT_CODES.get(cls) if cls else None,
        finished_at=None,
        waiting_reason=None,
        deleting=deleting,
    )


def run(steps: list[tuple[float, list[Sample]]]) -> Tracker:
    t = Tracker()
    for now, samples in steps:
        t.observe(now, samples)
    return t


def test_restarts_before_first_sample_are_not_counted() -> None:
    t = run([(0, [sample(9, "halt")]), (600, [sample(9, "halt")])])
    assert t.loops(600) == {}


def test_two_halts_in_window_are_a_loop() -> None:
    t = run([(0, [sample(0)]), (300, [sample(1, "halt")]), (600, [sample(2, "halt")])])
    loops = t.loops(600)
    assert list(loops) == ["clusterd/pod-u1/c"]
    assert loops["clusterd/pod-u1/c"]["crashes_in_window"] == 2
    assert [r["observed_at"] for r in loops["clusterd/pod-u1/c"]["history"]] == [
        -300,
        0,
    ]


def test_one_designed_halt_is_not_a_loop() -> None:
    t = run([(0, [sample(0)]), (100, [sample(1, "halt")]), (700, [sample(1, "halt")])])
    assert t.loops(700) == {}


def test_crashes_outside_the_window_do_not_count() -> None:
    t = run(
        [
            (0, [sample(0)]),
            (10, [sample(1, "panic")]),
            (20, [sample(2, "panic")]),
            (900, [sample(2, "panic")]),
        ]
    )
    assert t.loops(600) == {}
    assert list(t.loops(900)) == ["clusterd/pod-u1/c"]


def test_a_slow_loop_is_caught_between_its_crashes() -> None:
    # Crashes every 500 s: the 720 s window ending at 1400 holds one crash, but
    # the window ending at the crash at 1100 held two.
    t = run(
        [
            (0, [sample(0)]),
            (100, [sample(1, "halt")]),
            (600, [sample(2, "halt")]),
            (1100, [sample(3, "halt")]),
            (1400, [sample(3, "halt")]),
        ]
    )
    assert t.loops(720) == {}
    loops = t.loops(720, span_s=720)
    assert loops["clusterd/pod-u1/c"]["crashes_in_window"] == 2


def test_any_live_crash_in_the_window_is_reported_until_it_ages_out() -> None:
    samples: list[tuple[float, list[Sample]]] = [
        (0, [sample(0), sample(0, uid="u2")]),
        (100, [sample(1, "halt"), sample(1, "halt", deleting=True, uid="u2")]),
        (800, [sample(1, "halt"), sample(1, "halt", deleting=True, uid="u2")]),
    ]
    assert run(samples).crashed(720) == ["clusterd/pod-u1/c"]
    assert run(samples + [(900, [sample(1, "halt")])]).crashed(720) == []


def test_a_single_crash_in_the_span_is_not_a_loop() -> None:
    t = run(
        [
            (0, [sample(0)]),
            (100, [sample(1, "halt")]),
            (1000, [sample(2, "halt")]),
            (1500, [sample(2, "halt")]),
        ]
    )
    assert t.loops(720, span_s=720) == {}


def test_external_kills_are_not_crashes() -> None:
    t = run(
        [(0, [sample(0)]), (100, [sample(1, "killed")]), (200, [sample(2, "killed")])]
    )
    assert t.loops(600) == {}


def test_unobserved_restarts_take_the_observed_class() -> None:
    t = run([(0, [sample(0)]), (100, [sample(2, "halt")])])
    assert t.loops(600)["clusterd/pod-u1/c"]["crashes_in_window"] == 2


def test_deleted_or_deleting_pods_are_not_loops() -> None:
    gone = run(
        [
            (0, [sample(0), sample(0, uid="u2")]),
            (100, [sample(2, "halt"), sample(0, uid="u2")]),
            (200, [sample(0, uid="u2")]),
        ]
    )
    assert gone.loops(600) == {}
    deleting = run([(0, [sample(0)]), (100, [sample(2, "halt", deleting=True)])])
    assert deleting.loops(600) == {}


def test_coverage() -> None:
    t = Tracker()
    assert not t.covered(10)
    t.observe(5, [])
    t.observe(14, [])
    assert not t.covered(10)
    t.observe(15, [])
    assert t.covered(10)


def test_a_reset_pod_hands_its_history_to_its_replacement() -> None:
    t = run([(0, [sample(0, pod="p")]), (100, [sample(1, "halt", pod="p")])])
    t.reset(("u1", "c"))
    t.observe(105, [sample(1, "halt", deleting=True, pod="p")])
    assert t.crashed(120) == ["clusterd/p/c"]
    # The replacement's first restart came after the reset.
    t.observe(110, [sample(1, "halt", uid="u2", pod="p")])
    loops = t.loops(120)
    assert loops["clusterd/p/c"]["crashes_in_window"] == 2


def test_persisting_needs_three_crashes_each_within_the_window() -> None:
    steps: list[tuple[float, list[Sample]]] = [(0, [sample(0)])]
    steps += [(at, [sample(n, "halt")]) for n, at in enumerate((10, 50, 90), 1)]
    assert list(run(steps).persisting(120, since=0)) == ["clusterd/pod-u1/c"]
    assert run(steps[:3]).persisting(120, since=0) == {}
    assert run(steps).persisting(120, since=20) == {}
    assert run(steps).persisting(30, since=0) == {}


def test_persisting_ignores_crashes_in_deleted_pods() -> None:
    steps: list[tuple[float, list[Sample]]] = [(0, [sample(0)])]
    steps += [(at, [sample(n, "halt")]) for n, at in enumerate((10, 50, 90), 1)]
    steps.append((95, [sample(3, "halt", deleting=True)]))
    assert run(steps).persisting(120, since=0) == {}


def observation(
    *,
    role: str = recovery.ROLE_CLUSTERD,
    pod: str = "p",
    uid: str = "u1",
    restarts: int = 0,
    exit_code: int | None = None,
    waiting: str | None = None,
    deleting: bool = False,
) -> recovery.ContainerObservation:
    return recovery.ContainerObservation(
        role=role,
        namespace="ns",
        pod=pod,
        pod_uid=uid,
        container="c",
        restart_count=restarts,
        exit_code=exit_code,
        reason=None,
        signal=None,
        finished_at=None,
        waiting_reason=waiting,
        deleting=deleting,
    )


def bare_recovery() -> recovery.Recovery:
    check = recovery.Recovery.__new__(recovery.Recovery)
    check.env = Mock()
    check.restarts = Tracker()
    check.restarts_lock = threading.Lock()
    check.containers = {}
    check.latest = []
    check.backoff_resets = recovery.Counter()
    check.sampling_done = threading.Event()
    return check


def test_sampler_counts_a_crash_while_its_restart_backs_off(monkeypatch) -> None:
    check = bare_recovery()
    monkeypatch.setattr(recovery, "CRASHLOOP_SAMPLE_S", 0)

    def sample_rounds(*rounds: list[recovery.ContainerObservation]) -> None:
        pending = iter(rounds)

        def observe(env):
            batch = next(pending, None)
            if batch is None:
                check.sampling_done.set()
                return []
            return batch

        monkeypatch.setattr(recovery, "observe_pods", observe)
        check.sampling_done.clear()
        check.sample_restarts()

    sample_rounds(
        [observation()], [observation(exit_code=166, waiting="CrashLoopBackOff")]
    )
    (history,) = check.restarts.containers.values()
    assert [r["class"] for r in history.restarts] == ["halt"]
    # The kubelet restarted it, and restart_count now counts the same crash.
    sample_rounds([observation(restarts=1, exit_code=166)])
    assert [r["class"] for r in history.restarts] == ["halt"]


def test_reset_backoff_deletes_statefulset_pods_in_backoff_up_to_the_limit(
    monkeypatch,
) -> None:
    check = bare_recovery()

    def halted(**kwargs: Any) -> recovery.ContainerObservation:
        return observation(exit_code=166, waiting="CrashLoopBackOff", **kwargs)

    check.latest = [
        halted(),
        halted(role=recovery.ROLE_ENVIRONMENTD, pod="e", uid="u2"),
        halted(role=recovery.ROLE_ORCHESTRATORD, pod="o", uid="u3"),
        halted(pod="d", uid="u4", deleting=True),
        observation(pod="r", uid="u5", restarts=1, exit_code=166),
        observation(pod="k", uid="u7", exit_code=137, waiting="CrashLoopBackOff"),
    ]
    check.restarts.observe(0, [])
    deleted = []

    def delete(kube, *, pod, uid, reason, **kwargs):
        deleted.append((pod, uid, reason))
        return True

    monkeypatch.setattr(pod_restarts, "delete_recorded", delete)
    monkeypatch.setattr(recovery.rollouts, "Kube", Mock())
    monkeypatch.setattr(recovery, "reachable", Mock())
    check.reset_backoff()
    assert deleted == [
        ("p", "u1", "crash_loop_backoff_reset"),
        ("e", "u2", "crash_loop_backoff_reset"),
    ]
    assert [o.pod for o in check.latest] == ["o", "d", "r", "k"]

    deleted.clear()
    check.backoff_resets["clusterd/p/c"] = recovery.CRASHLOOP_RESET_LIMIT
    check.latest = [halted(uid="u6")]
    check.reset_backoff()
    assert deleted == []


def test_crash_loop_verdict_fails_as_soon_as_a_loop_persists(monkeypatch) -> None:
    check = bare_recovery()
    clock = SimpleNamespace(now=1000.0)
    monkeypatch.setattr(
        recovery,
        "time",
        SimpleNamespace(monotonic=lambda: clock.now, sleep=Mock()),
    )
    monkeypatch.setattr(check, "exit_log_lines", lambda name: ["halting process: x"])
    check.restarts.observe(900, [sample(0)])
    for n, at in enumerate((1010, 1050, 1090), 1):
        check.restarts.observe(at, [sample(n, "halt")])
    clock.now = 1000.0
    ok, info, _ = check.crash_loop_verdict(budget_s=1000)
    assert not ok
    assert info["persisting"]["clusterd/pod-u1/c"]["exit_log"] == ["halting process: x"]
    recovery.time.sleep.assert_not_called()


def test_crash_loop_verdict_passes_after_a_crash_free_window(monkeypatch) -> None:
    check = bare_recovery()
    clock = SimpleNamespace(now=1000.0)

    def sleep(seconds):
        clock.now += seconds
        check.restarts.observe(clock.now, [sample(2, "halt")])

    monkeypatch.setattr(
        recovery, "time", SimpleNamespace(monotonic=lambda: clock.now, sleep=sleep)
    )
    # Two crashes before the verdict loop, then quiet: the container settled.
    check.restarts.observe(800, [sample(0)])
    check.restarts.observe(950, [sample(1, "halt")])
    check.restarts.observe(990, [sample(2, "halt")])
    ok, info, took = check.crash_loop_verdict(budget_s=1000)
    assert ok, info
    assert clock.now - 990 >= recovery.CRASHLOOP_WINDOW_S
    assert took < 1000

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

from materialize.antithesis.crash_loops import Sample, Tracker

EXIT_CODES = {"halt": 166, "panic": 134, "killed": 137}


def sample(
    restarts: int,
    cls: str | None = None,
    uid: str = "u1",
    deleting: bool = False,
) -> Sample:
    return Sample(
        key=(uid, "c"),
        name=f"clusterd/pod-{uid}/c",
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

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import sql
from materialize.antithesis_driver import cancel
from materialize.antithesis_driver.client import Op, Outcome, Status
from materialize.antithesis_driver.draw import SeededDraw

NO_EFFECT = "cancel: a statement answered as cancelled or timed out took no effect"
COUNT = "cancel: a session sees no more cancelled statements than cancel requests sent to it"
CANCELLED = sql.Classified(
    sql.Outcome.INDETERMINATE, "57014", "canceling statement due to user request"
)
TIMED_OUT = sql.Classified(
    sql.Outcome.INDETERMINATE, "57014", "canceling statement due to statement timeout"
)


def rows(op: int, gen: int = 0, count: int = cancel.ROWS) -> list[tuple[int, int, int]]:
    return [(op, i, gen) for i in range(count)]


def acked(at: float = 1.0, **fields: Any) -> cancel.Batch:
    return cancel.Batch(insert="ok", acked_at=at, **fields)


def test_only_cancel_and_timeout_answers_count_as_cancelled() -> None:
    assert cancel.cancelled(CANCELLED) and cancel.requested(CANCELLED)
    assert cancel.cancelled(TIMED_OUT) and not cancel.requested(TIMED_OUT)
    lost = sql.Classified(sql.Outcome.INDETERMINATE, None, "connection lost")
    assert not cancel.cancelled(lost) and not cancel.cancelled(None)


def test_whole_batches_at_an_allowed_generation_are_clean() -> None:
    batches = {
        1: acked(),
        2: acked(updates_acked=[2.0], updates_open=1),
        3: cancel.Batch(insert="indeterminate"),
        4: cancel.Batch(insert="pending"),
    }
    errors = cancel.judge(rows(1) + rows(2, 2) + rows(4), batches, start=5.0)
    assert not any(errors.values())
    assert batches[2].max_gen_seen == 2 and batches[4].seen and not batches[3].seen


def test_a_partial_batch_mixed_generations_and_an_unsent_batch_are_torn() -> None:
    batches = {1: acked(), 2: acked(updates_open=1)}
    mixed = rows(2)[:4] + [(2, i, 1) for i in range(4, cancel.ROWS)]
    errors = cancel.judge(rows(1, count=5) + mixed + rows(9), batches, start=5.0)
    assert [e["op"] for e in errors["torn"]] == [1, 2, 9]


def test_a_generation_outside_the_updates_is_reported() -> None:
    batches = {1: acked(updates_acked=[2.0, 3.0]), 2: acked(updates_acked=[9.0])}
    # Batch 1 misses an update acknowledged before the read, batch 2 shows one
    # more than was ever sent.
    errors = cancel.judge(rows(1, 1) + rows(2, 2), batches, start=5.0)
    assert [(e["op"], e["lo"], e["hi"]) for e in errors["generation"]] == [
        (1, 2, 2),
        (2, 0, 1),
    ]


def test_an_acknowledged_batch_must_be_visible_until_a_delete_is_sent() -> None:
    batches = {
        1: acked(),
        2: acked(delete="pending"),
        3: acked(at=9.0),
        4: acked(delete="ok", deleted_at=2.0),
    }
    errors = cancel.judge(rows(4), batches, start=5.0)
    assert [e["op"] for e in errors["presence"]] == [4, 1]
    assert batches[2].missing_seen and not batches[3].missing_seen


def test_rows_of_a_cancelled_insert_and_a_cancelled_delete_that_applied() -> None:
    batches = {
        1: cancel.Batch(insert="rejected", insert_cancelled=True),
        2: cancel.Batch(insert="rejected"),
        3: acked(delete_cancelled=True),
    }
    errors = cancel.judge(rows(1) + rows(2), batches, start=5.0)
    assert [e["op"] for e in errors["cancelled"]] == [1, 3]
    assert [e["op"] for e in errors["rejected"]] == [2]


class Pool:
    """Runs nothing: a test completes the operation it captured."""

    def __init__(self) -> None:
        self.sessions = [
            SimpleNamespace(name="s0", statement_timeout_ms=30_000, cancels=0)
        ]
        self.ops: list[Op] = []

    def call(self, index: int, op: Op, wait: float) -> Status:
        self.ops.append(op)
        return Status.PENDING


class Recorder:
    def __init__(self) -> None:
        self.failed: list[str] = []

    def __call__(self, condition: bool, message: str, details: Any = None) -> None:
        if not condition:
            self.failed.append(message)


@pytest.fixture
def component(monkeypatch: pytest.MonkeyPatch) -> tuple[cancel.Cancel, Pool, Recorder]:
    recorder = Recorder()
    monkeypatch.setattr(cancel, "always", recorder)
    monkeypatch.setattr(cancel, "sometimes", lambda *a, **k: None)
    monkeypatch.setattr(cancel, "reachable", lambda *a, **k: None)
    pool = Pool()
    ctx = SimpleNamespace(sessions=pool, config=SimpleNamespace(host="localhost"))
    return cancel.Cancel(ctx), pool, recorder  # pyright: ignore[reportArgumentType]


def answered(
    error: sql.Classified | None, value: Any = None, observed: int = 0
) -> Outcome:
    result = cancel.Result(value, error, observed, 0.1)
    return Outcome(result, None, 10.0, 11.0, "s0.1")


def test_an_insert_seen_by_a_read_and_then_answered_cancelled_is_a_violation(
    component: tuple[cancel.Cancel, Pool, Recorder],
) -> None:
    c, pool, recorder = component
    c.write(SeededDraw(1))
    (op,) = c.batches
    c.read(SeededDraw(2))
    write, read = pool.ops
    read.complete(answered(None, rows(op)))
    assert not recorder.failed
    write.complete(answered(CANCELLED, observed=1))
    assert recorder.failed == [COUNT, NO_EFFECT]


def test_a_cancelled_insert_that_shows_up_later_is_a_violation(
    component: tuple[cancel.Cancel, Pool, Recorder],
) -> None:
    c, pool, recorder = component
    c.write(SeededDraw(1))
    (op,) = c.batches
    pool.ops[0].complete(answered(TIMED_OUT))
    assert not recorder.failed
    c.read(SeededDraw(2))
    pool.ops[1].complete(answered(None, rows(op)))
    assert recorder.failed == [NO_EFFECT]


def test_a_cancelled_update_that_a_read_saw_applied_is_a_violation(
    component: tuple[cancel.Cancel, Pool, Recorder],
) -> None:
    c, pool, recorder = component
    c.batches[1] = acked()
    d = SeededDraw(4)
    c.change(d)
    assert c.batches[1].updates_open == 1 and c.batches[1].delete == "none"
    c.read(SeededDraw(2))
    change, read = pool.ops
    read.complete(answered(None, rows(1, 1)))
    assert not recorder.failed
    change.complete(answered(CANCELLED))
    assert recorder.failed == [NO_EFFECT]


def test_an_update_that_found_no_row_does_not_raise_the_expected_generation(
    component: tuple[cancel.Cancel, Pool, Recorder],
) -> None:
    c, pool, recorder = component
    c.batches[1] = acked()
    c.change(SeededDraw(4))
    assert c.batches[1].updates_open == 1
    # A delete sent after the update applied first, so the update found no row.
    c.batches[1].delete = "indeterminate"
    pool.ops[0].complete(answered(None, 0))
    assert c.batches[1].updates_acked == [] and c.batches[1].updates_open == 0
    assert not recorder.failed


def test_more_cancelled_statements_than_requests_is_a_violation(
    component: tuple[cancel.Cancel, Pool, Recorder],
) -> None:
    c, pool, recorder = component
    c.read(SeededDraw(2))
    # The statement and both probes were cancelled by at most one request.
    pool.ops[0].complete(answered(CANCELLED, observed=3))
    assert recorder.failed == [COUNT]


def test_a_cancelled_create_that_exists_and_a_cancelled_drop_that_applied(
    component: tuple[cancel.Cancel, Pool, Recorder],
) -> None:
    c, pool, recorder = component
    c.ddl(SeededDraw(1))
    result = cancel.Result(None, CANCELLED, 0, 0.1, follow_up="missing")
    pool.ops[0].complete(Outcome(result, None, 10.0, 11.0, "s0.1"))
    assert not recorder.failed and not c.objects

    c.ddl(SeededDraw(1))
    result = cancel.Result(None, CANCELLED, 0, 0.1, follow_up="dropped")
    pool.ops[1].complete(Outcome(result, None, 10.0, 11.0, "s0.1"))
    assert recorder.failed == [NO_EFFECT]

    recorder.failed.clear()
    c.objects[7] = cancel.Obj("INDEX", "driver", "live")
    for n in (8, 9):
        c.objects[n] = cancel.Obj("INDEX", "driver", "live")
    c.ddl(SeededDraw(1))
    result = cancel.Result(None, CANCELLED, 0, 0.1, follow_up="missing")
    pool.ops[2].complete(Outcome(result, None, 10.0, 11.0, "s0.1"))
    assert recorder.failed == [NO_EFFECT] and 7 not in c.objects

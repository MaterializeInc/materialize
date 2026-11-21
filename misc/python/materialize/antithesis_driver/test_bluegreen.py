# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, TypeVar

import psycopg
import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import sql
from materialize.antithesis_driver import bluegreen
from materialize.antithesis_driver.bluegreen import BUCKETS, derived, value
from materialize.antithesis_driver.client import Op, Outcome, Status

T = TypeVar("T")

ROWS = "blue/green: every row of a read has one generation and is correct for it and its seed"
RECENT = "blue/green: a read shows every seed acknowledged before it began and none deleted before it began"
FLOOR = (
    "blue/green: a read never returns a generation older than one live before it began"
)
LIVE = "blue/green: a read through the live names never fails because a live name is missing"
CATALOG = (
    "blue/green: the live names hold one whole deployment of an expected generation"
)
UNKNOWN = sql.Classified(sql.Outcome.INDETERMINATE, None, "connection lost")


class Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[bool, str]] = []

    def __call__(self, condition: bool, message: str, details: Any = None) -> None:
        self.calls.append((bool(condition), message))

    def failed(self) -> list[str]:
        return sorted({m for c, m in self.calls if not c})

    def held(self) -> set[str]:
        return {m for c, m in self.calls if c}


class Clock:
    """Stands in for `time`: each completed operation advances it."""

    def __init__(self) -> None:
        self.now = 100.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class Conn:
    """Records statements, finds the staging cluster hydrated and commits."""

    statusmessage = "COMMIT"

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: bytes) -> Conn:
        self.statements.append(statement.decode())
        return self

    def fetchone(self) -> tuple[int]:
        return (0,)

    def fetchall(self) -> list[tuple[Any, ...]]:
        return []


class Sessions:
    """Stands in for the session pool: runs an operation's statements against
    `Conn` and leaves it pending until the test completes it."""

    def __init__(self) -> None:
        self.sessions = [None] * 4
        self.pending: list[tuple[Op, list[str]]] = []

    def call(self, index: int, op: Op, wait: float) -> Status:
        conn = Conn()
        op.execute(conn)  # pyright: ignore[reportArgumentType]
        self.pending.append((op, conn.statements))
        return Status.PENDING


class Config:
    host = "localhost"
    replica_sizes = ("1",)


class Ctx:
    def __init__(self) -> None:
        self.sessions = Sessions()
        self.config = Config()


class Draw:
    """The lowest integer, the first option, never true: a write inserts one
    seed, a read reads `agg`, a swap does not wait."""

    def integer(self, lo: int, hi: int) -> int:
        return lo

    def boolean(self, p: float = 0.5) -> bool:
        return False

    def choice(self, options: Sequence[T]) -> T:
        return options[0]


D = Draw()


@pytest.fixture
def env(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[bluegreen.BlueGreen, Sessions, Recorder, Clock]:
    recorder, clock = Recorder(), Clock()
    monkeypatch.setattr(bluegreen, "always", recorder)
    monkeypatch.setattr(bluegreen, "always_or_unreachable", recorder)
    monkeypatch.setattr(bluegreen, "sometimes", lambda *_: None)
    monkeypatch.setattr(bluegreen, "time", clock)
    ctx = Ctx()
    component = bluegreen.BlueGreen(ctx)  # pyright: ignore[reportArgumentType]
    return component, ctx.sessions, recorder, clock


def run(
    sessions: Sessions,
    clock: Clock,
    value: Any = None,
    error: sql.Classified | None = None,
) -> list[str]:
    """Complete the latest pending operation and return its statements."""
    op, statements = sessions.pending.pop()
    start = clock.now
    clock.now += 1
    op.complete(Outcome(value, error, start, clock.now, "s0"))
    clock.now += 1
    return statements


def rows(gen: int, seeds: Sequence[int], source: str = "agg") -> list[tuple[Any, ...]]:
    return [
        (source, gen, s, value(s), derived(source, gen, s, value(s))) for s in seeds
    ]


def grp(gen: int, seeds: Sequence[int]) -> list[tuple[Any, ...]]:
    buckets: dict[int, list[int]] = {}
    for s in seeds:
        b = buckets.setdefault(s % BUCKETS, [0, 0])
        b[0] += 1
        b[1] += value(s)
    return [("grp", gen, b, n, s) for b, (n, s) in buckets.items()]


def insert(bg: bluegreen.BlueGreen, sessions: Sessions, clock: Clock, n: int) -> None:
    for _ in range(n):
        bg.write(D)
        run(sessions, clock)


def read(
    bg: bluegreen.BlueGreen,
    sessions: Sessions,
    clock: Clock,
    kind: str,
    result: list[tuple[Any, ...]],
) -> None:
    sessions.call(0, bg._read_op(kind, []), 1.0)
    run(sessions, clock, result)


def deploy(bg: bluegreen.BlueGreen, sessions: Sessions, clock: Clock) -> None:
    """Stage, swap and clean up, each acknowledged."""
    for rule in (bg.stage, bg.swap, bg.cleanup):
        assert rule(D) == Status.PENDING
        run(sessions, clock)


def test_a_correct_read_passes(env: Any) -> None:
    bg, sessions, recorder, clock = env
    insert(bg, sessions, clock, 3)
    read(bg, sessions, clock, "grp", rows(1, [1, 2, 3]) + grp(1, [1, 2, 3]))
    read(bg, sessions, clock, "idx_view", rows(1, [3, 1, 2], "idx_view"))
    assert recorder.failed() == []
    assert {ROWS, RECENT, FLOOR, LIVE} <= recorder.held()


def test_a_mixed_generation_read_fails(env: Any) -> None:
    bg, sessions, recorder, clock = env
    insert(bg, sessions, clock, 2)
    deploy(bg, sessions, clock)
    read(bg, sessions, clock, "agg", rows(2, [1]) + rows(3, [2]))
    assert ROWS in recorder.failed()


def test_a_grp_that_disagrees_with_agg_fails(env: Any) -> None:
    bg, sessions, recorder, clock = env
    insert(bg, sessions, clock, 2)
    read(bg, sessions, clock, "grp", rows(1, [1, 2]) + grp(1, [1]))
    assert recorder.failed() == [ROWS]


def test_a_stale_generation_after_an_acknowledged_swap_fails(env: Any) -> None:
    bg, sessions, recorder, clock = env
    insert(bg, sessions, clock, 2)
    deploy(bg, sessions, clock)
    assert bg.live == 2
    read(bg, sessions, clock, "agg", rows(2, [1, 2]))
    assert recorder.failed() == []
    read(bg, sessions, clock, "agg", rows(1, [1, 2]))
    assert recorder.failed() == [FLOOR]


def test_a_missing_acknowledged_seed_fails(env: Any) -> None:
    bg, sessions, recorder, clock = env
    insert(bg, sessions, clock, 3)
    read(bg, sessions, clock, "agg", rows(1, [1, 3]))
    assert recorder.failed() == [RECENT]


def test_a_seed_may_be_missing_once_its_delete_was_sent(env: Any) -> None:
    bg, sessions, recorder, clock = env
    insert(bg, sessions, clock, bluegreen.MAX_ROWS + 1)
    bg.write(D)
    assert sessions.pending[0][1] == [
        f"DELETE FROM {bluegreen.SCHEMA}.t WHERE seed IN (1)"
    ]
    seeds = range(2, bluegreen.MAX_ROWS + 2)
    read(bg, sessions, clock, "agg", rows(1, seeds))
    assert recorder.failed() == []
    run(sessions, clock, error=UNKNOWN)
    read(bg, sessions, clock, "agg", rows(1, [1, *seeds]))
    assert recorder.failed() == []


def test_an_unknown_stage_is_resolved_before_a_swap(env: Any) -> None:
    bg, sessions, _, clock = env
    assert bg.stage(D) == Status.PENDING
    run(sessions, clock, error=UNKNOWN)
    assert bg.swap(D) == Status.NOT_SENT
    assert bg.stage(D) == Status.PENDING
    statements = run(sessions, clock)
    assert statements[:2] == bluegreen.DROP_STAGE
    assert bg.swap(D) == Status.PENDING
    assert f"ALTER SCHEMA {bluegreen.LIVE} SWAP WITH {bluegreen.STAGE}" in run(
        sessions, clock
    )
    assert (bg.live, bg.staging) == (2, "old")


def test_an_unknown_swap_is_resolved_from_the_catalog(env: Any) -> None:
    bg, sessions, recorder, clock = env
    insert(bg, sessions, clock, 1)
    bg.stage(D)
    run(sessions, clock)
    bg.swap(D)
    run(sessions, clock, error=UNKNOWN)
    assert bg.unsure
    assert bg.cleanup(D) == Status.PENDING
    catalog = [
        ("schema", "bg_live", None, None),
        ("schema", "bg_stage", None, None),
        ("cluster", "bg_live", None, None),
        ("cluster", "bg_stage", None, None),
        ("agg", "bg_live", "bg_live", "generation 2"),
        ("agg", "bg_stage", "bg_stage", "generation 1"),
    ]
    assert run(sessions, clock, catalog) == [bluegreen.CATALOG_QUERY]
    assert (bg.live, bg.staging, bg.unsure) == (2, "old", False)
    assert CATALOG in recorder.held()
    # The lost COMMIT can still apply and the next swap then puts the older
    # generation back, so the order of generations is no longer judged.
    assert not bg.order_known
    read(bg, sessions, clock, "agg", rows(1, [1]))
    assert recorder.failed() == []


def test_a_swap_of_only_the_schema_fails(env: Any) -> None:
    bg, sessions, recorder, clock = env
    bg.unsure = True
    bg.stage(D)
    run(sessions, clock, [("agg", "bg_live", "bg_stage", "generation 2")])
    assert recorder.failed() == [CATALOG]


@pytest.mark.parametrize(
    "message,missing",
    [
        ("unknown schema 'bg_live'", True),
        ("unknown cluster 'bg_live'", True),
        ("unknown catalog item 'bg_live.agg'", True),
        ('query could not complete because cluster "bg_stage" was dropped', False),
        (
            'query could not complete because relation "materialize.bg_stage.agg" was dropped',
            False,
        ),
    ],
)
def test_a_read_fails_only_with_names_that_are_no_longer_live(
    env: Any, message: str, missing: bool
) -> None:
    bg, sessions, recorder, clock = env
    sessions.call(0, bg._read_op("agg", []), 1.0)
    op = sessions.pending[0][0]
    error = op.classify(psycopg.errors.InternalError_(message))
    assert error.outcome == sql.Outcome.REJECTED
    run(sessions, clock, error=error)
    assert recorder.failed() == ([LIVE] if missing else [])

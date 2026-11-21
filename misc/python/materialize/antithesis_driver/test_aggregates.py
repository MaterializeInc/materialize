# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import re
from typing import Any

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import aggregate_retractions_model as model
from materialize.antithesis_driver import aggregates
from materialize.antithesis_driver.aggregates import Read
from materialize.antithesis_driver.client import Op, Status
from materialize.antithesis_driver.draw import SeededDraw

EQUAL = "aggregate retractions: representable grouped interval sums equal the independent component model"
MUST_OVERFLOW = "aggregate retractions: a grouped interval sum outside its component range reports overflow"
ONLY_OVERFLOW = "aggregate retractions: a view reports overflow only while a grouped interval sum is outside its component range"
REPAIRED = "aggregate retractions: a modeled overflow was retracted and a nonempty maintained result verified"

BIG = (1 << 30) + 7
POISON = f"{3 << 29} mons"


class Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[bool, str, Any]] = []

    def __call__(self, condition: bool, message: str, details: Any = None) -> None:
        self.calls.append((bool(condition), message, details))

    def failed(self, messages: tuple[str, ...]) -> list[str]:
        return sorted({m for c, m, _ in self.calls if not c and m in messages})

    def held(self) -> set[str]:
        return {m for c, m, _ in self.calls if c}


ALWAYS = (EQUAL, MUST_OVERFLOW, ONLY_OVERFLOW)


class Statements:
    """Stands in for the session pool and its connections."""

    def __init__(self) -> None:
        self.sessions = [None] * 4
        self.sent: list[tuple[str, str]] = []

    def execute(self, statement: bytes) -> None:
        self.sent.append((self.name, statement.decode()))

    def call(self, index: int, op: Op, wait: float) -> Status:
        self.name = op.name
        op.execute(self)  # pyright: ignore[reportArgumentType]
        return Status.DONE


class Ctx:
    def __init__(self) -> None:
        self.sessions = Statements()


def component(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[aggregates.Aggregates, Recorder]:
    recorder = Recorder()
    monkeypatch.setattr(aggregates, "always", recorder)
    monkeypatch.setattr(aggregates, "sometimes", recorder)
    monkeypatch.setattr(aggregates, "ABANDON_SHARE", 0.0)
    return aggregates.Aggregates(Ctx()), recorder  # pyright: ignore[reportArgumentType]


# The text forms are those Materialize v26.51.0 prints.
@pytest.mark.parametrize(
    "text,expected",
    [
        ("00:00:00", (0, 0, 0)),
        ("1 mon", (1, 0, 0)),
        ("1 year", (12, 0, 0)),
        ("-1 years -2 mons", (-14, 0, 0)),
        ("2 years 1 mon -3 days +00:00:00.000017", (25, -3, 17)),
        ("1 day", (0, 1, 0)),
        ("-1 days", (0, -1, 0)),
        ("00:00:00.000001", (0, 0, 1)),
        ("-00:00:00.000001", (0, 0, -1)),
        ("01:00:00", (0, 0, 3_600_000_000)),
        ("-01:01:01.000001", (0, 0, -3_661_000_001)),
        (
            "178956970 years 7 mons 2147483647 days 2562047788:00:54.775807",
            ((1 << 31) - 1, (1 << 31) - 1, (1 << 63) - 1),
        ),
        (
            "-178956970 years -8 mons -2147483648 days -2562047788:00:54.775808",
            (-(1 << 31), -(1 << 31), -(1 << 63)),
        ),
        ("-1 mons +1 day -00:00:00.000001", (-1, 1, -1)),
        ("1 mon -1 days +00:00:00.000001", (1, -1, 1)),
        ("00:00:59.999999", (0, 0, 59_999_999)),
        ("72:00:00.000005", (0, 0, 259_200_000_005)),
        ("00:00:00.5", (0, 0, 500_000)),
    ],
)
def test_interval_text_is_parsed_into_its_components(
    text: str, expected: model.Interval
) -> None:
    assert aggregates.parse_interval(text) == expected


@pytest.mark.parametrize("text", ["", "1 fortnight", "1", "1:2:3", "7 days later"])
def test_unknown_interval_text_is_rejected(text: str) -> None:
    with pytest.raises(ValueError):
        aggregates.parse_interval(text)


def test_generated_values_cancel_or_overflow_as_named() -> None:
    d = SeededDraw(3)
    for _ in range(200):
        pair = aggregates.values("pair", d)
        assert not model.evaluate(pair).overflows
        assert model.evaluate(pair).rows[0][1:4] == (0, 0, 0)
        poison = aggregates.values("poison", d)
        assert len(model.evaluate(poison).overflows) == 1
        # One half alone is representable.
        assert not model.evaluate(poison[:1]).overflows
        literal = aggregates.literal(poison[0][1])
        assert re.fullmatch(
            r"'-?\d+ months -?\d+ days -?\d+ microseconds'::interval", literal
        )
    assert aggregates.literal(None) == "NULL"


def test_sums_that_equal_the_model_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    a, recorder = component(monkeypatch)
    base = [
        (1, 0, f"{BIG // 12} years {BIG % 12} mons"),
        (2, 0, f"-{BIG // 12} years -{BIG % 12} mons"),
        (3, 0, "2 mons -3 days +00:00:00.000017"),
        (4, 0, None),
        (5, 1, None),
    ]
    view = [(0, "2 mons -3 days +00:00:00.000017", 3, 4), (1, None, 0, 1)]
    assert a.judge("mv", Read(base, view))
    assert recorder.failed(ALWAYS) == []
    assert a.ids == [1, 2, 3, 4, 5] and a.heal == []
    assert {
        "aggregate retractions: a check compared sums over large values that cancel",
        "aggregate retractions: a check compared a group that holds only NULLs",
    } <= recorder.held()


def test_a_wrong_sum_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    a, recorder = component(monkeypatch)
    base = [(1, 0, "1 mon"), (2, 0, "1 day")]
    assert a.judge("index", Read(base, [(0, "1 mon 2 days", 2, 2)]))
    assert recorder.failed(ALWAYS) == [EQUAL]


def test_an_overflow_must_be_reported_and_only_then(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    a, recorder = component(monkeypatch)
    poisoned = [(1, 2, "1 mon"), (7, 2, POISON), (8, 2, POISON), (9, 1, "1 day")]
    assert not a.judge("mv", Read(poisoned, None))
    assert recorder.failed(ALWAYS) == []
    # The large values of the group out of range, and nothing else.
    assert a.heal == [7, 8]

    assert not a.judge("index", Read(poisoned, [(1, "1 day", 1, 1)]))
    assert recorder.failed(ALWAYS) == [MUST_OVERFLOW]

    b, recorder = component(monkeypatch)
    assert not b.judge("mv", Read([(1, 2, "1 mon")], None))
    assert recorder.failed(ALWAYS) == [ONLY_OVERFLOW]


def test_a_repair_counts_only_for_the_view_that_errored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    a, recorder = component(monkeypatch)
    poisoned = [(1, 2, "1 mon"), (7, 2, POISON), (8, 2, POISON)]
    healthy = [(1, 2, "1 mon")]
    view = [(2, "1 mon", 1, 1)]
    a.judge("mv", Read(poisoned, None))
    a.judge("index", Read(healthy, view))
    assert REPAIRED not in recorder.held()
    a.judge("mv", Read(healthy, view))
    assert REPAIRED in recorder.held()
    assert recorder.failed(ALWAYS) == []


def test_writes_heal_a_seen_poison_and_keep_the_table_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    a, _ = component(monkeypatch)
    d = SeededDraw(11)
    a.judge("mv", Read([(7, 2, POISON), (8, 2, POISON)], None))
    for _ in range(40):
        a.write(d)
    sent = a.ctx.sessions.sent  # pyright: ignore[reportAttributeAccessIssue]
    assert ("aggregates heal", "DELETE FROM agg.t WHERE id IN (7, 8)") in sent
    # No second poison while one is known.
    assert not any(name == "aggregates poison" for name, _ in sent)

    a.judge("mv", Read([], []))
    for _ in range(600):
        a.write(d)
    assert any(name == "aggregates poison" for name, _ in sent)
    inserted = [
        int(key)
        for _, statement in sent
        if statement.startswith("INSERT")
        for key in re.findall(r"\((\d+), \d+, ", statement)
    ]
    assert inserted == sorted(set(inserted))
    bounds = [
        int(statement.rsplit(" ", 1)[1])
        for name, statement in sent
        if name == "aggregates trim"
    ]
    assert bounds and bounds == sorted(bounds)
    assert inserted[-1] - bounds[-1] <= aggregates.MAX_ROWS + 2

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

from materialize.antithesis import window_retractions_model as model
from materialize.antithesis_driver import windows
from materialize.antithesis_driver.client import Op, Status
from materialize.antithesis_driver.draw import SeededDraw

INDEX = "window retractions: maintained index equals the input-derived window oracle"
MV = "window retractions: materialized windows equal the input-derived window oracle"
UNIQUE = "window retractions: every base row id appears once"

BASE: list[model.Input] = [
    (1, None, None, 3, 1, 101),
    (2, None, 0, None, -1, 102),
    (3, 0, 1, 5, None, 103),
    (4, 0, 1, -7, 2, 104),
    (5, 0, -2, 9, -2, 105),
    (6, 1, None, 4, 1, 106),
]


class Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[bool, str, Any]] = []

    def __call__(self, condition: bool, message: str, details: Any = None) -> None:
        self.calls.append((bool(condition), message, details))

    def failed(self) -> list[str]:
        return sorted({m for c, m, _ in self.calls if not c})

    def held(self) -> set[str]:
        return {m for c, m, _ in self.calls if c}


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


def component(monkeypatch: pytest.MonkeyPatch) -> tuple[windows.Windows, Recorder]:
    recorder = Recorder()
    monkeypatch.setattr(windows, "always", recorder)
    monkeypatch.setattr(windows, "sometimes", recorder)
    monkeypatch.setattr(windows, "ABANDON_SHARE", 0.0)
    return windows.Windows(Ctx()), recorder  # pyright: ignore[reportArgumentType]


def read(base: list[model.Input]) -> list[tuple[Any, ...]]:
    """What `check_query` returns when every view equals the model."""
    padding = (None,) * (model.WIDTH - len(windows.BASE_COLUMNS))
    rows: list[tuple[Any, ...]] = [("t", *row, *padding) for row in base]
    for i, spec in enumerate(windows.SPECS):
        for variant in windows.VARIANTS:
            rows += [
                (windows.view(i, variant), *row)
                for row in model.evaluate(base, spec).elements()
            ]
    return rows


def test_check_query_reads_the_base_and_every_view_with_one_shape() -> None:
    query = windows.check_query()
    parts = query.split(" UNION ALL ")
    assert len(parts) == 1 + len(windows.SPECS) * len(windows.VARIANTS)
    for part in parts:
        assert part.count("::bigint") == model.WIDTH
    for i in range(len(windows.SPECS)):
        for variant in windows.VARIANTS:
            assert f"FROM {windows.SCHEMA}.{windows.view(i, variant)}" in query


def test_setup_maintains_every_spec_as_an_index_and_a_materialized_view() -> None:
    statements = windows.setup_statements()
    assert sum("CREATE INDEX" in s for s in statements) == len(windows.SPECS)
    assert sum("CREATE MATERIALIZED VIEW" in s for s in statements) == len(
        windows.SPECS
    )
    frames = {spec.frame for spec in windows.SPECS}
    assert len(frames) == 3
    assert {(s.descending, s.nulls_first) for s in windows.SPECS} == {
        (False, False),
        (False, True),
        (True, False),
        (True, True),
    }


def test_views_that_equal_the_model_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    w, recorder = component(monkeypatch)
    assert w.compare(read(BASE))
    assert recorder.failed() == [
        "window retractions: a check compared windows after a row left a partition that kept others",
        "window retractions: a check compared windows after a row moved to another partition",
    ]
    assert (
        "window retractions: checked peer ranks and nullable offsets against generated input"
        in recorder.held()
    )
    assert w.compare([])
    assert INDEX not in recorder.failed() and MV not in recorder.failed()


@pytest.mark.parametrize("variant,message", [("v", INDEX), ("mv", MV)])
def test_a_view_that_differs_fails_its_own_path(
    monkeypatch: pytest.MonkeyPatch, variant: str, message: str
) -> None:
    w, recorder = component(monkeypatch)
    rows = read(BASE)
    name = windows.view(2, variant)
    victim = next(i for i, row in enumerate(rows) if row[0] == name)
    # The row number of one output row is off by one.
    row = list(rows[victim])
    row[1 + len(windows.BASE_COLUMNS)] += 1
    rows[victim] = tuple(row)
    assert w.compare(rows)
    assert message in recorder.failed()
    assert ({INDEX, MV} - {message}).isdisjoint(recorder.failed())
    details = next(d for c, m, d in recorder.calls if m == message and not c)
    assert list(details["views"]) == [name]


def test_a_duplicate_id_is_reported_and_nothing_is_compared(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    w, recorder = component(monkeypatch)
    assert not w.compare(read([*BASE, BASE[0]]))
    assert recorder.failed() == [UNIQUE]


def test_guards_see_a_move_and_a_departure(monkeypatch: pytest.MonkeyPatch) -> None:
    w, recorder = component(monkeypatch)
    w.compare(read(BASE))
    moved = [(3, 1, 1, 5, None, 103) if row[0] == 3 else row for row in BASE]
    w.compare(read(moved))
    w.compare(read([row for row in moved if row[0] != 4]))
    assert {
        "window retractions: a check compared windows after a row moved to another partition",
        "window retractions: a check compared windows after a row left a partition that kept others",
    } <= recorder.held()


def test_writes_keep_ids_unique_and_the_table_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    w, _ = component(monkeypatch)
    d = SeededDraw(7)
    for _ in range(600):
        w.write(d)
    sent = w.ctx.sessions.sent  # pyright: ignore[reportAttributeAccessIssue]
    inserted = [
        int(key)
        for _, statement in sent
        if statement.startswith("INSERT")
        for key in re.findall(r"\((\d+), ", statement)
    ]
    assert len(inserted) > windows.MAX_ROWS
    assert inserted == sorted(set(inserted))
    # `off` is the fifth column: never 0.
    offsets = [
        row.split(", ")[4]
        for _, statement in sent
        if statement.startswith("INSERT")
        for row in re.findall(r"\(([^)]*)\)", statement)
    ]
    assert set(offsets) <= {"NULL", "-2", "-1", "1", "2"}
    assert "0" not in re.findall(
        r"SET off = (\S+),", " ".join(statement for _, statement in sent)
    )
    # Before each trim, no more than MAX_ROWS ids lie above the last bound.
    bound = 0
    highest = 0
    for name, statement in sent:
        if name == "windows trim":
            assert highest - bound > windows.MAX_ROWS
            bound = int(statement.rsplit(" ", 1)[1])
            assert highest - bound == windows.MAX_ROWS // 2
        else:
            highest = max(
                [highest, *(int(k) for k in re.findall(r"\((\d+), ", statement))]
            )
            assert highest - bound <= windows.MAX_ROWS + 4
    assert bound > 0

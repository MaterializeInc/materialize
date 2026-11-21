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
import threading
import time
import weakref
from typing import Any

import psycopg
import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import session_rebinding_model as model
from materialize.antithesis_driver import rebinding as rb
from materialize.antithesis_driver.client import Op, Outcome, Status
from materialize.antithesis_driver.draw import SeededDraw

PARAMETERS = model.Parameters(2, 1, False)


class Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[bool, str, Any]] = []

    def __call__(self, condition: bool, message: str, details: Any = None) -> None:
        self.calls.append((bool(condition), message, details))

    def failed(self) -> list[str]:
        return sorted({m for c, m, _ in self.calls if not c})


def result(marker: int, binding: int, p: model.Parameters = PARAMETERS) -> list[tuple]:
    """What a correct system returns from the definition `marker`."""
    return [(*row, marker, "1700") for row in p.project(rb.INPUTS[binding])]


class Rows:
    def __init__(self, rows: list[tuple]) -> None:
        self.rows = rows

    def fetchall(self) -> list[tuple]:
        return self.rows


class Server:
    """Stands in for one connection: a durable view shared through `views`,
    and a temporary view, prepared statements and a cursor of its own."""

    def __init__(self, views: dict[str, tuple[int, int]]) -> None:
        self.views = views
        self.temp: tuple[int, int] | None = None
        self.prepared: dict[str, tuple[str, bool]] = {}
        self.cursor: list[tuple] | None = None
        self.sent: list[str] = []
        self.keep_cursor = False

    def _rows(self, source: str, p: model.Parameters) -> list[tuple]:
        view = self.temp if source == "v" and self.temp else self.views["v"]
        return result(view[0], view[1], p)

    def execute(self, statement: bytes, prepare: bool | None = None) -> Rows:
        assert prepare is False
        text = statement.decode()
        self.sent.append(text)
        select = re.search(
            r"\* (-?\d+)::bigint \+ (-?\d+)::bigint .* FROM (\S+) ", text
        )
        if text.startswith("PREPARE"):
            m = re.match(r"PREPARE (\S+) AS .* FROM (\S+) ORDER BY id (\w+)", text)
            assert m is not None
            self.prepared[m[1]] = (m[2], m[3] == "DESC")
        elif text.startswith("EXECUTE"):
            m = re.match(r"EXECUTE (\S+)\((-?\d+), (-?\d+)\)", text)
            assert m is not None
            source, descending = self.prepared[m[1]]
            p = model.Parameters(int(m[2]), int(m[3]), descending)
            return Rows(self._rows(source, p))
        elif text.startswith("DEALLOCATE"):
            del self.prepared[text.split()[1]]
        elif text.startswith("DROP VIEW IF EXISTS mz_temp.v"):
            self.temp = None
        elif text.startswith("CREATE TEMPORARY VIEW v"):
            m = re.search(r"(-?\d+)::bigint AS gen FROM rebind.t(\d)", text)
            assert m is not None
            self.temp = (int(m[1]), int(m[2]))
        elif text.startswith("DECLARE"):
            assert select is not None
            p = model.Parameters(int(select[1]), int(select[2]), "DESC" in text)
            self.cursor = self._rows("durable", p)
        elif text.startswith("FETCH"):
            if self.cursor is None:
                raise psycopg.errors.InvalidCursorName("cursor does not exist")
            count = text.split()[1]
            n = len(self.cursor) if count == "ALL" else int(count)
            page, self.cursor = self.cursor[:n], self.cursor[n:]
            return Rows(page)
        elif text == "COMMIT" and not self.keep_cursor:
            self.cursor = None
        elif select is not None:
            p = model.Parameters(int(select[1]), int(select[2]), "DESC" in text)
            return Rows(self._rows(select[3], p))
        return Rows([])


class Inline:
    """A pool that runs each operation at once on the connection given."""

    def __init__(self, conns: list[Server]) -> None:
        self.conns = conns

    def call(self, index: int, op: Op, wait: float) -> Status:
        start = time.monotonic()
        value = op.execute(self.conns[index])  # pyright: ignore[reportArgumentType]
        op.complete(Outcome(value, None, start, time.monotonic(), f"s{index}"))
        return Status.DONE


class Session(SeededDraw):
    """Draws that always pick one session."""

    def __init__(self, session: int, seed: int = 1) -> None:
        super().__init__(seed)
        self.session = session

    def integer(self, lo: int, hi: int) -> int:
        return (
            self.session
            if (lo, hi) == (0, rb.SESSIONS - 1)
            else super().integer(lo, hi)
        )


def shadow(c: rb.Rebinding, conn: Server, session: int) -> None:
    """Give `session` a temporary view."""
    seed = 0
    while conn.temp is None:
        c.temp(Session(session, seed))
        seed += 1


@pytest.fixture
def always(monkeypatch: pytest.MonkeyPatch) -> Recorder:
    recorder = Recorder()
    monkeypatch.setattr(rb, "always", recorder)
    monkeypatch.setattr(rb, "sometimes", Recorder())
    # Cursors linger between pages.
    monkeypatch.setattr(rb.time, "sleep", lambda seconds: None)
    return recorder


def component() -> tuple[rb.Rebinding, list[Server], dict[str, tuple[int, int]]]:
    views = {"v": (0, 0)}
    conns = [Server(views) for _ in range(rb.SESSIONS)]
    c = rb.Rebinding.__new__(rb.Rebinding)
    c.pool = Inline(conns)  # pyright: ignore[reportAttributeAccessIssue]
    c._lock = threading.Lock()
    c._states = weakref.WeakKeyDictionary()
    c.replaces = [rb.Replace(0, 0, float("-inf"), float("-inf"), "ok")]
    c.definitions = {0: 0}
    c._next = 1
    c._reads = {}
    return c, conns, views


def replace(marker: int, start: float, end: float | None, outcome: str) -> rb.Replace:
    return rb.Replace(marker, marker % 2, start, end, outcome)


def test_an_acknowledged_replacement_rules_out_those_before_it() -> None:
    first, second = replace(1, 1.0, 2.0, "ok"), replace(2, 3.0, 4.0, "ok")
    assert set(rb.allowed([first, second], 5.0, 6.0)) == {2}
    # The read overlaps the second replacement, so it may still see the first.
    assert set(rb.allowed([first, second], 3.5, 6.0)) == {1, 2}
    # The read ended before the second was sent.
    assert set(rb.allowed([first, second], 2.5, 2.8)) == {1}
    # Two replacements that overlap may have taken effect in either order.
    overlapping = replace(2, 1.5, 4.0, "ok")
    assert set(rb.allowed([first, overlapping], 5.0, 6.0)) == {1, 2}


def test_a_replacement_in_doubt_may_take_effect_at_any_later_time() -> None:
    doubt, later = replace(1, 1.0, 2.0, "unknown"), replace(2, 3.0, 4.0, "ok")
    assert set(rb.allowed([doubt, later], 5.0, 6.0)) == {1, 2}
    pending = replace(3, 5.5, None, "pending")
    assert set(rb.allowed([doubt, later, pending], 5.0, 6.0)) == {1, 2, 3}
    assert set(rb.allowed([later, replace(4, 1.0, 2.0, "rejected")], 5.0, 6.0)) == {2}
    # One in doubt does not rule out the acknowledged ones before it.
    assert set(rb.allowed([later, replace(5, 4.5, 4.6, "unknown")], 5.0, 6.0)) == {2, 5}


def test_a_result_must_be_one_definition_at_one_timestamp() -> None:
    definitions = {0: 0, 3: 1, -4: 0}
    assert rb.verdict(result(3, 1), PARAMETERS, definitions) == (3, None)
    assert rb.verdict(result(-4, 0), PARAMETERS, definitions) == (-4, None)
    descending = model.Parameters(-1, 7, True)
    assert rb.verdict(result(0, 0, descending), descending, definitions) == (0, None)
    for rows in (
        [],
        result(0, 0)[:-1] + result(3, 1)[-1:],
        result(0, 0)[:-1] + [(*result(0, 0)[-1][:4], "1701")],
        result(9, 0),
        result(3, 0),
        result(0, 0)[:-1],
        list(reversed(result(0, 0))),
    ):
        assert rb.verdict(rows, PARAMETERS, definitions)[1] is not None, rows


def test_cursor_pages_follow_the_snapshot() -> None:
    rows = result(0, 0)
    pages = [(2, rows[:2]), (3, rows[2:5]), (3, rows[5:]), (3, []), (None, [])]
    assert rb.paged(pages, PARAMETERS, 0)
    assert not rb.paged([(2, rows[:1]), (None, rows[1:])], PARAMETERS, 0)
    assert not rb.paged([(2, rows[:2]), (None, rows[3:])], PARAMETERS, 0)
    assert not rb.paged(pages, PARAMETERS, 1)


def test_reads_follow_replacements_and_their_own_temporary_view(
    always: Recorder,
) -> None:
    c, conns, views = component()
    c.prepared(Session(0))
    c.replace(Session(1))
    marker = c.replaces[-1].marker
    views["v"] = (marker, c.replaces[-1].binding)
    # The statement prepared before the replacement returns the new definition.
    c.prepared(Session(0))
    for seed in range(8):
        c.read(Session(0, seed))
        c.cursor(Session(2, seed))
    assert always.failed() == []
    assert (
        len({s.split()[1] for s in conns[0].sent if s.startswith("EXECUTE rebind_p")})
        <= 2
    )

    for seed in range(8):
        c.temp(Session(1, seed))
        c.read(Session(1, seed))
        c.read(Session(0, seed))
        c.prepared(Session(1, seed))
    assert always.failed() == []


def test_a_stale_definition_fails(always: Recorder) -> None:
    c, conns, views = component()
    c.replace(Session(1))
    # The server keeps answering from the definition that was replaced.
    c.prepared(Session(0))
    assert always.failed() == [
        "session rebinding: a read never returns a durable view definition that was replaced before it began"
    ]


def test_another_sessions_temporary_view_fails(always: Recorder) -> None:
    c, conns, views = component()
    shadow(c, conns[1], 1)
    conns[0].temp = conns[1].temp
    c.read(Session(0))
    assert always.failed() == [
        "session rebinding: results match generated session bindings and begun snapshots"
    ]
    always.calls.clear()
    # A session that has one must not fall through to the durable view.
    conns[1].temp = None
    c.read(Session(1))
    assert always.failed() == [
        "session rebinding: results match generated session bindings and begun snapshots"
    ]


def test_a_temporary_view_in_doubt_accepts_either(always: Recorder) -> None:
    c, conns, views = component()
    shadow(c, conns[1], 1)
    state = c._states[conns[1]]  # pyright: ignore[reportArgumentType]
    assert state.temp is not None and not state.uncertain
    # A statement that changes it ended without an answer.
    state.uncertain = True
    state.candidates = {state.temp}
    c.read(Session(1))
    conns[1].temp = None
    c.read(Session(1))
    assert always.failed() == []
    # A new connection has none.
    conns[1] = Server(views)
    c.pool.conns = conns  # pyright: ignore[reportAttributeAccessIssue]
    c.read(Session(1))
    assert always.failed() == []


def test_a_cursor_that_outlives_its_transaction_fails(always: Recorder) -> None:
    c, conns, views = component()
    conns[0].keep_cursor = True
    c.cursor(Session(0))
    assert always.failed() == ["session rebinding: COMMIT destroys its portals"]

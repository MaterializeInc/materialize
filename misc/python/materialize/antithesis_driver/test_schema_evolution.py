# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import time
from typing import Any

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import schema_model as model
from materialize.antithesis import sql
from materialize.antithesis_driver import schema_evolution as se
from materialize.antithesis_driver.client import Op, Outcome, Status
from materialize.antithesis_driver.draw import SeededDraw

REJECTED = sql.Classified(sql.Outcome.REJECTED, "40001", "rejected")
UNKNOWN = sql.Classified(sql.Outcome.INDETERMINATE, None, "connection lost")


class Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[bool, str, Any]] = []

    def __call__(self, condition: bool, message: str, details: Any = None) -> None:
        self.calls.append((bool(condition), message, details))

    def failed(self) -> list[str]:
        return sorted({m for c, m, _ in self.calls if not c})


class Statements:
    """Stands in for a connection, keeping the statements sent."""

    def __init__(self) -> None:
        self.sent: list[str] = []

    def execute(self, statement: bytes, prepare: bool | None = None) -> None:
        self.sent.append(statement.decode())


class Held:
    """A session pool that hands each operation to the test to finish."""

    sessions = [None]

    def __init__(self) -> None:
        self.ops: list[Op] = []

    def call(self, index: int, op: Op, wait: float) -> Status:
        self.ops.append(op)
        return Status.PENDING

    def finish(
        self, error: sql.Classified | None = None, value: Any = None
    ) -> list[str]:
        """Run the oldest operation's statements and complete it."""
        op = self.ops.pop(0)
        conn = Statements()
        op.execute(conn)  # pyright: ignore[reportArgumentType]
        now = time.monotonic()
        op.complete(Outcome(value, error, now, now, "s"))
        return conn.sent


@pytest.fixture
def always(monkeypatch: pytest.MonkeyPatch) -> Recorder:
    recorder = Recorder()
    monkeypatch.setattr(se, "always", recorder)
    monkeypatch.setattr(se, "sometimes", Recorder())
    monkeypatch.setattr(se, "reachable", Recorder())
    return recorder


def component() -> tuple[se.SchemaEvolution, Held]:
    class Ctx:
        sessions = Held()

    c = se.SchemaEvolution(Ctx())  # pyright: ignore[reportArgumentType]
    return c, Ctx.sessions


def read_outcome(start: float, end: float) -> Outcome[Any]:
    return Outcome(None, None, start, end, "s")


def test_select_star_must_show_acknowledged_columns_in_order() -> None:
    added = [("c1_i4", 10.0), ("c2_tx", 20.0)]
    base = list(model.BASE_COLUMNS)
    # Both acknowledged before the read began.
    assert se.columns_ok([*base, "c1_i4", "c2_tx"], added, None, 30.0)
    assert not se.columns_ok([*base, "c1_i4"], added, None, 30.0)
    # The second was acknowledged while the read ran, so it may be missing.
    assert se.columns_ok([*base, "c1_i4"], added, None, 15.0)
    assert se.columns_ok([*base, "c1_i4", "c2_tx"], added, None, 15.0)
    assert not se.columns_ok([*base, "c2_tx", "c1_i4"], added, None, 15.0)
    # A column in doubt may show, but only after the acknowledged ones.
    assert se.columns_ok([*base, "c1_i4", "c2_tx", "c3_nm"], added, "c3_nm", 30.0)
    assert not se.columns_ok([*base, "c1_i4", "c2_tx", "c3_nm"], added, None, 30.0)
    assert not se.columns_ok([*base, "c1_i4", "c3_nm"], added, "c3_nm", 15.0)
    assert not se.columns_ok(["k"], [], None, 0.0)


def test_a_column_in_doubt_is_retried_before_the_next_one(always: Recorder) -> None:
    c, pool = component()
    d = SeededDraw(1)
    table = c.tables[0]
    assert c.add_column(d) == Status.PENDING
    column = table.pending
    assert column is not None
    # Only one ADD COLUMN per table is in flight.
    assert c.add_column(d) == Status.NOT_SENT
    first = pool.finish(UNKNOWN)
    assert column in first[0]
    assert table.pending == column and table.added == []
    c.add_column(d)
    retry = pool.finish()
    assert f"ADD COLUMN IF NOT EXISTS {column} " in retry[0]
    assert [name for name, _ in table.added] == [column]
    assert table.pending is None

    c.add_column(d)
    rejected = table.pending
    pool.finish(REJECTED)
    assert table.pending is None and len(table.added) == 1
    c.add_column(d)
    assert table.pending not in (column, rejected)

    # A rejection after an attempt in doubt does not settle the column.
    pool.finish(UNKNOWN)
    doubt = table.pending
    c.add_column(d)
    pool.finish(REJECTED)
    assert table.pending == doubt


def test_inserts_name_only_acknowledged_columns(always: Recorder) -> None:
    c, pool = component()
    table = c.tables[0]
    table.added = [("c1_i4", 0.0)]
    table.pending = "c2_tx"
    for seed in range(40):
        assert c.insert(SeededDraw(seed)) == Status.PENDING
        (statement,) = pool.finish()
        entry = table.inserts[-1]
        assert entry.outcome == "ok" and entry.complete is not None
        assert "c2_tx" not in statement
        assert set(entry.written) <= {"k", "w", "c1_i4"} and entry.written[0] == "k"
        if "(k" not in statement.split(" VALUES ")[0]:
            # A positional insert supplies a prefix of the columns.
            assert entry.written == ("k", "w", "c1_i4")[: len(entry.written)]
    keys = [k for entry in table.inserts for k in entry.keys]
    assert len(keys) == len(set(keys))

    c.insert(SeededDraw(100))
    pool.finish(REJECTED)
    assert table.inserts[-1].outcome == "rejected"
    c.insert(SeededDraw(101))
    pool.finish(UNKNOWN)
    assert table.inserts[-1].outcome == "indeterminate"


def test_a_read_is_judged_against_the_ledger_and_the_columns(always: Recorder) -> None:
    c, _ = component()
    table = c.tables[0]
    table.added = [("c1_i4", 1.0)]
    table.inserts = [
        model.Insert((1,), ("k", "c1_i4"), "ok", 2.0, 3.0),
        model.Insert((2,), ("k", "w"), "indeterminate", 2.0, 3.0),
        model.Insert((3,), ("k",), "rejected", 2.0, 3.0),
    ]
    columns = ["k", "w", "c1_i4"]
    good = (1, "w-default", 1001)
    c.judge("t", table, (columns, [good]), read_outcome(4.0, 5.0))
    # The insert in doubt may have applied.
    c.judge("t", table, (columns, [good, (2, "w2", None)]), read_outcome(4.0, 5.0))
    assert always.failed() == []

    c.judge("t", table, (columns, []), read_outcome(4.0, 5.0))
    assert always.failed() == [
        "schema evolution: a read of an evolved table includes every row acknowledged before it began"
    ]
    always.calls.clear()
    c.judge(
        "t", table, (columns, [good, (3, "w-default", None)]), read_outcome(4.0, 5.0)
    )
    assert always.failed() == [
        "schema evolution: a read of an evolved table holds each row at most once and only rows that were attempted"
    ]
    always.calls.clear()
    c.judge("t", table, (columns, [(1, "w-default", 1002)]), read_outcome(4.0, 5.0))
    assert always.failed() == [
        "schema evolution: every row of an evolved table holds its written values and NULL or the default elsewhere"
    ]
    always.calls.clear()
    c.judge("t", table, (["k", "w"], [(1, "w-default")]), read_outcome(4.0, 5.0))
    assert always.failed() == [
        "schema evolution: SELECT * exposes every column whose ADD COLUMN was acknowledged before the read began, in the order added"
    ]


def test_a_dependent_keeps_its_columns_and_agrees_with_the_table(
    always: Recorder,
) -> None:
    c, _ = component()
    table = c.tables[0]
    star = se.Dependent("d0", ("CREATE ...",), None, None, readable=True)
    filtered = se.Dependent("d1", ("CREATE ...",), ("k", "c1_i4"), "c1_i4", True)
    base = (["k", "w", "c1_i4"], [(1, "w1", 1001), (2, "w2", None)])
    c.compare(table, star, base, (["k", "w"], [(2, "w2"), (1, "w1")]))
    c.compare(table, filtered, base, (["k", "c1_i4"], [(1, 1001)]))
    assert always.failed() == []
    assert star.shape == ("k", "w")

    c.compare(table, filtered, base, (["k", "c1_i4"], [(1, 1001), (2, None)]))
    c.compare(table, star, base, (["k", "w"], [(1, "w1")]))
    assert always.failed() == [
        "schema evolution: every dependent of an evolved table agrees with the table at one timestamp"
    ]
    always.calls.clear()
    # The dependent was planned against the first version and must stay there.
    c.compare(table, star, base, (["k", "w", "c1_i4"], base[1]))
    assert always.failed() == [
        "schema evolution: a dependent exposes exactly the columns it was defined over"
    ]


def test_a_full_table_is_replaced_and_dropped_once_idle(
    always: Recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    c, pool = component()
    d = SeededDraw(1)
    old = c.tables[0]
    assert c.rotate(d) == Status.NOT_SENT
    old.inserts = [
        model.Insert((k,), ("k",), "ok", 0.0, 0.0) for k in range(se.MAX_INSERTS)
    ]
    assert c.insert(d) == Status.NOT_SENT
    assert c.rotate(d) == Status.PENDING
    assert pool.finish() == [
        f"CREATE TABLE IF NOT EXISTS schema_evo.t1 {model.BASE_DDL}"
    ]
    assert c.current == 1 and old.retiring_since is not None

    # A read still in flight on the old table holds its drop back.
    old.in_flight = 1
    assert c.rotate(d) == Status.NOT_SENT
    now = time.monotonic()
    monkeypatch.setattr(se.time, "monotonic", lambda: now + se.RETIRE_AFTER_SECONDS + 1)
    assert c.rotate(d) == Status.PENDING
    assert pool.finish(UNKNOWN) == ["DROP TABLE IF EXISTS schema_evo.t0 CASCADE"]
    assert 0 in c.tables
    c.rotate(d)
    pool.finish()
    assert sorted(c.tables) == [1]

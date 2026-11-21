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
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import sql
from materialize.antithesis_driver import asof
from materialize.antithesis_driver.client import Op, Outcome, Status

COMPACTED = "as of: a reread was refused because its timestamp was compacted"
SELECT = "as of: a SELECT at a recorded timestamp returns the recorded table"
DERIVED = "as of: the indexed view and the materialized view at a recorded timestamp equal their definitions applied to the recorded table"
SNAPSHOT = "as of: a SUBSCRIBE snapshot at a recorded timestamp equals the record"
ACCUMULATED = "as of: a SUBSCRIBE's snapshot plus updates equals the record at every later recorded timestamp up to its end"
READABLE = "as of: a timestamp recorded within half the retain window stays readable"
EVENTUALLY = "as of: an observe and a reread of every recent recorded timestamp succeed after faults stop"
ALWAYS = {SELECT, DERIVED, SNAPSHOT, ACCUMULATED, READABLE, EVENTUALLY}


class Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[bool, str, Any]] = []

    def __call__(self, condition: bool, message: str, details: Any = None) -> None:
        self.calls.append((bool(condition), message, details))

    def failed(self) -> list[str]:
        return sorted({m for c, m, _ in self.calls if not c})

    def held(self) -> set[str]:
        return {m for c, m, _ in self.calls if c}


class System:
    """Answers the component's statements from a history of the table, as a
    correct Materialize would."""

    def __init__(self) -> None:
        self.history: dict[int, asof.Rows] = {}
        self.now = 0

    def at(self, ts: int) -> asof.Rows:
        return self.history[max(t for t in self.history if t <= ts)]

    def commit(self, ts: int, table: dict[tuple[int, int], int]) -> None:
        self.history[ts] = dict(table)
        self.now = ts

    def __call__(self, statement: str) -> list[tuple[Any, ...]]:
        if statement.startswith("SELECT mz_now()"):
            rows = expand(self.at(self.now))
            return [(self.now, *r) for r in rows] or [(self.now, None, None)]
        m = re.fullmatch(r"SELECT .* FROM asof\.(\w+) AS OF (\d+)", statement)
        if m:
            return expand(asof.derive(m[1], self.at(int(m[2]))))
        m = re.fullmatch(
            r"COPY \(SUBSCRIBE \(SELECT .* FROM asof\.(\w+)\) AS OF (\d+) UP TO (\d+)\) TO STDOUT",
            statement,
        )
        assert m, statement
        obj, a, up_to = m[1], int(m[2]), int(m[3])
        out = [(a, 1, *r) for r in expand(asof.derive(obj, self.at(a)))]
        before = asof.derive(obj, self.at(a))
        for t in sorted(t for t in self.history if a < t < up_to):
            after = asof.derive(obj, self.history[t])
            for row in set(before) | set(after):
                change = after.get(row, 0) - before.get(row, 0)
                if change:
                    out.append((t, change, *row))
            before = after
        return [tuple(str(x) for x in r) for r in out]


def expand(rows: asof.Rows) -> list[tuple[int, ...]]:
    return [r for r, n in sorted(rows.items()) for _ in range(n)]


class Copy:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows

    def rows(self) -> list[tuple[Any, ...]]:
        return self._rows


class Conn:
    def __init__(self, answer: Callable[[str], list[tuple[Any, ...]]]) -> None:
        self.answer = answer
        self.rows: list[tuple[Any, ...]] = []

    def execute(self, statement: bytes) -> Conn:
        self.rows = self.answer(statement.decode())
        return self

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def fetchone(self) -> tuple[Any, ...]:
        return self.rows[0]

    def cursor(self) -> Conn:
        return self

    @contextmanager
    def copy(self, statement: bytes) -> Iterator[Copy]:
        yield Copy(self.answer(statement.decode()))


class Pool:
    """Runs each operation at once against `answer`, or fails it with `error`."""

    def __init__(self, answer: Callable[[str], list[tuple[Any, ...]]]) -> None:
        self.sessions = [None] * 4
        self.answer = answer
        self.error: sql.Classified | None = None
        self.ops: list[Op] = []

    def call(self, index: int, op: Op, wait: float) -> Status:
        self.ops.append(op)
        if self.error is not None:
            op.complete(Outcome(None, self.error, 0.0, 0.0, "s0"))
        else:
            value = op.execute(Conn(self.answer))  # pyright: ignore[reportArgumentType]
            op.complete(Outcome(value, None, 0.0, 0.0, "s0"))
        return Status.DONE


class Ctx:
    def __init__(self, pool: Pool) -> None:
        self.sessions = pool


def component(
    monkeypatch: pytest.MonkeyPatch, answer: Callable[[str], list[tuple[Any, ...]]]
) -> tuple[asof.AsOf, Pool, Recorder]:
    recorder = Recorder()
    for name in ("always", "sometimes", "always_or_unreachable"):
        monkeypatch.setattr(asof, name, recorder)
    pool = Pool(answer)
    return asof.AsOf(Ctx(pool)), pool, recorder  # pyright: ignore[reportArgumentType]


def now_ms() -> int:
    return int(time.time() * 1000)


def observe(c: asof.AsOf, system: System, ts: int, table: dict) -> None:
    system.commit(ts, table)
    c.ctx.sessions.call(0, c._observe_op({}, []), 1.0)


def test_derive_applies_the_view_definitions() -> None:
    table = {(1, 10): 1, (5, 2): 1, (2, 3): 2}
    assert asof.derive("t", table) == table
    assert asof.derive("v", table) == {(1, 29): 1, (5, 1): 1, (2, 7): 2}
    assert asof.derive("mv", table) == {(1, 2, 12): 1, (2, 2, 6): 1}
    assert asof.derive("mv", {}) == {}


def test_equal_rereads_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    system = System()
    c, _, recorder = component(monkeypatch, system)
    base = now_ms()
    observe(c, system, base, {(1, 10): 1, (2, 20): 1, (6, 7): 1})
    observe(c, system, base + 5, {(1, 10): 1, (6, 8): 1})
    observe(c, system, base + 5, {(1, 10): 1, (6, 8): 1})
    assert set(c.records) == {base, base + 5}
    for ts in (base, base + 5):
        for obj in asof.OBJECTS:
            c.ctx.sessions.call(0, c._reread_op(ts, c.records[ts], obj, "r2", []), 1.0)
    assert recorder.failed() == [
        "as of: a reread succeeded at a timestamp recorded over 2 minutes earlier"
    ]
    assert {SELECT, DERIVED, "as of: a reread ran on replica r2"} <= (recorder.held())


def test_observe_records_an_empty_table(monkeypatch: pytest.MonkeyPatch) -> None:
    system = System()
    c, _, _ = component(monkeypatch, system)
    observe(c, system, now_ms(), {})
    assert list(c.records.values()) == [{}]


def test_a_differing_observe_at_a_recorded_timestamp_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    system = System()
    c, _, recorder = component(monkeypatch, system)
    ts = now_ms()
    observe(c, system, ts, {(1, 10): 1})
    observe(c, system, ts, {(1, 11): 1})
    assert recorder.failed() == [SELECT]
    assert c.records[ts] == {(1, 10): 1}


@pytest.mark.parametrize(
    "obj,message", [("t", SELECT), ("v", DERIVED), ("mv", DERIVED)]
)
def test_a_differing_reread_fails_its_message(
    monkeypatch: pytest.MonkeyPatch, obj: str, message: str
) -> None:
    system = System()

    def stale(statement: str) -> list[tuple[Any, ...]]:
        # A read as of the second timestamp returns the first one's rows.
        return system(statement.replace(f"AS OF {ts + 5}", f"AS OF {ts}"))

    c, _, recorder = component(monkeypatch, stale)
    ts = now_ms()
    observe(c, system, ts, {(1, 10): 1, (2, 20): 1})
    observe(c, system, ts + 5, {(2, 20): 1})
    c.ctx.sessions.call(0, c._reread_op(ts, c.records[ts], obj, None, []), 1.0)
    assert message not in recorder.failed()
    c.ctx.sessions.call(0, c._reread_op(ts + 5, c.records[ts + 5], obj, None, []), 1.0)
    assert message in recorder.failed()
    assert ({SELECT, DERIVED} - {message}).isdisjoint(recorder.failed())
    details = next(d for ok, m, d in recorder.calls if m == message and not ok)
    assert details["object"] == obj and details["extra"]


def compaction_error() -> psycopg.Error:
    return psycopg.errors.lookup("22000")(
        "could not find a valid timestamp for the query"
    )


def compacting(system: System) -> Callable[[str], list[tuple[Any, ...]]]:
    """Answers like `system`, but refuses every read at a past timestamp."""

    def answer(statement: str) -> list[tuple[Any, ...]]:
        if " AS OF " in statement:
            raise compaction_error()
        return system(statement)

    return answer


def test_classify_read_rejects_only_the_compaction_error() -> None:
    assert asof.classify_read(compaction_error()).outcome == sql.Outcome.REJECTED
    other = psycopg.errors.lookup("22000")("something else")
    assert asof.classify_read(other).outcome == sql.Outcome.VIOLATION
    with pytest.raises(type(other)):
        asof.refused(Conn(System()), other)  # pyright: ignore[reportArgumentType]


def test_a_compacted_timestamp_is_dropped_and_not_a_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    system = System()
    c, pool, recorder = component(monkeypatch, compacting(system))
    old = now_ms() - asof.READABLE_MS - 1000
    observe(c, system, old, {(1, 10): 1})
    # The system's clock, not the driver's, says how old the record is.
    observe(c, system, now_ms(), {(1, 10): 1})
    done: list[bool] = []
    pool.call(0, c._reread_op(old, c.records[old], "v", "r1", done), 1.0)
    assert old not in c.records and done
    assert recorder.failed() == []
    assert COMPACTED in recorder.held()


def test_a_young_timestamp_that_was_compacted_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    system = System()
    c, pool, recorder = component(monkeypatch, compacting(system))
    ts = now_ms()
    observe(c, system, ts, {(1, 10): 1})
    pool.call(0, c._reread_op(ts, c.records[ts], "t", None, []), 1.0)
    assert recorder.failed() == [READABLE]
    assert ts not in c.records


def test_another_failure_is_no_information(monkeypatch: pytest.MonkeyPatch) -> None:
    system = System()
    c, pool, recorder = component(monkeypatch, system)
    ts = now_ms()
    observe(c, system, ts, {(1, 10): 1})
    pool.error = sql.Classified(sql.Outcome.INDETERMINATE, "57014", "canceled")
    done: list[bool] = []
    pool.call(0, c._reread_op(ts, c.records[ts], "t", "r1", done), 1.0)
    assert not done and ts in c.records and recorder.calls == []


def history(c: asof.AsOf, system: System) -> list[int]:
    base = now_ms()
    tables = [
        {(1, 10): 1, (2, 20): 1},
        {(1, 10): 1, (2, 21): 1, (5, 1): 1},
        {(2, 21): 1, (5, 1): 1},
        {(2, 21): 1, (5, 1): 1, (9, 4): 1},
    ]
    for i, table in enumerate(tables):
        observe(c, system, base + 10 * i, table)
    # A write between records, at a timestamp no observe saw.
    system.commit(base + 15, {(1, 10): 1, (2, 22): 1, (5, 1): 1})
    return sorted(c.records)


@pytest.mark.parametrize("obj", asof.OBJECTS)
def test_subscribe_accumulates_to_every_record(
    monkeypatch: pytest.MonkeyPatch, obj: str
) -> None:
    system = System()
    c, _, recorder = component(monkeypatch, system)
    times = history(c, system)
    between = {ts: c.records[ts] for ts in times}
    c.ctx.sessions.call(0, c._subscribe_op(between, obj, None, []), 1.0)
    assert recorder.failed() == []
    assert {SNAPSHOT, ACCUMULATED} <= recorder.held()
    assert (
        "as of: a SUBSCRIBE saw an update between its two recorded timestamps"
        in recorder.held()
    )


@pytest.mark.parametrize(
    "corrupt,messages",
    [
        # The snapshot misses a row, so every later state does too.
        (lambda rows: rows[1:], [SNAPSHOT, ACCUMULATED]),
        # The last update is lost.
        (lambda rows: rows[:-1], [ACCUMULATED]),
        # An update beyond `UP TO`.
        (lambda rows: [*rows, (str(10**15), "1", "7", "7")], [ACCUMULATED]),
    ],
)
def test_subscribe_fails_the_path_that_differs(
    monkeypatch: pytest.MonkeyPatch,
    corrupt: Callable[[list[tuple[Any, ...]]], list[tuple[Any, ...]]],
    messages: list[str],
) -> None:
    system = System()

    def answer(statement: str) -> list[tuple[Any, ...]]:
        rows = system(statement)
        return corrupt(rows) if statement.startswith("COPY") else rows

    c, _, recorder = component(monkeypatch, answer)
    between = {ts: c.records[ts] for ts in history(c, system)}
    c.ctx.sessions.call(0, c._subscribe_op(between, "t", None, []), 1.0)
    assert recorder.failed() == sorted(messages)


def test_prune_thins_to_a_spread_and_expires(monkeypatch: pytest.MonkeyPatch) -> None:
    system = System()
    c, _, _ = component(monkeypatch, system)
    base = now_ms() - asof.EXPIRE_MS + 60_000
    for i in range(3 * asof.MAX_RECORDS):
        observe(c, system, base + 1000 * i, {(i, i): 1})
    times = sorted(c.records)
    assert len(times) == asof.MAX_RECORDS
    assert times[0] == base and times[-1] == base + 1000 * (3 * asof.MAX_RECORDS - 1)
    gaps = [b - a for a, b in zip(times, times[1:])]
    assert max(gaps) <= 4 * min(gaps)
    with c._lock:
        c.records[base - 120_000] = {}
        c._prune()
    assert base - 120_000 not in c.records


def test_eventually_rereads_every_recent_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    system = System()
    c, pool, recorder = component(monkeypatch, system)
    old = now_ms() - asof.READABLE_MS - 1000
    observe(c, system, old, {(1, 1): 1})
    history(c, system)
    pool.ops.clear()
    c.eventually(time.monotonic() + 10)
    assert ALWAYS.isdisjoint(recorder.failed())
    assert EVENTUALLY in recorder.held()
    rereads = [op for op in pool.ops if op.name == "as of reread"]
    # Five recent records, the one taken by the eventually's observe among
    # them, from each object. The old record is not required.
    assert len(rereads) == 5 * len(asof.OBJECTS)
    assert sum(op.name == "as of subscribe" for op in pool.ops) == len(asof.OBJECTS)


def test_writes_stay_within_the_key_range() -> None:
    from materialize.antithesis_driver.draw import SeededDraw

    sent: list[str] = []
    pool = Pool(lambda s: sent.append(s) or [])  # type: ignore[func-returns-value]
    c = asof.AsOf(Ctx(pool))  # pyright: ignore[reportArgumentType]
    d = SeededDraw(3)
    c.configure(d)
    for _ in range(300):
        c.write(d)
    keys = [
        int(k)
        for s in sent
        for k in re.findall(r"\((\d+), -?\d+\)", s)
        + re.findall(r"\d+", "".join(re.findall(r"k IN \(([\d, ]+)\)", s)))
    ]
    assert len(keys) >= 300 and max(keys) <= asof.MAX_KEY
    assert {s.split()[0] for s in sent} == {"INSERT", "UPDATE", "DELETE"}

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random

import pytest

from materialize.antithesis import window_retractions_model as m


def test_peer_ranks_null_ordering_and_current_row_defaults():
    rows = [(1, 0, None, None, 1, 91), (2, 0, None, 7, -1, 92), (3, 0, 4, 5, None, 93)]
    actual = {row[0]: row for row in m.evaluate(rows, m.Spec(False, True, -1, -1))}
    assert actual[1][6:] == (1, 1, 1, 91, 7, 91, 7, 0, None, None, None, 7)
    assert actual[2][6:] == (2, 1, 1, 5, None, 5, 92, 0, None, None, None, 7)
    assert actual[3][6:] == (3, 3, 2, None, None, None, None, 1, 7, 7, 7, 12)


def test_descending_nulls_last_preserves_peer_rank_gaps():
    rows = [(1, None, None, 2, 1, 91), (2, None, 8, 3, 1, 92), (3, None, 8, 4, 1, 93)]
    actual = {row[0]: row for row in m.evaluate(rows, m.Spec(True, False, 1, 2))}
    assert actual[2][6:9] == (1, 1, 1)
    assert actual[3][6:9] == (2, 1, 1)
    assert actual[1][6:9] == (3, 3, 2)
    assert actual[1][13:17] == (0, None, None, None)
    assert actual[2][-1] == actual[3][-1] == 7


def test_generated_schedules_restore_input_after_partition_delete_reinsert():
    specs = set()
    for seed in range(100):
        plan = m.generate(random.Random(seed))
        specs.add(plan.spec)
        state = plan.seed
        assert 12 <= len(state) <= 21
        for index, mutations in enumerate(plan.phases):
            previous = state
            state = m.apply(state, mutations)
            assert len(state) <= 21
            assert sum(m.evaluate(state, plan.spec).values()) == len(state)
            if index == 2:
                assert any(row[1] is None for row in previous)
                assert not any(row[1] is None for row in state)
            if index == 3:
                assert any(row[1] is None for row in state)
        assert state == plan.seed
        assert all(row[4] != 0 for row in plan.seed)
    assert len(specs) == 12


def test_stream_consolidates_replacements_at_each_closed_timestamp():
    row = next(iter(m.evaluate([(1, 0, 1, 7, 1, 8)], m.Spec(False, False, -1, -1))))
    stream = m.Stream()
    stream.feed([(10, True, *([object()] * (m.WIDTH + 1))), (10, False, 1, *row)])
    assert not stream.bag
    stream.feed([(11, True, *([None] * (m.WIDTH + 1)))])
    assert stream.bag == {row: 1}
    stream.feed(
        [
            (12, False, -1, *row),
            (12, False, 1, *row),
            (13, True, *([None] * (m.WIDTH + 1))),
        ]
    )
    assert stream.bag == {row: 1}
    assert stream.retracted
    assert not stream.errors


@pytest.mark.parametrize(
    "row",
    [
        None,
        (),
        (10, False, 1),
        (None, True, *([None] * (m.WIDTH + 1))),
        (10, False, None, *([None] * m.WIDTH)),
    ],
)
def test_stream_malformed_data_becomes_protocol_violation(row):
    stream = m.Stream()
    stream.feed([row])
    assert stream.errors == ["malformed subscription row"]


def test_stream_rejects_below_frontier_and_negative_closed_multiplicity():
    row = next(iter(m.evaluate([(1, 0, 1, 7, 1, 8)], m.Spec(False, False, -1, -1))))
    stream = m.Stream()
    stream.feed(
        [
            (10, True, *([None] * (m.WIDTH + 1))),
            (9, False, -1, *row),
            (11, True, *([None] * (m.WIDTH + 1))),
        ]
    )
    assert "subscription data arrived below closed progress" in stream.errors
    assert "closed output has negative multiplicity" in stream.errors


def test_generated_oracle_matches_independent_sqlite_window_semantics():
    import sqlite3

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t(id int,g int,o int,v int,off int,def int)")
    # SQLite lacks IGNORE NULLS. Every other result has an independent SQL
    # implementation here, including peer ranks and empty bounded frames.
    # SQLite offset NULL/default semantics differ from PostgreSQL, and LAG
    # returns its default for offsets below -1. Cross-check positive offsets.
    # Negative and NULL offsets have explicit model controls.
    retained = [index for index in range(m.WIDTH) if index not in (11, 12)]
    for seed in range(24):
        plan = m.generate(random.Random(seed))
        rows = plan.seed
        for mutations in ((), *plan.phases):
            rows = m.apply(rows, mutations)
            rows = tuple(
                (*row[:4], 1 if row[4] is None else abs(row[4]), row[5]) for row in rows
            )
            conn.execute("DELETE FROM t")
            conn.executemany("INSERT INTO t VALUES(?,?,?,?,?,?)", rows)
            actual = conn.execute(
                m.query("t", plan.spec).replace(" IGNORE NULLS", "")
            ).fetchall()
            actual = sorted(tuple(row[index] for index in retained) for row in actual)
            expected = sorted(
                tuple(row[index] for index in retained)
                for row in m.evaluate(rows, plan.spec).elements()
            )
            assert actual == expected, (seed, plan.spec)
    conn.close()

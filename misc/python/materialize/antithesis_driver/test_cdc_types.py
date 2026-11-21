# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis_driver import cdc_types as t

DIALECTS = {"postgres": t.POSTGRES, "mysql": t.MYSQL, "sql server": t.SQL_SERVER}
dialects = pytest.mark.parametrize("columns", DIALECTS.values(), ids=DIALECTS.keys())


def rows(columns: tuple[t.Column, ...], keys: list[tuple[int, int, int]]) -> list:
    return [
        (k, seed, tseed, *(t.value(c, seed, tseed)[1] for c in columns))
        for k, seed, tseed in keys
    ]


@dialects
def test_the_sweep_rows_hold_every_sample_of_every_column(
    columns: tuple[t.Column, ...],
) -> None:
    assert len({c.name for c in columns}) == len(columns)
    for c in columns:
        if c.big is not None:
            assert not c.samples
            continue
        assert 1 < len(c.samples) <= t.SWEEP, c.name
        assert c.samples[0] == (None, None), c.name
        assert {t.value(c, s, s) for s in range(t.SWEEP)} == set(c.samples), c.name
        assert all(t.value(c, s, 0) in c.samples for s in (t.SWEEP, 10**6, 2**32))


@dialects
def test_statements_bind_one_parameter_per_placeholder(
    columns: tuple[t.Column, ...],
) -> None:
    big = [c for c in columns if c.big is not None]
    assert big
    for statement, params in (
        t.insert("x", columns, 40, 7, 9),
        t.update("x", columns, 40, 42, 7, 9),
        t.update("x", columns, 40, 42, t.UNTOUCHED, None),
        t.delete("x", 40, 42),
        *t.sweep("x", columns),
    ):
        assert statement.count("%s") == len(params), statement
    untouched, _ = t.update("x", columns, 40, 42, t.UNTOUCHED, None)
    assert "tseed" not in untouched
    assert not any(f"{c.name} =" in untouched for c in big)
    full, _ = t.update("x", columns, 40, 42, 7, 9)
    assert all(f"{c.name} =" in full for c in columns) and "tseed = %s" in full
    assert t.select("x", columns).count(",") >= len(columns) + 2
    text = [c.name for c in columns if c.text]
    assert t.text_columns(columns) == f" WITH (TEXT COLUMNS ({', '.join(text)}))"


@dialects
def test_a_row_is_judged_by_its_own_seeds(columns: tuple[t.Column, ...]) -> None:
    keys = [(k, k, k) for k in range(t.SWEEP)]
    keys += [(t.SWEEP, 12345, 3), (t.SWEEP + 1, t.UNTOUCHED + 5, 4)]
    good = rows(columns, keys)
    v = t.judge(columns, good)
    assert (v.rows, v.mismatches, v.duplicates) == (len(keys), [], [])
    assert (v.snapshot_rows, v.workload_rows, v.untouched_rows) == (t.SWEEP, 2, 1)
    assert v.nulls > 0 and v.keys[t.SWEEP] == (12345, 3)

    # A value of another seed, a value where NULL belongs, and a lost value.
    index = next(i for i, c in enumerate(columns) if c.big is None)
    name = columns[index].name
    for row, wrong in ((1, good[2][3 + index]), (0, "x"), (1, None)):
        bad = [list(r) for r in good]
        assert bad[row][3 + index] != wrong
        bad[row][3 + index] = wrong
        v = t.judge(columns, [tuple(r) for r in bad])
        assert [(m["k"], m["column"]) for m in v.mismatches] == [(row, name)]

    # A large column that kept the value of an earlier `tseed`.
    stale = rows(columns, [(t.SWEEP, 1, 1)])[0]
    current = list(rows(columns, [(t.SWEEP, 1, 2)])[0])
    big = next(i for i, c in enumerate(columns) if c.big is not None)
    current[3 + big] = stale[3 + big]
    v = t.judge(columns, [tuple(current)])
    assert [m["column"] for m in v.mismatches] == [columns[big].name]

    v = t.judge(columns, good + [good[5]])
    assert v.duplicates == [5]


def test_large_values_differ_by_seed_and_are_compared_by_digest() -> None:
    a, b = t.big_text(1), t.big_text(2)
    assert len(a) == len(b) == t.BIG_CHARS and a != b and a == t.big_text(1)
    column = next(c for c in t.POSTGRES if c.name == "c_big_bytea")
    literal, expected = t.value(column, 0, 1)
    assert literal == a and expected == f"{t.BIG_CHARS // 2}:{t.md5(a)}"

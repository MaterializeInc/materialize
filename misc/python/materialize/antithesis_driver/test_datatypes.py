# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis_driver import datatypes as dt

SEEDS = [seed * 7919 for seed in range(1, 400)]


def column(name: str) -> int:
    return next(i for i, c in enumerate(dt.COLUMNS) if c.name == name)


def read(seeds: list[int]) -> list[tuple[Any, ...]]:
    """What `read_query` returns for a table of these seeds, ids from 1."""
    rows: list[tuple[Any, ...]] = []
    for path in dt.READ_PATHS:
        for id, seed in enumerate(seeds, 1):
            texts = [None if v is None else v.text for v in dt.row_values(seed)]
            rows.append((path, id, seed, *texts))
    padding = [None] * (len(dt.COLUMNS) - 2)
    for col, lo, hi, n in dt.expected_groups(seeds):
        rows.append(("grouped", col, lo, str(hi), str(n), *padding))
    for col, direction, seed in dt.expected_extremes(seeds):
        rows.append(("extremes", col, seed, str(direction), None, *padding))
    return rows


def test_values_are_a_pure_function_of_the_seed() -> None:
    # A row written by one statement is read back by another, so a change of
    # these values would make every check fail.
    assert dt.mix(1, 0) == dt.mix(1, 0)
    assert dt.mix(1, 0) != dt.mix(1, 1) != dt.mix(2, 1)
    assert dt.row_values(42) == dt.row_values(42)
    values = dt.row_values(12345)
    assert values[column("c_int4")] == dt.Value(
        "-1383302865", "-1383302865", -1383302865
    )
    assert values[column("c_int2")] is None


def test_every_column_is_null_an_edge_and_generated_within_a_few_hundred_seeds() -> (
    None
):
    names = [c.name for c in dt.COLUMNS]
    assert len(set(names)) == len(names)
    kinds = {
        (i, "null" if v is None else "edge" if v.edge else "generated")
        for seed in SEEDS
        for i, v in enumerate(dt.row_values(seed))
    }
    assert len(kinds) == 3 * len(dt.COLUMNS)
    for c in dt.COLUMNS:
        picked = {v.input for s in SEEDS if (v := c.pick(dt.mix(s, 0))) and v.edge}
        assert picked == {e.input for e in c.edges}, c.name


@pytest.mark.parametrize(
    "fields,text",
    [
        ((0, 0, 0), "00:00:00"),
        ((14, 3, 14_706_789_000), "1 year 2 mons 3 days 04:05:06.789"),
        ((0, -1, 0), "-1 days"),
        ((-14, 3, -14_706_000_000), "-1 years -2 mons +3 days -04:05:06"),
        ((0, 0, 90_000_000_000), "25:00:00"),
        ((1, 1, 1), "1 mon 1 day 00:00:00.000001"),
        ((-1, 0, 3_600_000_000), "-1 mons +01:00:00"),
        ((0, 2, -1_000_000), "2 days -00:00:01"),
    ],
)
def test_intervals_render_in_the_postgres_style(
    fields: tuple[int, int, int], text: str
) -> None:
    assert dt.interval_text(*fields) == text


def test_elements_are_quoted_where_the_escaper_quotes_them() -> None:
    assert dt.quote("abc") == "abc"
    assert dt.quote("") == '""'
    assert dt.quote("null") == '"null"'
    assert dt.quote("a b") == '"a b"'
    assert dt.quote('a"b\\') == '"a\\"b\\\\"'
    assert dt.quote("{}") == '"{}"'
    # Only ASCII whitespace quotes.
    assert dt.quote("a b") == "a b"
    assert dt.quote("x=>y") == "x=>y"
    assert dt.quote_in_map("x=>y") == '"x=>y"'


def test_values_render_as_materialize_stores_them() -> None:
    def edges(name: str) -> dict[str, str]:
        return {e.input: e.text for e in dt.COLUMNS[column(name)].edges}

    assert edges("c_numeric")["1.50"] == "1.5"
    assert edges("c_numeric")["-0"] == "0"
    assert edges("c_timestamptz")["2024-02-29 12:00:00+05:30"] == (
        "2024-02-29 06:30:00+00"
    )
    assert edges("c_timestamp_ms")["2024-02-29 12:00:00.123456"] == (
        "2024-02-29 12:00:00.123"
    )
    assert edges("c_float8")["1e21"] == "1e+21"
    assert edges("c_interval")["-1 day"] == "-1 days"
    assert edges("c_bool_array")["{true,false}"] == "{t,f}"
    assert any('"4714-11-24 BC"' in text for text in edges("c_date_list").values())
    char = dt.COLUMNS[column("c_char")]
    assert char.wire("ab") == "ab    "
    assert dt.COLUMNS[column("c_bool")].wire("true") == "t"
    assert dt.literal(dt.COLUMNS[column("c_text")], dt.Value("it's", "it's")) == (
        "'it''s'::text"
    )
    assert dt.literal(dt.COLUMNS[column("c_text")], None) == "NULL::text"


def test_sort_keys_order_as_materialize_orders() -> None:
    def keys(name: str, *inputs: str) -> list[Any]:
        by_input = {e.input: e.order for e in dt.COLUMNS[column(name)].edges}
        return [by_input[i] for i in inputs]

    nan, inf, negative, zero = keys("c_float8", "NaN", "Infinity", "-0", "0")
    assert inf < nan and negative == zero
    assert keys("c_numeric", "NaN")[0] > keys("c_numeric", "1.50")[0]
    bc, first = keys("c_date", "4714-11-24 BC", "0001-01-01")
    assert bc < first
    # Fields compare one by one, months first: a month is more than 31 days.
    month, days = keys("c_interval", "1 mon", "30 days")
    assert month > days
    empty, blank = keys("c_bytea", "\\x", "\\x00")
    assert empty < blank


def test_a_read_that_matches_its_seeds_is_clean() -> None:
    seeds = SEEDS[:40]
    verdict = dt.judge(read(seeds))
    assert not verdict.wrong_values and not verdict.paths_differ
    assert not verdict.wrong_groups and not verdict.wrong_extremes
    assert verdict.rows == dict(enumerate(seeds, 1))
    assert dt.judge([]).rows == {}


def test_a_wrong_value_is_reported_with_its_column() -> None:
    i = column("c_uuid")
    seed = next(s for s in SEEDS if dt.row_values(s)[i] is not None)
    rows = read([seed])
    for path in range(len(dt.READ_PATHS)):
        row = list(rows[path])
        row[3 + i] = "00000000-0000-0000-0000-00000000dead"
        rows[path] = tuple(row)
    verdict = dt.judge(rows)
    assert [w["column"] for w in verdict.wrong_values] == ["c_uuid"]
    assert not verdict.paths_differ


def test_a_read_path_that_differs_from_the_table_is_reported() -> None:
    rows = read(SEEDS[:5])
    missing = next(i for i, row in enumerate(rows) if row[0] == "mv")
    verdict = dt.judge(rows[:missing] + rows[missing + 1 :])
    assert [d["path"] for d in verdict.paths_differ] == ["mv"]
    assert not verdict.wrong_values


def test_wrong_groups_and_extremes_are_reported() -> None:
    rows = read(SEEDS[:20])
    g = next(i for i, row in enumerate(rows) if row[0] == "grouped")
    changed = list(rows[g])
    changed[4] = str(int(changed[4]) + 1)
    assert dt.judge(rows[:g] + [tuple(changed)] + rows[g + 1 :]).wrong_groups
    e = next(i for i, row in enumerate(rows) if row[0] == "extremes")
    assert dt.judge(rows[:e] + rows[e + 1 :]).wrong_extremes


def test_grouping_negative_zero_with_zero_or_apart_are_both_accepted() -> None:
    i = column("c_float8")

    def seed_with(text: str) -> int:
        return next(
            s
            for s in range(1, 100_000)
            if (v := dt.row_values(s)[i]) and v.text == text
        )

    seeds = [seed_with("-0"), seed_with("0")]
    apart, together = dt.expected_groups(seeds), dt.expected_groups(seeds, True)
    assert apart != together
    for grouped in (apart, together):
        rows = [row for row in read(seeds) if row[0] != "grouped"]
        padding = [None] * (len(dt.COLUMNS) - 2)
        rows += [
            ("grouped", col, lo, str(hi), str(n), *padding)
            for col, lo, hi, n in grouped
        ]
        assert not dt.judge(rows).wrong_groups
    # The least value is -0 or 0, whichever row has the lesser seed.
    assert (i, 0, min(seeds)) in dt.expected_extremes(seeds)


def test_nan_is_the_greatest_value_and_null_is_none() -> None:
    i = column("c_float4")

    def seed_with(text: str | None) -> int:
        return next(
            s
            for s in range(1, 100_000)
            if (None if (v := dt.row_values(s)[i]) is None else v.text) == text
        )

    nan, infinity, null = seed_with("NaN"), seed_with("Infinity"), seed_with(None)
    extremes = dt.expected_extremes([nan, infinity, null])
    assert (i, 1, nan) in extremes and (i, 0, infinity) in extremes
    assert dt.expected_extremes([null]).get((i, 0, null), 0) == 0


def test_setup_statements_name_every_column() -> None:
    statements = dt.setup_statements()
    assert all(c.name in statements[1] for c in dt.COLUMNS)
    assert statements[-2].count("GROUP BY") == len(dt.COLUMNS)
    assert statements[-1].count("LIMIT 1") == 2 * len(dt.ORDERED)
    assert dt.read_query().count("UNION ALL") == len(dt.READ_PATHS) + 1

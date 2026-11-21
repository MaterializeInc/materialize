# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import threading
from typing import Any

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis_driver import copies
from materialize.antithesis_driver.datatypes import row_values

SEEDS = (7919, 15838, 23757)


def stored(batch: int, seeds: tuple[int, ...]) -> list[tuple[Any, ...]]:
    """The rows a check reads of a batch that is visible whole."""
    return [
        (batch, len(seeds), n, seed)
        + tuple(None if v is None else v.text for v in row_values(seed))
        for n, seed in enumerate(seeds)
    ]


def test_text_lines_escape_what_copy_reads_as_structure() -> None:
    assert copies.text_line(["a\tb", None, "c\\d\ne\r", ""]) == (
        "a\\tb\t\\N\tc\\\\d\\ne\\r\t\n"
    )


def test_csv_tells_null_from_the_empty_string_in_both_directions() -> None:
    fields = ["plain", None, "", 'a,b "q"', "line1\nline2\r\n", " spaced "]
    line = copies.csv_line(fields)
    assert line == 'plain,,"","a,b ""q""","line1\nline2\r\n", spaced \n'
    assert copies.parse_csv(line + line) == [fields, fields]


def test_every_generated_row_survives_the_csv_encoding() -> None:
    for seed in range(1, 300):
        fields = copies.input_fields(1, 1, 0, seed * 7919)
        assert copies.parse_csv(copies.csv_line(fields)) == [fields]


def test_chunks_cut_inside_rows_and_join_to_the_data() -> None:
    data = "é\tx\n1\t2\n".encode()
    pieces = copies.chunks(data, [1, 4, 4, 0, len(data), 99])
    assert pieces == [data[:1], data[1:4], data[4:]]
    assert copies.chunks(data, []) == [data]
    assert copies.chunks(b"", [0]) == [b""]


def test_a_whole_batch_and_an_absent_one_are_clean() -> None:
    plans = {
        1: copies.Plan(SEEDS, "visible"),
        2: copies.Plan(SEEDS, "absent"),
        3: copies.Plan(SEEDS, "maybe"),
        4: copies.Plan(SEEDS[:1], "maybe"),
    }
    errors = copies.judge(stored(1, SEEDS) + stored(4, SEEDS[:1]), plans)
    assert not any(errors.values())


@pytest.mark.parametrize("state", ["visible", "maybe"])
def test_a_batch_missing_a_row_or_holding_one_twice_is_torn(state: str) -> None:
    plans = {1: copies.Plan(SEEDS, state)}
    rows = stored(1, SEEDS)
    assert [e["batch"] for e in copies.judge(rows[1:], plans)["torn"]] == [1]
    assert [e["batch"] for e in copies.judge(rows + rows, plans)["torn"]] == [1]
    assert [e["batch"] for e in copies.judge(rows + rows[:1], plans)["torn"]] == [1]
    # A row of the batch with another row's seed.
    swapped = [rows[0][:3] + rows[1][3:], *rows[1:]]
    assert copies.judge(swapped, plans)["torn"]


def test_rows_of_a_rejected_or_unknown_batch_are_reported() -> None:
    plans = {1: copies.Plan(SEEDS, "absent")}
    errors = copies.judge(stored(1, SEEDS) + stored(9, SEEDS), plans)
    assert sorted(e["batch"] for e in errors["unsent"]) == [1, 9]
    assert not errors["torn"] and not errors["missing"]


def test_an_acknowledged_batch_that_is_not_visible_is_missing() -> None:
    plans = {1: copies.Plan(SEEDS, "visible"), 2: copies.Plan(SEEDS, "maybe")}
    assert copies.judge([], plans)["missing"] == [1]


def test_a_copied_value_that_differs_from_its_seed_is_reported() -> None:
    rows = stored(1, SEEDS[:1])
    index = next(i for i, text in enumerate(rows[0]) if i >= 4 and text is not None)
    rows[0] = rows[0][:index] + ("wrong",) + rows[0][index + 1 :]
    errors = copies.judge(rows, {1: copies.Plan(SEEDS[:1], "visible")})
    assert [e["observed"] for e in errors["values"]] == ["wrong"]
    assert not errors["torn"]


def test_copy_to_rows_are_compared_in_their_wire_form() -> None:
    seed = next(s for s in range(1, 10_000) if row_values(s)[0] is not None)
    row = ["1", "1", "0", str(seed), *copies.wire_fields(seed)]
    # The wire renders a boolean as one letter, unlike `::text`.
    assert row[4] in ("t", "f")
    assert copies.wrong_rows([row]) == []
    row[4] = "true"
    assert copies.wrong_rows([row]) == [{"batch": "1", "n": "0", "seed": str(seed)}]


def test_what_a_read_may_see_follows_from_when_it_began() -> None:
    c = copies.Copies.__new__(copies.Copies)
    c._lock = threading.Lock()
    c.batches = {
        1: copies.Batch(SEEDS, "text", "done", "ok", acked_at=10.0),
        2: copies.Batch(SEEDS, "text", "done", "ok", acked_at=30.0),
        3: copies.Batch(SEEDS, "csv", "fail", "rejected"),
        4: copies.Batch(SEEDS, "csv", "done", "indeterminate"),
        5: copies.Batch(SEEDS, "csv", "done", "ok", acked_at=5.0, retired=True),
        6: copies.Batch(
            SEEDS, "csv", "done", "ok", acked_at=5.0, retired=True, pruned_at=15.0
        ),
        7: copies.Batch(SEEDS, "text", "done"),
    }
    assert {n: p.state for n, p in c._plans(20.0).items()} == {
        1: "visible",
        2: "maybe",
        3: "absent",
        4: "maybe",
        5: "maybe",
        7: "maybe",
    }
    # A read that began before the deletion was acknowledged may see batch 6.
    assert c._plans(12.0)[6].state == "maybe"

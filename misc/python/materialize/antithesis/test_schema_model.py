# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

from collections import Counter
from decimal import Decimal

from materialize.antithesis.schema_model import (
    DEFAULT_VALUE,
    Insert,
    added_column,
    check_table,
    expected_row,
    is_model_column,
    latest_version,
    project,
    sql_literal,
    version_columns,
    written_value,
)

C1 = added_column(1, "tx")
C2 = added_column(2, "nm")
COLUMNS = ["k", "w", C1, C2]

CREATE_SQL = (
    "CREATE TABLE materialize.schema_evo.t0_g1 (k pg_catalog.int8 NOT NULL,"
    " w pg_catalog.text DEFAULT 'w-default', c1_tx pg_catalog.text VERSION ADDED 1,"
    " c2_nm pg_catalog.numeric VERSION ADDED 2)"
)


def ins(
    keys: tuple[int, ...],
    written: tuple[str, ...],
    outcome: str = "ok",
    invoke: float = 1.0,
    complete: float | None = 2.0,
) -> Insert:
    return Insert(keys, written, outcome, invoke, complete)


STORED_CREATE_SQL = (
    'CREATE TABLE "materialize"."schema_evo"."t1" ("k" [s21 AS "pg_catalog"."int8"]'
    ' NOT NULL, "w" [s46 AS "pg_catalog"."text"] DEFAULT \'w-default\','
    ' "c2_nm" [s1700 AS "pg_catalog"."numeric"] VERSION ADDED 2,'
    ' "c1_tx" [s46 AS "pg_catalog"."text"] VERSION ADDED 1)'
)


def test_versions_from_stored_create_sql() -> None:
    assert version_columns(STORED_CREATE_SQL, 0) == ["k", "w"]
    assert version_columns(STORED_CREATE_SQL, 1) == ["k", "w", C1]
    assert version_columns(STORED_CREATE_SQL, 2) == COLUMNS
    assert latest_version(STORED_CREATE_SQL) == 2


def test_values_follow_the_column_name() -> None:
    assert written_value(7, "k") == "7"
    assert written_value(7, "w") == "w7"
    assert written_value(7, C1) == "7:1"
    assert written_value(7, C2) == "7002"
    assert sql_literal(7, C1) == "'7:1'"
    assert sql_literal(7, "w") == "'w7'"
    assert sql_literal(7, C2) == "7002"
    assert is_model_column(C2) and not is_model_column("c2_zz")


def test_omitted_columns_read_as_default_or_null() -> None:
    assert expected_row(3, ("k",), COLUMNS) == ("3", DEFAULT_VALUE, None, None)
    assert expected_row(3, ("k", "w", C2), COLUMNS) == ("3", "w3", None, "3002")


def test_versions_from_create_sql() -> None:
    assert version_columns(CREATE_SQL, 0) == ["k", "w"]
    assert version_columns(CREATE_SQL, 1) == ["k", "w", C1]
    assert version_columns(CREATE_SQL, 2) == COLUMNS
    assert latest_version(CREATE_SQL) == 2
    assert latest_version("CREATE TABLE t (k pg_catalog.int8 NOT NULL)") == 0


def test_rows_written_before_a_column_existed_pass() -> None:
    rows = [
        (1, "w1", None, None),
        (2, DEFAULT_VALUE, None, None),
        (3, "w3", "3:1", Decimal("3002")),
    ]
    ledger = [
        ins((1,), ("k", "w")),
        ins((2,), ("k",)),
        ins((3,), ("k", "w", C1, C2)),
    ]
    v = check_table(COLUMNS, rows, ledger, read_start=3.0, read_end=4.0)
    assert v.complete and v.sound and v.values_ok, v
    assert v.rows == 3 and v.with_added_column == 1


def test_acked_row_missing_is_reported() -> None:
    v = check_table(COLUMNS, [], [ins((1,), ("k", "w"))], 3.0, 4.0)
    assert v.missing == [1]


def test_row_acked_after_read_began_may_be_absent() -> None:
    v = check_table(COLUMNS, [], [ins((1,), ("k",), complete=3.5)], 3.0, 4.0)
    assert v.complete


def test_indeterminate_rows_may_go_either_way() -> None:
    ledger = [
        ins((1,), ("k",), outcome="indeterminate"),
        ins((2,), ("k",), outcome="pending", complete=None),
    ]
    present = check_table(
        COLUMNS, [(1, DEFAULT_VALUE, None, None)], ledger, read_start=3.0, read_end=4.0
    )
    assert present.complete and present.sound and present.values_ok
    absent = check_table(COLUMNS, [], ledger, read_start=3.0, read_end=4.0)
    assert absent.complete


def test_rejected_or_later_rows_are_unexpected() -> None:
    ledger = [
        ins((1,), ("k",), outcome="rejected"),
        ins((2,), ("k",), invoke=5.0, complete=6.0),
    ]
    rows = [
        (1, DEFAULT_VALUE, None, None),
        (2, DEFAULT_VALUE, None, None),
        (9, "w9", None, None),
    ]
    v = check_table(COLUMNS, rows, ledger, read_start=3.0, read_end=4.0)
    assert v.unexpected == [1, 2, 9]


def test_duplicates_are_reported() -> None:
    row = (1, "w1", None, None)
    v = check_table(COLUMNS, [row, row], [ins((1,), ("k", "w"))], 3.0, 4.0)
    assert v.duplicated == [1] and not v.sound


def test_new_column_filled_for_a_row_that_never_named_it_is_wrong() -> None:
    v = check_table(
        COLUMNS, [(1, "w1", "1:1", None)], [ins((1,), ("k", "w"))], 3.0, 4.0
    )
    assert [w["k"] for w in v.wrong] == [1]


def test_default_dropped_for_an_omitted_base_column_is_wrong() -> None:
    v = check_table(COLUMNS, [(1, None, None, None)], [ins((1,), ("k",))], 3.0, 4.0)
    assert not v.values_ok


def test_unmodeled_column_is_reported() -> None:
    v = check_table(["k", "w", "x"], [(1, "w1", None)], [ins((1,), ("k", "w"))], 3, 4)
    assert v.unmodeled_columns == ["x"]


def test_project_with_and_without_filter() -> None:
    rows = [(1, "w1", None, None), (2, "w2", "2:1", 5)]
    assert project(COLUMNS, rows, ["k", C1]) == Counter(
        {("1", None): 1, ("2", "2:1"): 1}
    )
    assert project(COLUMNS, rows, ["w"], not_null=C1) == Counter({("w2",): 1})

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.query.query_result import (
    QueryResult,
)


def print_query_outcome(outcome: QueryResult) -> None:
    print("+++ Query outcome")
    print(f"-- strategy={outcome.strategy}")
    print(f"-- sql={outcome.sql}")
    print(
        f"-- row_count={outcome.row_count()}, column_count={outcome.query_column_count}"
    )

    column_lengths = _determine_column_lengths(outcome, min_length=1, max_length=100)

    for row_index in range(0, outcome.row_count()):
        row_values = []

        for col_index in range(0, outcome.query_column_count):
            value = outcome.result_rows[row_index][col_index]
            str_value = str(value) if value is not None else "NULL"
            str_value = str_value.ljust(column_lengths[col_index])
            row_values.append(str_value)

        print(" | ".join(row_values))


def _determine_column_lengths(
    outcome: QueryResult, min_length: int, max_length: int
) -> list[int]:
    column_lengths = [min_length for _ in range(0, outcome.query_column_count)]

    for row_index in range(0, outcome.row_count()):
        for col_index in range(0, outcome.query_column_count):
            value = outcome.result_rows[row_index][col_index]
            value_length = len(str(value))
            column_lengths[col_index] = min(
                max(column_lengths[col_index], value_length), max_length
            )

    return column_lengths

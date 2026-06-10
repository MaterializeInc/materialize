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

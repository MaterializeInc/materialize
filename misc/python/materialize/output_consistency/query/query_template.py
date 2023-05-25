# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import List

from materialize.output_consistency.data_value.data_row_selection import (
    DataRowSelection,
)
from materialize.output_consistency.execution.evaluation_strategy import (
    EvaluationStrategy,
)
from materialize.output_consistency.execution.value_storage_layout import (
    VERTICAL_LAYOUT_ROW_INDEX_COL_NAME,
    ValueStorageLayout,
)
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.query.query_format import QueryOutputFormat


class QueryTemplate:
    """Query template as base for creating SQL for different evaluation strategies"""

    def __init__(
        self,
        expect_error: bool,
        select_expressions: List[Expression],
        storage_layout: ValueStorageLayout,
        contains_aggregations: bool,
        row_selection: DataRowSelection,
    ) -> None:
        self.expect_error = expect_error
        self.select_expressions: List[Expression] = select_expressions
        self.storage_layout = storage_layout
        self.contains_aggregations = contains_aggregations
        self.row_selection = row_selection

    def add_select_expression(self, expression: Expression) -> None:
        self.select_expressions.append(expression)

    def add_multiple_select_expressions(self, expressions: List[Expression]) -> None:
        self.select_expressions.extend(expressions)

    def to_sql(
        self, strategy: EvaluationStrategy, output_format: QueryOutputFormat
    ) -> str:
        space_separator = self._get_space_separator(output_format)

        column_sql = self._create_column_sql(space_separator)
        where_clause = self._create_where_clause()
        order_by_clause = self._create_order_by_clause()

        sql = f"""
SELECT{space_separator}{column_sql}
FROM{space_separator}{strategy.get_db_object_name(self.storage_layout)}
{where_clause}
{order_by_clause};""".strip()

        return self._post_format_sql(sql, output_format)

    def _get_space_separator(self, output_format: QueryOutputFormat) -> str:
        return "\n  " if output_format == QueryOutputFormat.MULTI_LINE else " "

    def _create_column_sql(self, space_separator: str) -> str:
        expressions_as_sql = [expr.to_sql() for expr in self.select_expressions]
        return f",{space_separator}".join(expressions_as_sql)

    def _create_where_clause(self) -> str:
        if self.row_selection.row_indices is None:
            return ""

        row_index_string = ", ".join(
            str(index) for index in self.row_selection.row_indices
        )
        return f"WHERE {VERTICAL_LAYOUT_ROW_INDEX_COL_NAME} IN ({row_index_string})"

    def _create_order_by_clause(self) -> str:
        if (
            self.storage_layout == ValueStorageLayout.VERTICAL
            and not self.contains_aggregations
        ):
            return f"ORDER BY {VERTICAL_LAYOUT_ROW_INDEX_COL_NAME} ASC"

        return ""

    def _post_format_sql(self, sql: str, output_format: QueryOutputFormat) -> str:
        sql = sql.replace("\n\n", "\n")
        sql = sql.replace("\n;", ";")

        if output_format == QueryOutputFormat.SINGLE_LINE:
            sql = sql.replace("\n", " ")

        return sql

    def column_count(self) -> int:
        return len(self.select_expressions)

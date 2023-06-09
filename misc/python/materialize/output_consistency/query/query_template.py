# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import List, Optional

from materialize.output_consistency.execution.evaluation_strategy import (
    EvaluationStrategy,
)
from materialize.output_consistency.execution.value_storage_layout import (
    VERTICAL_LAYOUT_ROW_INDEX_COL_NAME,
    ValueStorageLayout,
)
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.query.query_format import QueryOutputFormat
from materialize.output_consistency.selection.selection import (
    DataRowSelection,
    QueryColumnByIndexSelection,
)


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
        self.disable_error_message_validation = not self.__can_compare_error_messages()

    def add_select_expression(self, expression: Expression) -> None:
        self.select_expressions.append(expression)

    def add_multiple_select_expressions(self, expressions: List[Expression]) -> None:
        self.select_expressions.extend(expressions)

    def to_sql(
        self,
        strategy: EvaluationStrategy,
        output_format: QueryOutputFormat,
        query_column_selection: QueryColumnByIndexSelection,
        override_db_object_name: Optional[str] = None,
    ) -> str:
        db_object_name = override_db_object_name or strategy.get_db_object_name(
            self.storage_layout
        )
        space_separator = self._get_space_separator(output_format)

        column_sql = self._create_column_sql(query_column_selection, space_separator)
        where_clause = self._create_where_clause()
        order_by_clause = self._create_order_by_clause()

        sql = f"""
SELECT{space_separator}{column_sql}
FROM{space_separator}{db_object_name}
{where_clause}
{order_by_clause};""".strip()

        return self._post_format_sql(sql, output_format)

    def _get_space_separator(self, output_format: QueryOutputFormat) -> str:
        return "\n  " if output_format == QueryOutputFormat.MULTI_LINE else " "

    def _create_column_sql(
        self, query_column_selection: QueryColumnByIndexSelection, space_separator: str
    ) -> str:
        expressions_as_sql = []
        for index, expression in enumerate(self.select_expressions):
            if query_column_selection.is_included(index):
                expressions_as_sql.append(expression.to_sql(True))

        return f",{space_separator}".join(expressions_as_sql)

    def _create_where_clause(self) -> str:
        if self.row_selection.keys is None:
            return ""

        if len(self.row_selection.keys) == 0:
            row_index_string = "-1"
        else:
            row_index_string = ", ".join(
                str(index) for index in self.row_selection.keys
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

    def __can_compare_error_messages(self) -> bool:
        if self.storage_layout == ValueStorageLayout.HORIZONTAL:
            return True

        for expression in self.select_expressions:
            if expression.contains_leaf_not_directly_consumed_by_aggregation():
                # The query operates on multiple rows and contains at least one non-aggregate function directly
                # operating on the value. Since the row processing order is not fixed, different evaluation
                # strategies may yield different error messages (depending on the first invalid value they
                # encounter). Therefore, error messages shall not be compared in case of a query failure.
                return False

        return True

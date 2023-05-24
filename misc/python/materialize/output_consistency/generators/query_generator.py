# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List, Optional, Set

from materialize.output_consistency.common.configuration import (
    ConsistencyTestConfiguration,
)
from materialize.output_consistency.execution.value_storage_layout import (
    ValueStorageLayout,
)
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.input_data.test_input_data import (
    ConsistencyTestInputData,
)
from materialize.output_consistency.query.query_template import QueryTemplate
from materialize.output_consistency.selection.randomized_picker import RandomizedPicker


class QueryGenerator:
    """Generates query templates based on expressions"""

    def __init__(
        self,
        config: ConsistencyTestConfiguration,
        randomized_picker: RandomizedPicker,
        input_data: ConsistencyTestInputData,
    ):
        self.config = config
        self.randomized_picker = randomized_picker
        self.vertical_storage_row_count = input_data.max_value_count

        self.count_pending_expressions = 0
        # ONE query PER expression using the storage layout specified in the expression, expressions presumably fail
        self.any_layout_presumably_failing_expressions: List[Expression] = []
        # ONE query FOR ALL expressions accessing the horizontal storage layout; expressions presumably succeed and do
        # not contain aggregations
        self.horizontal_layout_normal_expressions: List[Expression] = []
        # ONE query FOR ALL expressions accessing the horizontal storage layout and applying aggregations; expressions
        # presumably succeed
        self.horizontal_layout_aggregate_expressions: List[Expression] = []
        # ONE query FOR ALL expressions accessing the vertical storage layout; expressions presumably succeed and do not
        # contain aggregations
        self.vertical_layout_normal_expressions: List[Expression] = []
        # ONE query FOR ALL expressions accessing the vertical storage layout and applying aggregations; expressions
        # presumably succeed
        self.vertical_layout_aggregate_expressions: List[Expression] = []

    def push_expression(self, expression: Expression) -> None:
        if expression.is_expect_error:
            self.any_layout_presumably_failing_expressions.append(expression)
            return

        if expression.storage_layout == ValueStorageLayout.HORIZONTAL:
            if expression.is_aggregate:
                self.horizontal_layout_aggregate_expressions.append(expression)
            else:
                self.horizontal_layout_normal_expressions.append(expression)
        elif expression.storage_layout == ValueStorageLayout.VERTICAL:
            if expression.is_aggregate:
                self.vertical_layout_aggregate_expressions.append(expression)
            else:
                self.vertical_layout_normal_expressions.append(expression)
        else:
            raise RuntimeError(f"Unknown storage layout: {expression.storage_layout}")

        self.count_pending_expressions += 1

    def shall_consume_queries(self) -> bool:
        return self.count_pending_expressions > self.config.max_pending_expressions

    def consume_queries(self) -> List[QueryTemplate]:
        queries = []
        queries.extend(
            self._create_multi_column_queries(
                self.horizontal_layout_normal_expressions,
                False,
                ValueStorageLayout.HORIZONTAL,
                False,
            )
        )
        queries.extend(
            self._create_multi_column_queries(
                self.horizontal_layout_aggregate_expressions,
                False,
                ValueStorageLayout.HORIZONTAL,
                True,
            )
        )
        queries.extend(
            self._create_multi_column_queries(
                self.vertical_layout_normal_expressions,
                False,
                ValueStorageLayout.VERTICAL,
                False,
            )
        )
        queries.extend(
            self._create_multi_column_queries(
                self.vertical_layout_aggregate_expressions,
                False,
                ValueStorageLayout.VERTICAL,
                True,
            )
        )
        queries.extend(
            self._create_single_column_queries(
                self.any_layout_presumably_failing_expressions
            )
        )

        self.reset_state()

        return queries

    def _create_multi_column_queries(
        self,
        expressions: List[Expression],
        expect_error: bool,
        storage_layout: ValueStorageLayout,
        contains_aggregations: bool,
    ) -> List[QueryTemplate]:
        """Creates queries not exceeding the maximum column count"""
        if len(expressions) == 0:
            return []

        queries = []
        for offset_index in range(0, len(expressions), self.config.max_cols_per_query):
            expression_chunk = expressions[
                offset_index : offset_index + self.config.max_cols_per_query
            ]

            restriction_to_row_indices = self._select_row_indices_selection(
                storage_layout
            )

            query = QueryTemplate(
                expect_error,
                expression_chunk,
                storage_layout,
                contains_aggregations,
                restriction_to_row_indices,
            )

            queries.append(query)

        return queries

    def _create_single_column_queries(
        self, expressions: List[Expression]
    ) -> List[QueryTemplate]:
        """Creates one query per expression"""

        queries = []
        for expression in expressions:
            restriction_to_row_indices = self._select_row_indices_selection(
                expression.storage_layout
            )

            queries.append(
                QueryTemplate(
                    expression.is_expect_error,
                    [expression],
                    expression.storage_layout,
                    False,
                    restriction_to_row_indices,
                )
            )

        return queries

    def _select_row_indices_selection(
        self, storage_layout: ValueStorageLayout
    ) -> Optional[Set[int]]:
        if storage_layout == ValueStorageLayout.HORIZONTAL:
            return None
        elif storage_layout == ValueStorageLayout.VERTICAL:
            max_number_of_rows_to_select = self.randomized_picker.random_number(2, 3)
            return self.randomized_picker.random_row_indices(
                self.vertical_storage_row_count, max_number_of_rows_to_select
            )
        else:
            raise RuntimeError(f"Unknown storage layout: {storage_layout}")

    def reset_state(self) -> None:
        self.count_pending_expressions = 0
        self.any_layout_presumably_failing_expressions = []
        self.horizontal_layout_normal_expressions = []
        self.horizontal_layout_aggregate_expressions = []
        self.vertical_layout_normal_expressions = []
        self.vertical_layout_aggregate_expressions = []

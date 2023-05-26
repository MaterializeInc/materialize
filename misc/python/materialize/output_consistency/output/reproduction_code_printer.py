# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List, Set

from materialize.output_consistency.execution.evaluation_strategy import (
    EvaluationStrategy,
)
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.expression.leaf_expression import LeafExpression
from materialize.output_consistency.input_data.test_input_data import (
    ConsistencyTestInputData,
)
from materialize.output_consistency.output.base_output_printer import BaseOutputPrinter
from materialize.output_consistency.query.query_format import QueryOutputFormat
from materialize.output_consistency.query.query_template import QueryTemplate
from materialize.output_consistency.selection.selection import (
    ALL_QUERY_COLUMNS_BY_INDEX_SELECTION,
    QueryColumnByIndexSelection,
    TableColumnByNameSelection,
)
from materialize.output_consistency.validation.validation_message import ValidationError


class ReproductionCodePrinter(BaseOutputPrinter):
    def __init__(self, input_data: ConsistencyTestInputData):
        self.input_data = input_data

    def print_reproduction_code(self, errors: List[ValidationError]) -> None:
        for error in errors:
            self.__print_reproduction_code_of_error(error)

    def __print_reproduction_code_of_error(self, error: ValidationError) -> None:
        query_template = error.query_template

        if error.col_index is None:
            query_column_selection = ALL_QUERY_COLUMNS_BY_INDEX_SELECTION
        else:
            query_column_selection = QueryColumnByIndexSelection({error.col_index})

        table_column_selection = TableColumnByNameSelection(
            self.__get_involved_column_names(query_template, query_column_selection)
        )

        self.print_major_separator()
        self._print_text("Minimal code for reproduction is:")
        self.print_minor_separator()
        self.__print_setup_code_for_error(
            query_template, error.strategy1, table_column_selection
        )
        self.print_minor_separator()
        self.__print_setup_code_for_error(
            query_template, error.strategy2, table_column_selection
        )
        self.print_minor_separator()
        self.__print_query_of_error(
            query_template, error.strategy1, query_column_selection
        )
        self.print_minor_separator()
        self.__print_query_of_error(
            query_template, error.strategy2, query_column_selection
        )
        self.print_minor_separator()
        characteristics = self.__get_involved_characteristics(
            query_template, query_column_selection
        )
        self._print_text(
            f"All directly or indirectly involved characteristics: {characteristics}"
        )

    def __print_setup_code_for_error(
        self,
        query_template: QueryTemplate,
        evaluation_strategy: EvaluationStrategy,
        table_column_selection: TableColumnByNameSelection,
    ) -> None:
        self._print_text(f"Setup for evaluation strategy '{evaluation_strategy.name}':")
        setup_code_lines = evaluation_strategy.generate_source_for_storage_layout(
            self.input_data,
            query_template.storage_layout,
            query_template.row_selection,
            table_column_selection,
        )

        for line in setup_code_lines:
            self._print_executable(line)

    def __print_query_of_error(
        self,
        query_template: QueryTemplate,
        evaluation_strategy: EvaluationStrategy,
        query_column_selection: QueryColumnByIndexSelection,
    ) -> None:
        self._print_text(
            f"Query using evaluation strategy '{evaluation_strategy.name}':"
        )
        self._print_executable(
            query_template.to_sql(
                evaluation_strategy,
                QueryOutputFormat.MULTI_LINE,
                query_column_selection,
            )
        )

    def __get_involved_column_names(
        self,
        query_template: QueryTemplate,
        query_column_selection: QueryColumnByIndexSelection,
    ) -> Set[str]:
        column_names = set()

        for index, expression in enumerate(query_template.select_expressions):
            if not query_column_selection.is_included(index):
                continue

            leave_expressions = expression.collect_leaves()
            for leaf_expression in leave_expressions:
                if isinstance(leaf_expression, LeafExpression):
                    # this is always the case
                    column_names.add(leaf_expression.column_name)

        return column_names

    def __get_involved_characteristics(
        self,
        query_template: QueryTemplate,
        query_column_selection: QueryColumnByIndexSelection,
    ) -> Set[ExpressionCharacteristics]:
        all_involved_characteristics: Set[ExpressionCharacteristics] = set()

        for index, expression in enumerate(query_template.select_expressions):
            if not query_column_selection.is_included(index):
                continue

            characteristics = expression.recursively_collect_involved_characteristics(
                query_template.row_selection
            )
            all_involved_characteristics = all_involved_characteristics.union(
                characteristics
            )

        return all_involved_characteristics

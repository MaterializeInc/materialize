# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.output_consistency.execution.evaluation_strategy import (
    EvaluationStrategy,
)
from materialize.output_consistency.input_data.test_input_data import (
    ConsistencyTestInputData,
)
from materialize.output_consistency.output.base_output_printer import (
    BaseOutputPrinter,
    OutputPrinterMode,
)
from materialize.output_consistency.query.query_format import QueryOutputFormat
from materialize.output_consistency.query.query_template import QueryTemplate
from materialize.output_consistency.selection.selection import (
    ALL_QUERY_COLUMNS_BY_INDEX_SELECTION,
    ALL_ROWS_SELECTION,
    QueryColumnByIndexSelection,
    TableColumnByNameSelection,
)
from materialize.output_consistency.validation.validation_message import ValidationError

MAX_ERRORS_WITH_REPRODUCTION_CODE = 5


class ReproductionCodePrinter(BaseOutputPrinter):
    def __init__(
        self,
        input_data: ConsistencyTestInputData,
        print_mode: OutputPrinterMode = OutputPrinterMode.PRINT,
    ):
        super().__init__(print_mode=print_mode)
        self.input_data = input_data

    def clone(self, print_mode: OutputPrinterMode):
        return ReproductionCodePrinter(self.input_data, print_mode)

    def get_reproduction_code_of_error(self, error: ValidationError) -> str:
        reproduction_code_generator = self.clone(OutputPrinterMode.COLLECT)
        reproduction_code_generator.print_reproduction_code_of_error(error)
        return "\n".join(reproduction_code_generator.collected_output)

    def print_reproduction_code(self, errors: list[ValidationError]) -> None:
        for i, error in enumerate(errors):
            if i == MAX_ERRORS_WITH_REPRODUCTION_CODE:
                break

            self.print_reproduction_code_of_error(error)

    def print_reproduction_code_of_error(self, error: ValidationError) -> None:
        query_template = error.query_execution.query_template

        if error.col_index is None:
            query_column_selection = ALL_QUERY_COLUMNS_BY_INDEX_SELECTION
        else:
            query_column_selection = QueryColumnByIndexSelection({error.col_index})

        # do not restrict the input to selected rows when a where clause is present;
        # there is no guarantee that the database filters the rows by the specified row indices before evaluating the
        # rest of the where condition such that the where condition evaluation may fail on rows outside the selection
        apply_row_filter = error.query_execution.query_template.where_expression is None

        table_column_selection = TableColumnByNameSelection(
            self.__get_involved_column_names(query_template, query_column_selection)
        )

        self.start_section("Minimal code for reproduction", collapsed=True)
        self.print_separator_line()

        # evaluation strategy 1
        if not query_template.custom_db_object_name:
            self.__print_setup_code_for_error(
                query_template,
                error.details1.strategy,
                table_column_selection,
                apply_row_filter,
            )
            self.print_separator_line()

        self.__print_query_of_error(
            query_template, error.details1.strategy, query_column_selection
        )
        self.print_separator_line()

        # evaluation strategy 2
        if not query_template.custom_db_object_name:
            self.__print_setup_code_for_error(
                query_template,
                error.details2.strategy,
                table_column_selection,
                apply_row_filter,
            )
            self.print_separator_line()

        self.__print_query_of_error(
            query_template, error.details2.strategy, query_column_selection
        )
        self.print_separator_line()

        characteristics = query_template.get_involved_characteristics(
            query_column_selection
        )
        characteristic_names = ", ".join([char.name for char in characteristics])
        self._print_text(
            f"All assumed directly or indirectly involved characteristics: {characteristic_names}"
        )

    def __print_setup_code_for_error(
        self,
        query_template: QueryTemplate,
        evaluation_strategy: EvaluationStrategy,
        table_column_selection: TableColumnByNameSelection,
        apply_row_filter: bool,
    ) -> None:
        self._print_text(f"Setup for evaluation strategy '{evaluation_strategy.name}':")
        row_selection = (
            query_template.row_selection if apply_row_filter else ALL_ROWS_SELECTION
        )
        setup_code_lines = evaluation_strategy.generate_source_for_storage_layout(
            self.input_data.types_input,
            query_template.storage_layout,
            row_selection,
            table_column_selection,
            override_db_object_name=(
                query_template.custom_db_object_name
                if query_template.custom_db_object_name is not None
                else evaluation_strategy.simple_db_object_name
            ),
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
                override_db_object_name=query_template.custom_db_object_name
                or evaluation_strategy.simple_db_object_name,
            )
        )

    def __get_involved_column_names(
        self,
        query_template: QueryTemplate,
        query_column_selection: QueryColumnByIndexSelection,
    ) -> set[str]:
        column_names = set()

        for index, expression in enumerate(query_template.select_expressions):
            if not query_column_selection.is_included(index):
                continue

            leave_expressions = expression.collect_leaves()
            for leaf_expression in leave_expressions:
                column_names.add(leaf_expression.column_name)

        if query_template.where_expression is not None:
            where_leaf_expressions = query_template.where_expression.collect_leaves()
            for leaf_expression in where_leaf_expressions:
                column_names.add(leaf_expression.column_name)

        return column_names

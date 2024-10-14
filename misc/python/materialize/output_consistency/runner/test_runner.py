# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.common.configuration import (
    ConsistencyTestConfiguration,
)
from materialize.output_consistency.execution.evaluation_strategy import (
    EvaluationStrategy,
)
from materialize.output_consistency.execution.query_execution_manager import (
    QueryExecutionManager,
)
from materialize.output_consistency.execution.sql_executors import SqlExecutors
from materialize.output_consistency.generators.expression_generator import (
    ExpressionGenerator,
)
from materialize.output_consistency.generators.query_generator import QueryGenerator
from materialize.output_consistency.ignore_filter.inconsistency_ignore_filter import (
    GenericInconsistencyIgnoreFilter,
)
from materialize.output_consistency.input_data.test_input_data import (
    ConsistencyTestInputData,
)
from materialize.output_consistency.output.output_printer import OutputPrinter
from materialize.output_consistency.runner.time_guard import TimeGuard
from materialize.output_consistency.selection.randomized_picker import RandomizedPicker
from materialize.output_consistency.status.test_summary import (
    ConsistencyTestSummary,
)
from materialize.output_consistency.validation.result_comparator import ResultComparator

ENABLE_ADDING_WHERE_CONDITIONS = True


class ConsistencyTestRunner:
    """Orchestrates the test execution"""

    def __init__(
        self,
        config: ConsistencyTestConfiguration,
        input_data: ConsistencyTestInputData,
        evaluation_strategies: list[EvaluationStrategy],
        expression_generator: ExpressionGenerator,
        query_generator: QueryGenerator,
        outcome_comparator: ResultComparator,
        sql_executors: SqlExecutors,
        randomized_picker: RandomizedPicker,
        ignore_filter: GenericInconsistencyIgnoreFilter,
        output_printer: OutputPrinter,
    ):
        self.config = config
        self.input_data = input_data
        self.evaluation_strategies = evaluation_strategies
        self.expression_generator = expression_generator
        self.query_generator = query_generator
        self.outcome_comparator = outcome_comparator
        self.execution_manager = QueryExecutionManager(
            evaluation_strategies,
            config,
            sql_executors,
            outcome_comparator,
            output_printer,
        )
        self.randomized_picker = randomized_picker
        self.ignore_filter = ignore_filter
        self.output_printer = output_printer

    def setup(self) -> None:
        self.input_data.assign_columns_to_tables(
            self.config.vertical_join_tables, self.randomized_picker
        )

        self.execution_manager.setup_database_objects(
            self.input_data, self.evaluation_strategies
        )

        # reset cache after having assigned columns to tables as a precaution
        self.input_data.types_input.cached_max_value_count_per_table_index.clear()

    def start(self) -> ConsistencyTestSummary:
        expression_count = 0
        test_summary = ConsistencyTestSummary(
            dry_run=self.config.dry_run,
            count_available_op_variants=self.input_data.count_available_op_variants(),
            count_available_data_types=self.input_data.count_available_data_types(),
            count_predefined_queries=self.input_data.count_predefined_queries(),
        )

        time_guard = TimeGuard(self.config.max_runtime_in_sec)

        if not self.config.disable_predefined_queries:
            self.output_printer.start_section("Running predefined queries")
            success = self._run_predefined_queries(test_summary)

            if not success and (
                test_summary.count_failures() >= self.config.max_failures_until_abort
            ):
                self.output_printer.print_info(
                    f"Ending test run because {test_summary.count_failures()} failures occurred"
                )
                return test_summary
        else:
            self.output_printer.print_info(
                "Not running predefined queries because they are disabled"
            )

        self.output_printer.print_empty_line()
        self.output_printer.start_section("Running generated queries")
        while True:
            if expression_count > 0 and expression_count % 200 == 0:
                self.output_printer.print_status(
                    f"Status: Expression {expression_count}..."
                    f" (last executed query #{self.execution_manager.query_counter})"
                )

            if expression_count % 5000 == 0:
                self.output_printer.print_status(
                    f"Random state verification: {self.expression_generator.randomized_picker.random_number(0, 10000)}"
                )

            operation = self.expression_generator.pick_random_operation(True)

            shall_abort_after_iteration = self._shall_abort(
                expression_count, time_guard
            )

            expression, number_of_args = (
                self.expression_generator.generate_expression_for_operation(operation)
            )
            test_summary.accept_expression_generation_statistics(
                operation, expression, number_of_args
            )

            if expression is None:
                continue

            self.query_generator.push_expression(expression)

            if (
                self.query_generator.shall_consume_queries()
                or shall_abort_after_iteration
            ):
                mismatch_occurred = self._consume_and_process_queries(
                    test_summary, time_guard
                )

                if mismatch_occurred:
                    shall_abort_after_iteration = True

                if time_guard.replied_abort_yes:
                    shall_abort_after_iteration = True

            expression_count += 1

            if shall_abort_after_iteration:
                break

        for strategy in self.evaluation_strategies:
            self.execution_manager.complete(strategy)

        test_summary.count_generated_select_expressions = expression_count
        return test_summary

    def _consume_and_process_queries(
        self, test_summary: ConsistencyTestSummary, time_guard: TimeGuard
    ) -> bool:
        """
        :return: if the test run should be aborted
        """
        queries = self.query_generator.consume_queries(test_summary)

        for query in queries:
            if ENABLE_ADDING_WHERE_CONDITIONS:
                self.query_generator.add_random_where_condition_to_query(
                    query, test_summary
                )
            success = self.execution_manager.execute_query(query, test_summary)

            if not success and (
                test_summary.count_failures() >= self.config.max_failures_until_abort
            ):
                self.output_printer.print_info(
                    f"Ending test run because {test_summary.count_failures()} failures occurred"
                )
                return True
            if time_guard.shall_abort():
                self.output_printer.print_info(
                    "Ending test run because the time elapsed"
                )
                return False

        return False

    def _shall_abort(self, iteration_count: int, time_guard: TimeGuard) -> bool:
        if (
            self.config.max_iterations != 0
            and iteration_count >= self.config.max_iterations
        ):
            self.output_printer.print_info(
                "Ending test run because the iteration count limit has been reached"
            )
            return True

        if time_guard.shall_abort():
            self.output_printer.print_info(
                "Ending test run because the maximum runtime has been reached"
            )
            return True

        return False

    def _run_predefined_queries(self, test_summary: ConsistencyTestSummary) -> bool:
        if len(self.input_data.predefined_queries) == 0:
            self.output_printer.print_status("No predefined queries exist")
            return True

        all_passed = True

        for predefined_query in self.input_data.predefined_queries:
            query_succeeded = self.execution_manager.execute_query(
                predefined_query, test_summary
            )
            all_passed = all_passed and query_succeeded

            if (
                not query_succeeded
                and test_summary.count_failures()
                >= self.config.max_failures_until_abort
            ):
                return False

        self.output_printer.print_status(
            f"Executed {len(self.input_data.predefined_queries)} predefined queries"
        )

        return all_passed

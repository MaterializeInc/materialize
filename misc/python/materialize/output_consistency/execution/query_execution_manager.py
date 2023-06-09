# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from datetime import datetime
from typing import List

from materialize.output_consistency.common.configuration import (
    ConsistencyTestConfiguration,
)
from materialize.output_consistency.execution.evaluation_strategy import (
    EvaluationStrategy,
)
from materialize.output_consistency.execution.sql_executor import (
    SqlExecutionError,
    SqlExecutor,
)
from materialize.output_consistency.execution.test_summary import ConsistencyTestSummary
from materialize.output_consistency.input_data.test_input_data import (
    ConsistencyTestInputData,
)
from materialize.output_consistency.output.output_printer import OutputPrinter
from materialize.output_consistency.query.query_format import QueryOutputFormat
from materialize.output_consistency.query.query_result import (
    QueryExecution,
    QueryFailure,
    QueryResult,
)
from materialize.output_consistency.query.query_template import QueryTemplate
from materialize.output_consistency.selection.selection import (
    ALL_QUERY_COLUMNS_BY_INDEX_SELECTION,
)
from materialize.output_consistency.validation.result_comparator import ResultComparator
from materialize.output_consistency.validation.validation_outcome import (
    ValidationOutcome,
)


class QueryExecutionManager:
    """Requests the execution of queries and handles transactions"""

    def __init__(
        self,
        evaluation_strategies: List[EvaluationStrategy],
        config: ConsistencyTestConfiguration,
        executor: SqlExecutor,
        comparator: ResultComparator,
        output_printer: OutputPrinter,
    ):
        self.evaluation_strategies = evaluation_strategies
        self.config = config
        self.executor = executor
        self.comparator = comparator
        self.output_printer = output_printer
        self.query_counter = 0

    def setup_database_objects(
        self,
        input_data: ConsistencyTestInputData,
        evaluation_strategies: List[EvaluationStrategy],
    ) -> None:
        self.output_printer.start_section("Setup code", collapsed=True)
        for strategy in evaluation_strategies:
            self.output_printer.print_info(
                f"Setup for evaluation strategy '{strategy.name}'"
            )
            ddl_statements = strategy.generate_sources(input_data)

            for sql_statement in ddl_statements:
                self.output_printer.print_sql(sql_statement)

                try:
                    self.executor.ddl(sql_statement)
                except SqlExecutionError as e:
                    self.output_printer.print_error(
                        f"Setting up data structures failed ({e.message})!"
                    )
                    raise e

    def execute_query(
        self, query: QueryTemplate, summary_to_update: ConsistencyTestSummary
    ) -> bool:
        if self.query_counter % self.config.queries_per_tx == 0:
            # commit after every couple of queries
            self.begin_tx(commit_previous_tx=self.query_counter > 0)

        query_index = self.query_counter
        self.query_counter += 1

        test_outcomes = self.fire_and_compare_query(
            query, query_index, "", self.evaluation_strategies
        )

        all_comparisons_passed = True

        for test_outcome in test_outcomes:
            summary_to_update.count_executed_query_templates += 1

            if test_outcome.success():
                summary_to_update.count_successful_query_templates += 1
            else:
                all_comparisons_passed = False

            if test_outcome.has_warnings():
                summary_to_update.count_with_warning_query_templates += 1

        return all_comparisons_passed

    def complete(self) -> None:
        self.commit_tx()

    def begin_tx(self, commit_previous_tx: bool) -> None:
        if commit_previous_tx:
            self.commit_tx()

        self.executor.begin_tx("SERIALIZABLE")

    def commit_tx(self) -> None:
        if not self.config.use_autocommit:
            self.executor.commit()

    def rollback_tx(self, start_new_tx: bool) -> None:
        # do this also when in autocommit mode
        self.executor.rollback()

        if start_new_tx:
            self.begin_tx(commit_previous_tx=False)

    # May return multiple outcomes if a query is split and retried. Will always return at least one outcome.
    def fire_and_compare_query(
        self,
        query_template: QueryTemplate,
        query_index: int,
        query_id_prefix: str,
        evaluation_strategies: List[EvaluationStrategy],
    ) -> List[ValidationOutcome]:
        query_no = query_index + 1
        query_id = f"{query_id_prefix}{query_no}"
        query_execution = QueryExecution(query_template, query_id)

        if self.config.verbose_output:
            # print the header with the query before the execution to have information if it gets stuck
            self.print_query_header(query_id, query_execution, collapsed=True)

        for strategy in evaluation_strategies:
            sql_query_string = query_template.to_sql(
                strategy,
                QueryOutputFormat.SINGLE_LINE,
                ALL_QUERY_COLUMNS_BY_INDEX_SELECTION,
            )

            start_time = datetime.now()

            try:
                data = self.executor.query(sql_query_string)
                duration = self._get_duration_in_ms(start_time)
                result = QueryResult(
                    strategy, sql_query_string, query_template.column_count(), data
                )
                query_execution.outcomes.append(result)
                query_execution.durations.append(duration)
            except SqlExecutionError as err:
                duration = self._get_duration_in_ms(start_time)
                self.rollback_tx(start_new_tx=True)

                if self.shall_retry_with_smaller_query(query_template):
                    # abort and retry with smaller query
                    # this will discard the outcomes of all strategies
                    return self.split_and_retry_queries(
                        query_template, query_id, evaluation_strategies
                    )

                failure = QueryFailure(
                    strategy, sql_query_string, query_template.column_count(), str(err)
                )
                query_execution.outcomes.append(failure)
                query_execution.durations.append(duration)

        if self.config.dry_run:
            return [ValidationOutcome()]

        validation_outcome = self.comparator.compare_results(query_execution)
        self.print_test_result(query_id, query_execution, validation_outcome)

        return [validation_outcome]

    def _get_duration_in_ms(self, start_time: datetime) -> float:
        end_time = datetime.now()
        duration = end_time - start_time
        return duration.total_seconds()

    def shall_retry_with_smaller_query(self, query_template: QueryTemplate) -> bool:
        return (
            self.config.split_and_retry_on_db_error
            and query_template.column_count() > 1
        )

    def split_and_retry_queries(
        self,
        original_query_template: QueryTemplate,
        query_id: str,
        evaluation_strategies: List[EvaluationStrategy],
    ) -> List[ValidationOutcome]:
        args_count = len(original_query_template.select_expressions)

        if args_count < 2:
            raise RuntimeError("Cannot split query")

        arg_split_index = int(args_count / 2)
        query1_args = original_query_template.select_expressions[arg_split_index:]
        query2_args = original_query_template.select_expressions[:arg_split_index]

        new_query_template1 = QueryTemplate(
            False,
            query1_args,
            original_query_template.storage_layout,
            original_query_template.contains_aggregations,
            original_query_template.row_selection,
        )
        new_query_template2 = QueryTemplate(
            False,
            query2_args,
            original_query_template.storage_layout,
            original_query_template.contains_aggregations,
            original_query_template.row_selection,
        )
        query_id_prefix = f"{query_id}."

        validation_outcomes = []
        validation_outcomes.extend(
            self.fire_and_compare_query(
                new_query_template1, 0, query_id_prefix, evaluation_strategies
            )
        )
        validation_outcomes.extend(
            self.fire_and_compare_query(
                new_query_template2, 1, query_id_prefix, evaluation_strategies
            )
        )

        return validation_outcomes

    def print_query_header(
        self,
        query_id: str,
        query_execution: QueryExecution,
        collapsed: bool,
        flush: bool = False,
    ) -> None:
        self.output_printer.start_section(
            f"Test query #{query_id}", collapsed=collapsed
        )
        self.output_printer.print_sql(query_execution.generic_sql)

        if flush:
            self.output_printer.flush()

    def print_test_result(
        self,
        query_id: str,
        query_execution: QueryExecution,
        validation_outcome: ValidationOutcome,
    ) -> None:
        if (
            validation_outcome.success()
            and not validation_outcome.has_warnings()
            and not self.config.verbose_output
        ):
            return

        if not self.config.verbose_output:
            # In verbose mode, the header has already been printed
            self.print_query_header(
                query_id, query_execution, collapsed=False, flush=True
            )

        result_desc = "PASSED" if validation_outcome.success() else "FAILED"
        success_reason = (
            f" ({validation_outcome.success_reason})"
            if validation_outcome.success_reason is not None
            and validation_outcome.success()
            else ""
        )

        self.output_printer.print_info(
            f"Test with query #{query_id} {result_desc}{success_reason}."
        )

        duration_info = ", ".join(
            "{:.3f}".format(duration) for duration in query_execution.durations
        )
        self.output_printer.print_info(f"Durations: {duration_info}")

        if validation_outcome.has_errors():
            self.output_printer.print_info(
                f"Errors:\n{validation_outcome.error_output()}"
            )

            if self.config.print_reproduction_code:
                self.output_printer.print_reproduction_code(validation_outcome.errors)

        if validation_outcome.has_warnings():
            self.output_printer.print_info(
                f"Warnings:\n{validation_outcome.warning_output()}"
            )

        if validation_outcome.has_remarks():
            self.output_printer.print_info(
                f"Remarks:\n{validation_outcome.remark_output()}"
            )

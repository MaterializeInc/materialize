# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from datetime import datetime

from materialize.output_consistency.common.configuration import (
    ConsistencyTestConfiguration,
)
from materialize.output_consistency.execution.evaluation_strategy import (
    EvaluationStrategy,
)
from materialize.output_consistency.execution.sql_executor import SqlExecutionError
from materialize.output_consistency.execution.sql_executors import SqlExecutors
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
from materialize.output_consistency.selection.column_selection import (
    ALL_QUERY_COLUMNS_BY_INDEX_SELECTION,
)
from materialize.output_consistency.status.test_summary import ConsistencyTestSummary
from materialize.output_consistency.validation.result_comparator import ResultComparator
from materialize.output_consistency.validation.validation_outcome import (
    ValidationOutcome,
    ValidationVerdict,
)


class QueryExecutionManager:
    """Requests the execution of queries and handles transactions"""

    def __init__(
        self,
        evaluation_strategies: list[EvaluationStrategy],
        config: ConsistencyTestConfiguration,
        executors: SqlExecutors,
        comparator: ResultComparator,
        output_printer: OutputPrinter,
    ):
        self.evaluation_strategies = evaluation_strategies
        self.config = config
        self.executors = executors
        self.comparator = comparator
        self.output_printer = output_printer
        self.query_counter = 0

    def setup_database_objects(
        self,
        input_data: ConsistencyTestInputData,
        evaluation_strategies: list[EvaluationStrategy],
    ) -> None:
        self.output_printer.start_section("Setup code", collapsed=True)
        for strategy in evaluation_strategies:
            self.output_printer.print_info(
                f"Setup for evaluation strategy '{strategy.name}'"
            )
            executor = self.executors.get_executor(strategy)
            ddl_statements = strategy.generate_sources(
                input_data.types_input, self.config.vertical_join_tables
            )

            for sql_statement in ddl_statements:
                self.output_printer.print_sql(sql_statement)

                try:
                    executor.ddl(sql_statement)
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
            for strategy in self.evaluation_strategies:
                self.begin_tx(strategy, commit_previous_tx=self.query_counter > 0)

        query_index = self.query_counter
        self.query_counter += 1

        test_outcomes = self.fire_and_compare_query(
            query, query_index, "", self.evaluation_strategies
        )

        all_comparisons_passed = True

        for test_outcome in test_outcomes:
            if test_outcome.verdict() == ValidationVerdict.FAILURE:
                all_comparisons_passed = False
            summary_to_update.accept_execution_result(
                query, test_outcome, self.output_printer.reproduction_code_printer
            )

        return all_comparisons_passed

    def complete(self, strategy: EvaluationStrategy) -> None:
        self.commit_tx(strategy)

    def begin_tx(self, strategy: EvaluationStrategy, commit_previous_tx: bool) -> None:
        if commit_previous_tx:
            self.commit_tx(strategy)

        self.executors.get_executor(strategy).before_new_tx()
        self.executors.get_executor(strategy).begin_tx("SERIALIZABLE")
        self.executors.get_executor(strategy).after_new_tx()

    def commit_tx(
        self,
        strategy: EvaluationStrategy,
    ) -> None:
        if not self.config.use_autocommit:
            self.executors.get_executor(strategy).commit()

    def rollback_tx(self, strategy: EvaluationStrategy, start_new_tx: bool) -> None:
        # do this also when in autocommit mode
        self.executors.get_executor(strategy).rollback()

        if start_new_tx:
            self.begin_tx(strategy, commit_previous_tx=False)

    # May return multiple outcomes if a query is split and retried. Will always return at least one outcome.
    def fire_and_compare_query(
        self,
        query_template: QueryTemplate,
        query_index: int,
        query_id_prefix: str,
        evaluation_strategies: list[EvaluationStrategy],
    ) -> list[ValidationOutcome]:
        query_no = query_index + 1
        query_id = f"{query_id_prefix}{query_no}"
        query_execution = QueryExecution(
            query_template, query_id, self.config.query_output_mode
        )

        if self.config.verbose_output:
            # print the header with the query before the execution to have information if it gets stuck
            self.print_query_header(query_id, query_execution, collapsed=True)

        for strategy in evaluation_strategies:
            sql_query_string = query_template.to_sql(
                strategy,
                QueryOutputFormat.SINGLE_LINE,
                ALL_QUERY_COLUMNS_BY_INDEX_SELECTION,
                self.config.query_output_mode,
            )

            start_time = datetime.now()

            try:
                self.executors.get_executor(strategy).before_query_execution()

                start_time = datetime.now()
                data = self.executors.get_executor(strategy).query(sql_query_string)
                duration = self._get_duration_in_ms(start_time)

                self.executors.get_executor(strategy).after_query_execution()

                result = QueryResult(
                    strategy, sql_query_string, query_template.column_count(), data
                )
                query_execution.outcomes.append(result)
                query_execution.durations.append(duration)
            except SqlExecutionError as err:
                duration = self._get_duration_in_ms(start_time)
                self.rollback_tx(strategy, start_new_tx=True)

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
        evaluation_strategies: list[EvaluationStrategy],
    ) -> list[ValidationOutcome]:
        args_count = len(original_query_template.select_expressions)

        if args_count < 2:
            raise RuntimeError("Cannot split query")

        # This code assumes that the query failed because of the SELECT expressions.
        # However, it is also possible that the where condition was invalid.
        # This is ignored as of now.
        arg_split_index = int(args_count / 2)
        query1_select_expressions = original_query_template.select_expressions[
            arg_split_index:
        ]
        query2_select_expressions = original_query_template.select_expressions[
            :arg_split_index
        ]

        new_query_template1 = original_query_template.clone(
            False,
            query1_select_expressions,
        )
        new_query_template2 = original_query_template.clone(
            False,
            query2_select_expressions,
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
        status: str | None = None,
        flush: bool = False,
    ) -> None:
        status = "" if status is None else f" ({status})"

        self.output_printer.start_section(
            f"Test query #{query_id}{status}", collapsed=collapsed
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
            validation_outcome.verdict() == ValidationVerdict.SUCCESS
            and not self.config.verbose_output
        ):
            return

        status = validation_outcome.verdict().name

        if not self.config.verbose_output:
            # In verbose mode, the header has already been printed
            self.print_query_header(
                query_id,
                query_execution,
                collapsed=validation_outcome.verdict().accepted(),
                status=status,
                flush=True,
            )

        result_desc = "PASSED" if validation_outcome.verdict().accepted() else "FAILED"
        success_reason = (
            f" ({validation_outcome.success_reason})"
            if validation_outcome.success_reason is not None
            and validation_outcome.verdict().succeeded()
            else ""
        )

        self.output_printer.print_info(
            f"Test with query #{query_id} {result_desc}{success_reason}."
        )

        duration_info = ", ".join(
            f"{duration:.3f}" for duration in query_execution.durations
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

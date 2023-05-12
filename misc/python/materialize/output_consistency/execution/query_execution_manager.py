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
from materialize.output_consistency.common.format_constants import (
    COMMENT_PREFIX,
    CONTENT_SEPARATOR_1,
)
from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.execution.evaluation_strategy import (
    EvaluationStrategy,
)
from materialize.output_consistency.execution.sql_executor import (
    SqlExecutionError,
    SqlExecutor,
)
from materialize.output_consistency.execution.test_summary import ConsistencyTestSummary
from materialize.output_consistency.query.query_format import QueryOutputFormat
from materialize.output_consistency.query.query_result import (
    QueryExecution,
    QueryFailure,
    QueryResult,
)
from materialize.output_consistency.query.query_template import QueryTemplate
from materialize.output_consistency.validation.result_comparator import ResultComparator
from materialize.output_consistency.validation.validation_outcome import (
    ValidationOutcome,
)


class QueryExecutionManager:
    def __init__(
        self,
        evaluation_strategies: list[EvaluationStrategy],
        config: ConsistencyTestConfiguration,
        executor: SqlExecutor,
        comparator: ResultComparator,
    ):
        self.evaluation_strategies = evaluation_strategies
        self.config = config
        self.executor = executor
        self.comparator = comparator

    def setup_database_objects(
        self,
        data_types: list[DataType],
        evaluation_strategies: list[EvaluationStrategy],
    ) -> None:
        if not self.config.execute_setup:
            return

        for strategy in evaluation_strategies:
            print(f"{COMMENT_PREFIX} Setup for evaluation strategy '{strategy.name}'")
            ddl_statements = strategy.generate_source(data_types)

            for sql_statement in ddl_statements:
                print(sql_statement)
                self.executor.ddl(sql_statement)

    def execute_queries(
        self,
        queries: list[QueryTemplate],
    ) -> ConsistencyTestSummary:
        if len(queries) == 0:
            print("No queries found!")
            return ConsistencyTestSummary(0, 0, self.config.dry_run)

        print(f"Processing {len(queries)} queries.")

        count_passed = 0

        for index, query in enumerate(queries):
            if index % self.config.queries_per_tx == 0:
                self.begin_tx(commit_previous_tx=index > 0)

            test_passed = self.fire_and_compare_queries(
                query, index, self.evaluation_strategies
            )

            if test_passed:
                count_passed += 1

        self.commit_tx()

        return ConsistencyTestSummary(
            count_executed_query_templates=len(queries),
            count_successful_query_templates=count_passed,
            dry_run=self.config.dry_run,
        )

    def begin_tx(self, commit_previous_tx: bool) -> None:
        if commit_previous_tx:
            self.commit_tx()

        self.executor.begin_tx("SERIALIZABLE")

    def commit_tx(self) -> None:
        self.executor.commit()

    def rollback_tx(self, start_new_tx: bool) -> None:
        self.executor.rollback()

        if start_new_tx:
            self.begin_tx(commit_previous_tx=False)

    def fire_and_compare_queries(
        self,
        query: QueryTemplate,
        query_index: int,
        evaluation_strategies: list[EvaluationStrategy],
    ) -> bool:
        query_execution = QueryExecution(query, query_index)

        query_no = query_execution.index + 1
        print(CONTENT_SEPARATOR_1)
        print(f"{COMMENT_PREFIX} Test query #{query_no}:")
        print(query_execution.generic_sql)

        for strategy in evaluation_strategies:
            sql_query_string = query.to_sql(strategy, QueryOutputFormat.SINGLE_LINE)

            try:
                data = self.executor.query(sql_query_string)
                result = QueryResult(
                    strategy, sql_query_string, query.column_count(), data
                )
                query_execution.outcomes.append(result)
            except SqlExecutionError as err:
                failure = QueryFailure(
                    strategy, sql_query_string, query.column_count(), str(err)
                )
                query_execution.outcomes.append(failure)
                self.rollback_tx(start_new_tx=True)

        if self.config.dry_run:
            return True

        validation_outcome = self.comparator.compare_results(query_execution)
        self.print_test_result(query_no, validation_outcome)

        return validation_outcome.success()

    def print_test_result(
        self, query_no: int, validation_outcome: ValidationOutcome
    ) -> None:
        result_desc = "PASSED" if validation_outcome.success() else "FAILED"

        print(f"{COMMENT_PREFIX} Test with query #{query_no} {result_desc}.")

        if validation_outcome.has_errors():
            print(f"Errors:\n{validation_outcome.error_output()}")

        if validation_outcome.has_warnings():
            print(f"Warnings:\n{validation_outcome.warning_output()}")

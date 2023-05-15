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
            return ConsistencyTestSummary(0, 0, 0, self.config.dry_run)

        print(f"Processing {len(queries)} queries.")

        # can be larger than the number of queries in case of retries with split queries
        count_executed = 0
        count_passed = 0
        count_with_warning = 0

        for index, query in enumerate(queries):
            if index % self.config.queries_per_tx == 0:
                self.begin_tx(commit_previous_tx=index > 0)

            test_outcomes = self.fire_and_compare_query(
                query, index, "", self.evaluation_strategies
            )

            for test_outcome in test_outcomes:
                count_executed += 1

                if test_outcome.success():
                    count_passed += 1

                if test_outcome.has_warnings():
                    count_with_warning += 1

        self.commit_tx()

        return ConsistencyTestSummary(
            count_executed_query_templates=count_executed,
            count_successful_query_templates=count_passed,
            count_with_warning_query_templates=count_with_warning,
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
        query_execution = QueryExecution(query_template, query_id)

        for strategy in evaluation_strategies:
            sql_query_string = query_template.to_sql(
                strategy, QueryOutputFormat.SINGLE_LINE
            )

            try:
                data = self.executor.query(sql_query_string)
                result = QueryResult(
                    strategy, sql_query_string, query_template.column_count(), data
                )
                query_execution.outcomes.append(result)
            except SqlExecutionError as err:
                self.rollback_tx(start_new_tx=True)

                if self.shall_retry_with_smaller_query(query_template):
                    # abort and retry with smaller query
                    return self.split_and_retry_queries(
                        query_template, query_id, evaluation_strategies
                    )

                failure = QueryFailure(
                    strategy, sql_query_string, query_template.column_count(), str(err)
                )
                query_execution.outcomes.append(failure)

        if self.config.dry_run:
            return [ValidationOutcome()]

        validation_outcome = self.comparator.compare_results(query_execution)
        self.print_test_result(query_id, query_execution, validation_outcome)

        return [validation_outcome]

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

        arg_split_index = int(args_count / 2)
        query1_args = original_query_template.select_expressions[arg_split_index:]
        query2_args = original_query_template.select_expressions[:arg_split_index]

        new_query_template1 = QueryTemplate(query1_args)
        new_query_template2 = QueryTemplate(query2_args)
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

        print(CONTENT_SEPARATOR_1)
        print(f"{COMMENT_PREFIX} Test query #{query_id}:")
        print(query_execution.generic_sql)

        result_desc = "PASSED" if validation_outcome.success() else "FAILED"
        success_reason = (
            f" ({validation_outcome.success_reason})"
            if validation_outcome.success_reason is not None
            and validation_outcome.success()
            else ""
        )

        print(
            f"{COMMENT_PREFIX} Test with query #{query_id} {result_desc}{success_reason}."
        )

        if validation_outcome.has_errors():
            print(f"Errors:\n{validation_outcome.error_output()}")

        if validation_outcome.has_warnings():
            print(f"Warnings:\n{validation_outcome.warning_output()}")

        if validation_outcome.has_remarks():
            print(f"Remarks:\n{validation_outcome.remark_output()}")

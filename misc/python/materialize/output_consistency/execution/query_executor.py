# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from pg8000 import Cursor
from pg8000.dbapi import ProgrammingError
from pg8000.exceptions import DatabaseError

from materialize.mzcompose import Composition
from materialize.output_consistency.configuration.output_consistency_configuration import (
    OutputConsistencyConfiguration,
)
from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.execution.evaluation_strategy import (
    EvaluationStrategy,
)
from materialize.output_consistency.execution.test_summary import ConsistencyTestSummary
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


class QueryExecutor:
    def __init__(
        self,
        evaluation_strategies: list[EvaluationStrategy],
        config: OutputConsistencyConfiguration,
        comparator: ResultComparator,
    ):
        self.evaluation_strategies = evaluation_strategies
        self.config = config
        self.comparator = comparator

    def setup_database_objects(
        self,
        c: Composition,
        data_types: list[DataType],
        evaluation_strategies: list[EvaluationStrategy],
    ) -> None:
        ddl_statements = []
        for strategy in evaluation_strategies:
            ddl_statements.extend(strategy.generate_source(data_types))

        for sql_statement in ddl_statements:
            c.sql(sql_statement)

    def execute_queries(
        self,
        c: Composition,
        queries: list[QueryTemplate],
    ) -> ConsistencyTestSummary:
        if len(queries) == 0:
            print("No queries found!")
            return ConsistencyTestSummary(0, 0)

        print(f"Processing {len(queries)} queries.")
        cursor: Cursor = c.sql_cursor()

        count_passed = 0

        for index, query in enumerate(queries):
            if index % self.config.queries_per_tx == 0:
                self.begin_tx(cursor, commit_previous_tx=index > 0)

            test_passed = self.fire_and_compare_queries(
                cursor, query, index, self.evaluation_strategies
            )

            if test_passed:
                count_passed += 1

        self.commit_tx(cursor)

        return ConsistencyTestSummary(
            count_executed_query_templates=len(queries),
            count_successful_query_templates=count_passed,
        )

    def begin_tx(self, cursor: Cursor, commit_previous_tx: bool) -> None:
        if commit_previous_tx:
            self.commit_tx(cursor)

        cursor.execute("BEGIN ISOLATION LEVEL SERIALIZABLE;")

    def commit_tx(self, cursor: Cursor) -> None:
        cursor.execute("COMMIT;")

    def rollback_tx(self, cursor: Cursor, start_new_tx: bool) -> None:
        cursor.execute("ROLLBACK;")

        if start_new_tx:
            self.begin_tx(cursor, commit_previous_tx=False)

    def fire_and_compare_queries(
        self,
        cursor: Cursor,
        query: QueryTemplate,
        query_index: int,
        evaluation_strategies: list[EvaluationStrategy],
    ) -> bool:
        query_execution = QueryExecution(query, query_index)

        query_no = query_execution.index + 1
        print(f"Test query {query_no}: {query_execution.generic_sql}")

        for strategy in evaluation_strategies:
            sql_query_string = query.to_sql(strategy)

            try:
                cursor.execute(sql_query_string)
                result = QueryResult(strategy, sql_query_string, cursor.fetchall())
                query_execution.outcomes.append(result)
            except (ProgrammingError, DatabaseError) as err:
                failure = QueryFailure(
                    strategy, sql_query_string, str(err), query.column_count()
                )
                query_execution.outcomes.append(failure)
                self.rollback_tx(cursor, start_new_tx=True)

        validation_outcome = self.comparator.compare_results(query_execution)
        self.print_test_result(query_no, validation_outcome)

        return validation_outcome.success()

    def print_test_result(
        self, query_no: int, validation_outcome: ValidationOutcome
    ) -> None:

        if validation_outcome.success():
            print(f"Test with query {query_no} PASSED.")
        else:
            print(f"Test with query {query_no} FAILED!")
            print(f"Errors:\n{validation_outcome.error_output()}")

        if validation_outcome.has_warnings():
            print(f"Warnings:\n{validation_outcome.warning_output()}")

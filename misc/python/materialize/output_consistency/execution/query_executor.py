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
from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.execution.execution_strategy import (
    EvaluationStrategy,
)
from materialize.output_consistency.execution.result_comparator import ResultComparator
from materialize.output_consistency.query.query_result import (
    QueryExecution,
    QueryFailure,
    QueryResult,
)
from materialize.output_consistency.query.query_template import QueryTemplate

QUERIES_PER_TX = 20


class QueryExecutor:
    def __init__(
        self,
        evaluation_strategies: list[EvaluationStrategy],
        comparator: ResultComparator,
    ):
        self.evaluation_strategies = evaluation_strategies
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
    ) -> None:
        if len(queries) == 0:
            print("No queries found!")
            return

        print(f"Processing {len(queries)} queries.")
        cursor: Cursor = c.sql_cursor()

        for index, query in enumerate(queries):
            if index % QUERIES_PER_TX == 0:
                self.begin_tx(cursor, commit_previous_tx=index > 0)

            self.fire_and_compare_queries(cursor, query, self.evaluation_strategies)

        self.commit_tx(cursor)

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
        evaluation_strategies: list[EvaluationStrategy],
    ) -> None:
        for strategy in evaluation_strategies:
            sql_query_string = query.to_sql(strategy)

            query_execution = QueryExecution(sql_query_string)

            try:
                cursor.execute(sql_query_string)
                result = QueryResult(strategy, cursor.fetchall())
                query_execution.outcomes.append(result)
            except DatabaseError as err:
                failure = QueryFailure(strategy, str(err))
                query_execution.outcomes.append(failure)
                self.rollback_tx(cursor, start_new_tx=True)
            except ProgrammingError as err:
                # TODO merge with previous
                failure = QueryFailure(strategy, str(err))
                query_execution.outcomes.append(failure)
                self.rollback_tx(cursor, start_new_tx=True)

            self.comparator.compare_results(query_execution)

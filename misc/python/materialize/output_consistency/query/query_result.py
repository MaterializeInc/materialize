# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import Any, Sequence

from materialize.output_consistency.execution.evaluation_strategy import (
    DummyEvaluation,
    EvaluationStrategy,
)
from materialize.output_consistency.query.query_format import QueryOutputFormat
from materialize.output_consistency.query.query_template import QueryTemplate


class QueryExecution:
    def __init__(self, query: QueryTemplate, index: int):
        self.generic_sql = query.to_sql(DummyEvaluation(), QueryOutputFormat.MULTI_LINE)
        self.index = index
        self.outcomes: list[QueryOutcome] = []

    def __str__(self) -> str:
        return f"QueryExecution with {len(self.outcomes)} outcomes for template query: {self.generic_sql})"


class QueryOutcome:
    def __init__(self, strategy: EvaluationStrategy, sql: str, successful: bool):
        self.strategy = strategy
        self.sql = sql
        self.successful = successful


class QueryResult(QueryOutcome):
    def __init__(
        self,
        strategy: EvaluationStrategy,
        sql: str,
        result_rows: Sequence[Sequence[Any]],
    ):
        super().__init__(strategy, sql, True)
        self.result_rows = result_rows

    def __str__(self) -> str:
        return f"Result with {len(self.result_rows)} rows with strategy '{self.strategy.name}'"


class QueryFailure(QueryOutcome):
    def __init__(
        self,
        strategy: EvaluationStrategy,
        sql: str,
        error_message: str,
        query_column_count: int,
    ):
        super().__init__(strategy, sql, False)
        self.error_message = error_message
        self.query_column_count = query_column_count

    def __str__(self) -> str:
        return f"Failure({self.error_message}) with strategy '{self.strategy.name}'"

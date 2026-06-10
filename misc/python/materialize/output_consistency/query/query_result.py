# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from materialize.output_consistency.execution.evaluation_strategy import (
    DummyEvaluation,
    EvaluationStrategy,
    EvaluationStrategyKey,
)
from materialize.output_consistency.execution.query_output_mode import QueryOutputMode
from materialize.output_consistency.query.query_format import QueryOutputFormat
from materialize.output_consistency.query.query_template import QueryTemplate
from materialize.output_consistency.selection.column_selection import (
    ALL_QUERY_COLUMNS_BY_INDEX_SELECTION,
)


class QueryExecution:
    """An executed query with the outcomes of the different evaluation strategies"""

    def __init__(
        self,
        query_template: QueryTemplate,
        query_id: str,
        query_output_mode: QueryOutputMode,
    ):
        self.generic_sql = query_template.to_sql(
            DummyEvaluation(),
            QueryOutputFormat.MULTI_LINE,
            ALL_QUERY_COLUMNS_BY_INDEX_SELECTION,
            query_output_mode,
        )
        self.query_id = query_id
        self.query_template = query_template
        self.query_output_mode = query_output_mode
        self.outcomes: list[QueryOutcome] = []
        self.durations: list[float] = []

    def get_outcome_by_strategy_key(self) -> dict[EvaluationStrategyKey, QueryOutcome]:
        return {outcome.strategy.identifier: outcome for outcome in self.outcomes}

    def get_first_failing_outcome(self) -> QueryFailure:
        for outcome in self.outcomes:
            if not outcome.successful:
                assert isinstance(outcome, QueryFailure)
                return outcome

        raise RuntimeError("No failing outcome found")

    def __str__(self) -> str:
        return f"QueryExecution with {len(self.outcomes)} outcomes for template query: {self.generic_sql})"


class QueryOutcome:
    """Outcome (result or failure) of a query execution with an evaluation strategy"""

    def __init__(
        self,
        strategy: EvaluationStrategy,
        sql: str,
        successful: bool,
        query_column_count: int,
    ):
        self.strategy = strategy
        self.sql = sql
        self.successful = successful
        self.query_column_count = query_column_count


class QueryResult(QueryOutcome):
    """Result of a successful query with data"""

    def __init__(
        self,
        strategy: EvaluationStrategy,
        sql: str,
        query_column_count: int,
        result_rows: Sequence[Sequence[Any]],
    ):
        super().__init__(strategy, sql, True, query_column_count)
        self.result_rows = result_rows

    def row_count(self) -> int:
        return len(self.result_rows)

    def __str__(self) -> str:
        return f"Result with {len(self.result_rows)} rows with strategy '{self.strategy.name}'"


class QueryFailure(QueryOutcome):
    """Error of a failed query with its error message"""

    def __init__(
        self,
        strategy: EvaluationStrategy,
        sql: str,
        query_column_count: int,
        error_message: str,
    ):
        super().__init__(strategy, sql, False, query_column_count)
        self.error_message = error_message

    def __str__(self) -> str:
        return f"Failure({self.error_message}) with strategy '{self.strategy.name}'"

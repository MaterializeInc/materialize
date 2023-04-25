# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import Any

from materialize.output_consistency.execution.execution_strategy import (
    EvaluationStrategy,
)


class QueryExecution:
    def __init__(self, query_sql: str):
        self.query_sql = query_sql
        self.outcomes: list[QueryOutcome] = []


class QueryOutcome:
    def __init__(self, strategy: EvaluationStrategy, successful: bool):
        self.strategy = strategy
        self.successful = successful


class QueryResult(QueryOutcome):
    def __init__(self, strategy: EvaluationStrategy, result_data: Any):
        super().__init__(strategy, True)
        self.result_data = result_data


class QueryFailure(QueryOutcome):
    def __init__(self, strategy: EvaluationStrategy, error_message: str):
        super().__init__(strategy, False)
        self.error_message = error_message

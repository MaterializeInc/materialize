# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import Optional

from materialize.output_consistency.execution.execution_strategy import (
    EvaluationStrategy,
)
from materialize.output_consistency.query.query_result import QueryExecution


class ComparisonMismatch:
    def __init__(self,
                 message: str,
                 description: Optional[str] = None,
                 value1: Optional[str] = None,
                 value2: Optional[str] = None,
                 strategy1: Optional[EvaluationStrategy] = None,
                 strategy2: Optional[EvaluationStrategy] = None,
                 col_index: Optional[int] = None,
                 ) -> None:
        self.message = message
        self.description = description
        self.value1 = value1
        self.value2 = value2
        self.strategy1 = strategy1
        self.strategy2 = strategy2
        self.col_index = col_index

        if value1 is None and value2 is not None:
            raise RuntimeError("value1 must be set if value2 is set")

        if strategy1 is None and strategy2 is not None:
            raise RuntimeError("strategy1 must be set if strategy2 is set")

    def __str__(self) -> str:
        error_desc = f" ({self.description})" if self.description else ""
        col_desc = f" at column index {self.col_index}" if self.col_index is not None else ""

        return f"Error: {self.message}{col_desc}{error_desc}."


class ComparisonOutcome:
    def __init__(self, query_execution: QueryExecution) -> None:
        self.errors: list[ComparisonMismatch] = []
        self.query_execution = query_execution.index

    def add_error(
            self,
            message: str,
            description: Optional[str] = None,
            value1: Optional[str] = None,
            value2: Optional[str] = None,
            strategy1: Optional[EvaluationStrategy] = None,
            strategy2: Optional[EvaluationStrategy] = None,
            col_index: Optional[int] = None,
    ) -> None:
        error = ComparisonMismatch(message, description, value1, value2, strategy1, strategy2, col_index)
        self.errors.append(error)

    def success(self) -> bool:
        return len(self.errors) == 0

    def error_details(self) -> str:
        return "\n".join([str(error) for error in self.errors])

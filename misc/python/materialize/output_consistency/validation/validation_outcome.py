# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import Optional

from materialize.output_consistency.execution.evaluation_strategy import (
    EvaluationStrategy,
)
from materialize.output_consistency.query.query_result import QueryExecution
from materialize.output_consistency.validation.problem_marker import ValidationError


class ValidationOutcome:
    def __init__(self, query_execution: QueryExecution) -> None:
        self.errors: list[ValidationError] = []
        self.query_execution = query_execution.index

    def add_error(
        self,
        message: str,
        description: Optional[str] = None,
        value1: Optional[str] = None,
        value2: Optional[str] = None,
        strategy1: Optional[EvaluationStrategy] = None,
        strategy2: Optional[EvaluationStrategy] = None,
        sql1: Optional[str] = None,
        sql2: Optional[str] = None,
        location: Optional[str] = None,
    ) -> None:
        error = ValidationError(
            message,
            description,
            value1,
            value2,
            strategy1,
            strategy2,
            sql1,
            sql2,
            location,
        )
        self.errors.append(error)

    def success(self) -> bool:
        return not self.has_errors()

    def has_errors(self) -> bool:
        return len(self.errors) > 0

    def error_details(self) -> str:
        return "\n=====\n".join([str(error) for error in self.errors])

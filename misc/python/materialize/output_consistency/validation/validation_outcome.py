# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import Optional

from materialize.output_consistency.common.format_constants import CONTENT_SEPARATOR_2
from materialize.output_consistency.execution.evaluation_strategy import (
    EvaluationStrategy,
)
from materialize.output_consistency.query.query_result import QueryExecution
from materialize.output_consistency.validation.problem_marker import (
    ValidationError,
    ValidationErrorType,
    ValidationWarning,
)


class ValidationOutcome:
    def __init__(self, query_execution: QueryExecution) -> None:
        self.errors: list[ValidationError] = []
        self.warnings: list[ValidationWarning] = []
        self.query_execution = query_execution.index

    def add_error(
        self,
        error_type: ValidationErrorType,
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
            error_type,
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

    def add_warning(
        self,
        message: str,
        description: Optional[str] = None,
        strategy: Optional[EvaluationStrategy] = None,
        sql: Optional[str] = None,
    ) -> None:
        warning = ValidationWarning(message, description, strategy, sql)
        self.warnings.append(warning)

    def success(self) -> bool:
        return not self.has_errors()

    def has_errors(self) -> bool:
        return len(self.errors) > 0

    def has_warnings(self) -> bool:
        return len(self.warnings) > 0

    def error_output(self) -> str:
        return f"\n{CONTENT_SEPARATOR_2}\n".join([str(error) for error in self.errors])

    def warning_output(self) -> str:
        return f"\n{CONTENT_SEPARATOR_2}\n".join(
            [str(warning) for warning in self.warnings]
        )

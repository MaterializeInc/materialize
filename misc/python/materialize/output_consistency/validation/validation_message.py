# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from enum import Enum
from typing import Any

from materialize.output_consistency.execution.evaluation_strategy import (
    EvaluationStrategy,
    EvaluationStrategyKey,
)
from materialize.output_consistency.query.query_result import QueryExecution


class ValidationErrorType(Enum):
    SUCCESS_MISMATCH = 1
    ROW_COUNT_MISMATCH = 2
    CONTENT_TYPE_MISMATCH = 3
    CONTENT_MISMATCH = 4
    ERROR_MISMATCH = 5


class ValidationMessage:
    """Either a `ValidationRemark`, `ValidationWarning`, or `ValidationError`"""

    def __init__(
        self,
        message: str,
        description: str | None = None,
    ):
        self.message = message
        self.description = description


class ValidationRemark(ValidationMessage):
    def __init__(
        self,
        message: str,
        description: str | None = None,
        sql: str | None = None,
    ):
        super().__init__(message, description)
        self.sql = sql

    def __str__(self) -> str:
        remark_desc = f" ({self.description})" if self.description else ""
        query_desc = f"\n  Query: {self.sql}" if self.sql else ""
        return f"{self.message}{remark_desc}{query_desc}"


class ValidationWarning(ValidationMessage):
    def __init__(
        self,
        message: str,
        description: str | None = None,
        strategy: EvaluationStrategy | None = None,
        sql: str | None = None,
    ):
        super().__init__(message, description)
        self.strategy = strategy
        self.sql = sql

    def __str__(self) -> str:
        warning_desc = f": {self.description}" if self.description else ""
        strategy_desc = f" with strategy '{self.strategy}'" if self.strategy else ""
        query_desc = f"\n  Query: {self.sql}" if self.sql else ""

        return f"{self.message}{strategy_desc}{warning_desc}{query_desc}"


class ValidationErrorDetails:
    def __init__(
        self,
        strategy: EvaluationStrategy,
        value: Any,
        sql: str | None = None,
        sql_error: str | None = None,
    ):
        self.strategy = strategy
        self.value = value
        self.sql = sql
        self.sql_error = sql_error


class ValidationError(ValidationMessage):
    def __init__(
        self,
        query_execution: QueryExecution,
        error_type: ValidationErrorType,
        message: str,
        details1: ValidationErrorDetails,
        details2: ValidationErrorDetails,
        description: str | None = None,
        col_index: int | None = None,
        concerned_expression: str | None = None,
        location: str | None = None,
    ):
        super().__init__(message, description)
        self.query_execution = query_execution
        self.error_type = error_type
        self.details1 = details1
        self.details2 = details2
        self.col_index = col_index
        self.concerned_expression = concerned_expression
        self.location = location

    def get_details_by_strategy_key(
        self,
    ) -> dict[EvaluationStrategyKey, ValidationErrorDetails]:
        return {
            details.strategy.identifier: details
            for details in [self.details1, self.details2]
        }

    def __str__(self) -> str:
        error_desc = f" ({self.description})" if self.description else ""
        location_desc = f" at {self.location}" if self.location is not None else ""

        strategy1_desc = f" ({self.details1.strategy})"
        strategy2_desc = f" ({self.details2.strategy})"
        value_and_strategy_desc = (
            f"\n  Value 1{strategy1_desc}: '{self.details1.value}' (type: {type(self.details1.value)})"
            f"\n  Value 2{strategy2_desc}: '{self.details2.value}' (type: {type(self.details2.value)})"
        )

        if self.error_type == ValidationErrorType.SUCCESS_MISMATCH:
            if self.details1.sql_error is not None:
                value_and_strategy_desc = value_and_strategy_desc + (
                    f"\n  Error 1: '{self.details1.sql_error}'"
                )
            if self.details2.sql_error is not None:
                value_and_strategy_desc = value_and_strategy_desc + (
                    f"\n  Error 2: '{self.details2.sql_error}'"
                )

        sql_desc = f"\n  Query 1: {self.details1.sql}\n  Query 2: {self.details2.sql}"
        return f"{self.error_type}: {self.message}{location_desc}{error_desc}.{value_and_strategy_desc}{sql_desc}"

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
from materialize.output_consistency.execution.sql_dialect_adjuster import (
    SqlDialectAdjuster,
)
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.query.query_result import QueryExecution


class ValidationErrorType(Enum):
    SUCCESS_MISMATCH = 1
    """Different outcome (success vs. error)"""
    ROW_COUNT_MISMATCH = 2
    """Different number of rows"""
    CONTENT_TYPE_MISMATCH = 3
    """Different data types in successful queries"""
    CONTENT_MISMATCH = 4
    """Different data in successful queries"""
    ERROR_MISMATCH = 5
    """Different error messages"""
    EXPLAIN_PLAN_MISMATCH = 6
    """Different explain plans"""

    def __str__(self) -> str:
        return self.name


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
        concerned_expression: Expression | None = None,
        location: str | None = None,
    ):
        super().__init__(message, description)
        self.query_execution = query_execution
        self.error_type = error_type
        self.details1 = details1
        self.details2 = details2
        self.col_index = col_index

        if concerned_expression is not None:
            self.concerned_expression_str = concerned_expression.to_sql(
                SqlDialectAdjuster(), query_execution.query_template.uses_join(), True
            )
            self.concerned_expression_hash = concerned_expression.hash()
        else:
            self.concerned_expression_str = None
            self.concerned_expression_hash = None

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
        expression_desc = (
            f"\nExpression: {self.concerned_expression_str}"
            if self.concerned_expression_str is not None
            else ""
        )

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
        expression_hash = (
            f"\n  Expression hash: {self.concerned_expression_hash}"
            if self.concerned_expression_hash is not None
            else ""
        )
        return f"{self.error_type}: {self.message}{location_desc}{error_desc}.{expression_desc}{value_and_strategy_desc}{sql_desc}{expression_hash}"

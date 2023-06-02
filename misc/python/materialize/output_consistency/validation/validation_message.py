# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from enum import Enum
from typing import Optional

from materialize.output_consistency.execution.evaluation_strategy import (
    EvaluationStrategy,
)
from materialize.output_consistency.query.query_result import QueryExecution


class ValidationErrorType(Enum):
    SUCCESS_MISMATCH = 1
    ROW_COUNT_MISMATCH = 2
    CONTENT_MISMATCH = 3
    ERROR_MISMATCH = 4


class ValidationMessage:
    """Either a `ValidationRemark`, `ValidationWarning`, or `ValidationError`"""

    def __init__(
        self,
        message: str,
        description: Optional[str] = None,
    ):
        self.message = message
        self.description = description


class ValidationRemark(ValidationMessage):
    def __init__(
        self,
        message: str,
        description: Optional[str] = None,
        sql: Optional[str] = None,
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
        description: Optional[str] = None,
        strategy: Optional[EvaluationStrategy] = None,
        sql: Optional[str] = None,
    ):
        super().__init__(message, description)
        self.strategy = strategy
        self.sql = sql

    def __str__(self) -> str:
        warning_desc = f" ({self.description})" if self.description else ""
        strategy_desc = f" with strategy '{self.strategy}'" if self.strategy else ""
        query_desc = f"\n  Query: {self.sql}"

        return f"{self.message}{strategy_desc}{warning_desc}{query_desc}"


class ValidationError(ValidationMessage):
    def __init__(
        self,
        query_execution: QueryExecution,
        error_type: ValidationErrorType,
        message: str,
        strategy1: EvaluationStrategy,
        strategy2: EvaluationStrategy,
        value1: str,
        value2: str,
        sql1: Optional[str] = None,
        sql2: Optional[str] = None,
        description: Optional[str] = None,
        col_index: Optional[int] = None,
        location: Optional[str] = None,
    ):
        super().__init__(message, description)
        self.query_execution = query_execution
        self.error_type = error_type
        self.value1 = value1
        self.value2 = value2
        self.strategy1 = strategy1
        self.strategy2 = strategy2
        self.sql1 = sql1
        self.sql2 = sql2
        self.col_index = col_index
        self.location = location

    def __str__(self) -> str:
        error_desc = f" ({self.description})" if self.description else ""
        location_desc = f" at {self.location}" if self.location is not None else ""

        strategy1_desc = f" ({self.strategy1})"
        strategy2_desc = f" ({self.strategy2})"
        value_and_strategy_desc = (
            f"\n  Value 1{strategy1_desc}: '{self.value1}' (type: {type(self.value1)})"
            f"\n  Value 2{strategy2_desc}: '{self.value2}' (type: {type(self.value2)})"
        )

        sql_desc = f"\n  Query 1: {self.sql1}\n  Query 2: {self.sql2}"
        return f"{self.error_type}: {self.message}{location_desc}{error_desc}.{value_and_strategy_desc}{sql_desc}"

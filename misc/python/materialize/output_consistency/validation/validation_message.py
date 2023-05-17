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
    ):
        super().__init__(message, description)
        self.error_type = error_type
        self.value1 = value1
        self.value2 = value2
        self.strategy1 = strategy1
        self.strategy2 = strategy2
        self.sql1 = sql1
        self.sql2 = sql2
        self.location = location

        if value1 is None and value2 is not None:
            raise RuntimeError("value1 must be set if value2 is set")

        if strategy1 is None and strategy2 is not None:
            raise RuntimeError("strategy1 must be set if strategy2 is set")

        if sql1 is None and sql2 is not None:
            raise RuntimeError("sql1 must be set if sql2 is set")

    def __str__(self) -> str:
        error_desc = f" ({self.description})" if self.description else ""
        location_desc = f" at {self.location}" if self.location is not None else ""
        value_and_strategy_desc = ""

        if self.value2 is not None:
            # self.value1 will never be null in this case
            strategy1_desc = f" ({self.strategy1})"
            strategy2_desc = f" ({self.strategy2})"
            value_and_strategy_desc = (
                f"\n  Value 1: '{self.value1}'{strategy1_desc}"
                f"\n  Value 2: '{self.value2}'{strategy2_desc}"
            )
        elif self.value1 is not None:
            strategy1_desc = f" ({self.strategy1})"
            value_and_strategy_desc = f"\n  Value: '{self.value1}'{strategy1_desc}"
        elif self.strategy2 is not None:
            # self.strategy1 will never be null in this case
            value_and_strategy_desc = (
                f"\n  Strategy 1: {self.strategy1}\nStrategy 2:   {self.strategy2}"
            )
        elif self.strategy1 is not None:
            value_and_strategy_desc = f"\n  Strategy: {self.strategy1}"

        sql_desc = ""
        if self.sql1 is not None:
            sql_desc = f"\n  Query 1: {self.sql1}"
        if self.sql2 is not None:
            sql_desc += f"\n  Query 2: {self.sql2}"

        return f"{self.error_type}: {self.message}{location_desc}{error_desc}.{value_and_strategy_desc}{sql_desc}"

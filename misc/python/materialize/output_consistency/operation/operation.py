# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum
from typing import List, Optional, Set

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.operation.operation_args_validator import (
    OperationArgsValidator,
)
from materialize.output_consistency.operation.operation_param import OperationParam

EXPRESSION_PLACEHOLDER = "$"


class OperationRelevance(Enum):
    HIGH = 2
    DEFAULT = 3
    LOW = 4


class DbOperationOrFunction:
    """Base class of `DbOperation` and `DbFunction`"""

    def __init__(
        self,
        params: List[OperationParam],
        min_param_count: int,
        max_param_count: int,
        return_type_category: DataTypeCategory,
        args_validators: Optional[Set[OperationArgsValidator]] = None,
        is_aggregation: bool = False,
        relevance: OperationRelevance = OperationRelevance.DEFAULT,
    ):
        if args_validators is None:
            args_validators = set()

        self.params = params
        self.min_param_count = min_param_count
        self.max_param_count = max_param_count
        self.return_type_category = return_type_category
        self.args_validators: Set[OperationArgsValidator] = args_validators
        self.is_aggregation = is_aggregation
        self.relevance = relevance

    def to_pattern(self, args_count: int) -> str:
        raise RuntimeError("Not implemented")

    def validate_args_count_in_range(self, args_count: int) -> None:
        if args_count < self.min_param_count:
            raise RuntimeError(
                f"To few arguments (got {args_count}, expected at least {self.min_param_count})"
            )
        if args_count > self.max_param_count:
            raise RuntimeError(
                f"To many arguments (got {args_count}, expected at most {self.max_param_count})"
            )

    def derive_characteristics(
        self, args: List[Expression]
    ) -> Set[ExpressionCharacteristics]:
        # a non-trivial implementation will be helpful for nested expressions
        return set()

    def __str__(self) -> str:
        raise RuntimeError("Not implemented")

    def is_expected_to_cause_db_error(self, args: List[Expression]) -> bool:
        """checks incompatibilities (e.g., division by zero) and potential error scenarios (e.g., addition of two max
        data_type)
        """

        self.validate_args_count_in_range(len(args))

        for validator in self.args_validators:
            if validator.is_expected_to_cause_error(args):
                return True

        for arg_index, arg in enumerate(args):
            param = self.params[arg_index]

            if not param.supports_arg(arg):
                return True

        return False


class DbOperation(DbOperationOrFunction):
    """A database operation (e.g., `a + b`)"""

    def __init__(
        self,
        pattern: str,
        params: List[OperationParam],
        return_type_category: DataTypeCategory,
        args_validators: Optional[Set[OperationArgsValidator]] = None,
        relevance: OperationRelevance = OperationRelevance.DEFAULT,
    ):
        param_count = len(params)
        super().__init__(
            params,
            min_param_count=param_count,
            max_param_count=param_count,
            return_type_category=return_type_category,
            args_validators=args_validators,
            is_aggregation=False,
            relevance=relevance,
        )
        self.pattern = pattern

        if param_count != self.pattern.count(EXPRESSION_PLACEHOLDER):
            raise RuntimeError(
                f"Operation has pattern {self.pattern} but has only {param_count} parameters"
            )

    def to_pattern(self, args_count: int) -> str:
        self.validate_args_count_in_range(args_count)
        return self.pattern

    def __str__(self) -> str:
        return f"DbOperation: {self.pattern}"


class DbFunction(DbOperationOrFunction):
    """A database function (e.g., `SUM(x)`)"""

    def __init__(
        self,
        function_name: str,
        params: List[OperationParam],
        return_type_category: DataTypeCategory,
        args_validators: Optional[Set[OperationArgsValidator]] = None,
        is_aggregation: bool = False,
        relevance: OperationRelevance = OperationRelevance.DEFAULT,
    ):
        self.validate_params(params)

        super().__init__(
            params,
            min_param_count=self.get_min_param_count(params),
            max_param_count=len(params),
            return_type_category=return_type_category,
            args_validators=args_validators,
            is_aggregation=is_aggregation,
            relevance=relevance,
        )
        self.function_name = function_name

    def validate_params(self, params: List[OperationParam]) -> None:
        optional_param_seen = False

        for param in params:
            if optional_param_seen and not param.optional:
                raise RuntimeError("Optional parameters must be at the end")

            if param.optional:
                optional_param_seen = True

    def get_min_param_count(self, params: List[OperationParam]) -> int:
        for index, param in enumerate(params):
            if param.optional:
                return index

        return len(params)

    def to_pattern(self, args_count: int) -> str:
        self.validate_args_count_in_range(args_count)
        args_pattern = ", ".join(["$"] * args_count)
        return f"{self.function_name}({args_pattern})"

    def __str__(self) -> str:
        return f"DbFunction: {self.function_name}"

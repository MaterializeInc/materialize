# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum
from typing import Dict, List, Optional, Set

from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.operation.operation_args_validator import (
    OperationArgsValidator,
)
from materialize.output_consistency.operation.operation_param import OperationParam
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec

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
        return_type_spec: ReturnTypeSpec,
        args_validators: Optional[Set[OperationArgsValidator]] = None,
        is_aggregation: bool = False,
        relevance: OperationRelevance = OperationRelevance.DEFAULT,
        is_enabled: bool = True,
    ):
        """
        :param is_enabled: an operation should only be disabled if its execution causes problems;
                            if it just fails, it should be ignored
        """
        if args_validators is None:
            args_validators = set()

        self.params = params
        self.min_param_count = min_param_count
        self.max_param_count = max_param_count
        self.return_type_spec = return_type_spec
        self.args_validators: Set[OperationArgsValidator] = args_validators
        self.is_aggregation = is_aggregation
        self.relevance = relevance
        self.is_enabled = is_enabled

    def to_pattern(self, args_count: int) -> str:
        raise NotImplementedError

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
        raise NotImplementedError

    def try_resolve_exact_data_type(self, args: List[Expression]) -> Optional[DataType]:
        return None

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

            if not param.supports_expression(arg):
                return True

        return False


class DbOperation(DbOperationOrFunction):
    """A database operation (e.g., `a + b`)"""

    def __init__(
        self,
        pattern: str,
        params: List[OperationParam],
        return_type_spec: ReturnTypeSpec,
        args_validators: Optional[Set[OperationArgsValidator]] = None,
        relevance: OperationRelevance = OperationRelevance.DEFAULT,
        is_enabled: bool = True,
    ):
        param_count = len(params)
        super().__init__(
            params,
            min_param_count=param_count,
            max_param_count=param_count,
            return_type_spec=return_type_spec,
            args_validators=args_validators,
            is_aggregation=False,
            relevance=relevance,
            is_enabled=is_enabled,
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
        return_type_spec: ReturnTypeSpec,
        args_validators: Optional[Set[OperationArgsValidator]] = None,
        is_aggregation: bool = False,
        relevance: OperationRelevance = OperationRelevance.DEFAULT,
        is_enabled: bool = True,
    ):
        self.validate_params(params)

        super().__init__(
            params,
            min_param_count=self.get_min_param_count(params),
            max_param_count=len(params),
            return_type_spec=return_type_spec,
            args_validators=args_validators,
            is_aggregation=is_aggregation,
            relevance=relevance,
            is_enabled=is_enabled,
        )
        self.function_name = function_name.lower()

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


class DbFunctionWithCustomPattern(DbFunction):
    def __init__(
        self,
        function_name: str,
        pattern_per_param_count: Dict[int, str],
        params: List[OperationParam],
        return_type_spec: ReturnTypeSpec,
        args_validators: Optional[Set[OperationArgsValidator]] = None,
        is_aggregation: bool = False,
        relevance: OperationRelevance = OperationRelevance.DEFAULT,
        is_enabled: bool = True,
    ):
        super().__init__(
            function_name,
            params,
            return_type_spec,
            args_validators,
            is_aggregation,
            relevance,
            is_enabled,
        )
        self.pattern_per_param_count = pattern_per_param_count

    def to_pattern(self, args_count: int) -> str:
        self.validate_args_count_in_range(args_count)

        if args_count not in self.pattern_per_param_count:
            raise RuntimeError(
                f"No pattern specified for {self.function_name} with {args_count} params"
            )

        return self.pattern_per_param_count[args_count]

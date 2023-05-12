# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import Optional

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.operations.operation_args_validator import (
    OperationArgsValidator,
)
from materialize.output_consistency.operations.operation_param import OperationParam

EXPRESSION_PLACEHOLDER = "$"


class DbOperationOrFunction:
    def __init__(
        self,
        params: list[OperationParam],
        min_param_count: int,
        max_param_count: int,
        return_type_category: DataTypeCategory,
        args_validators: Optional[set[OperationArgsValidator]] = None,
        commutative: bool = False,
    ):
        if args_validators is None:
            args_validators = set()

        self.params = params
        self.min_param_count = min_param_count
        self.max_param_count = max_param_count
        self.return_type_category = return_type_category
        self.commutative = commutative
        self.args_validators: set[OperationArgsValidator] = args_validators

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


class DbOperation(DbOperationOrFunction):
    def __init__(
        self,
        pattern: str,
        params: list[OperationParam],
        return_type_category: DataTypeCategory,
        args_validators: Optional[set[OperationArgsValidator]] = None,
        commutative: bool = False,
    ):
        param_count = len(params)
        super().__init__(
            params,
            min_param_count=param_count,
            max_param_count=param_count,
            return_type_category=return_type_category,
            commutative=commutative,
            args_validators=args_validators,
        )
        self.pattern = pattern

        if param_count != self.pattern.count(EXPRESSION_PLACEHOLDER):
            raise RuntimeError(
                f"Operation has pattern {self.pattern} but has only {param_count} parameters"
            )

    def to_pattern(self, args_count: int) -> str:
        self.validate_args_count_in_range(args_count)
        return self.pattern


class DbFunction(DbOperationOrFunction):
    def __init__(
        self,
        function_name: str,
        params: list[OperationParam],
        return_type_category: DataTypeCategory,
        args_validators: Optional[set[OperationArgsValidator]] = None,
        commutative: bool = False,
    ):
        self.validate_params(params)

        super().__init__(
            params,
            min_param_count=self.get_min_param_count(params),
            max_param_count=len(params),
            return_type_category=return_type_category,
            args_validators=args_validators,
            commutative=commutative,
        )
        self.function_name = function_name

    def validate_params(self, params: list[OperationParam]) -> None:
        optional_param_seen = False

        for param in params:
            if optional_param_seen and not param.optional:
                raise RuntimeError("Optional parameters must be at the end")

            if param.optional:
                optional_param_seen = True

    def get_min_param_count(self, params: list[OperationParam]) -> int:
        for index, param in enumerate(params):
            if param.optional:
                return index

        return len(params)

    def to_pattern(self, args_count: int) -> str:
        self.validate_args_count_in_range(args_count)
        args_pattern = ", ".join(["$"] * args_count)
        return f"{self.function_name}({args_pattern})"

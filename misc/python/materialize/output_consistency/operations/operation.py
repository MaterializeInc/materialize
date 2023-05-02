# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import Optional

from materialize.output_consistency.data_type.data_type_group import DataTypeGroup
from materialize.output_consistency.operations.operation_args_validator import (
    OperationArgsValidator,
)
from materialize.output_consistency.operations.operation_param import OperationParam

EXPRESSION_PLACEHOLDER = "$"


class OperationWithNParams:
    def __init__(
        self,
        pattern: str,
        params: list[OperationParam],
        return_type_group: DataTypeGroup,
        args_validators: Optional[set[OperationArgsValidator]] = None,
        commutative: bool = False,
    ):
        if args_validators is None:
            args_validators = set()

        self.pattern = pattern
        self.params = params
        self.param_count = len(params)
        self.return_type_group = return_type_group
        self.commutative = commutative
        self.args_validators: set[OperationArgsValidator] = args_validators

        if self.param_count != self.pattern.count(EXPRESSION_PLACEHOLDER):
            raise RuntimeError(
                f"Operation has pattern {self.pattern} but has only  {self.param_count} parameters"
            )


class OperationWithOneParam(OperationWithNParams):
    def __init__(
        self,
        pattern: str,
        param: OperationParam,
        return_type_group: DataTypeGroup,
        args_validators: Optional[set[OperationArgsValidator]] = None,
    ):
        self.pattern = pattern
        super().__init__(pattern, [param], return_type_group, args_validators, False)


class OperationWithTwoParams(OperationWithNParams):
    def __init__(
        self,
        pattern: str,
        param1: OperationParam,
        param2: OperationParam,
        return_type_group: DataTypeGroup,
        args_validators: Optional[set[OperationArgsValidator]] = None,
        commutative: bool = False,
    ):
        self.pattern = pattern
        super().__init__(
            pattern, [param1, param2], return_type_group, args_validators, commutative
        )


class UnaryFunction(OperationWithOneParam):
    def __init__(
        self,
        name: str,
        param: OperationParam,
        return_type_group: DataTypeGroup,
        args_validators: Optional[set[OperationArgsValidator]] = None,
    ):
        super().__init__(f"{name}($)", param, return_type_group, args_validators)


class BinaryFunction(OperationWithTwoParams):
    def __init__(
        self,
        name: str,
        param1: OperationParam,
        param2: OperationParam,
        return_type_group: DataTypeGroup,
        args_validators: Optional[set[OperationArgsValidator]] = None,
        commutative: bool = False,
    ):
        super().__init__(
            f"{name}($, $)",
            param1,
            param2,
            return_type_group,
            args_validators,
            commutative,
        )

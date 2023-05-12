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


class DbOperation:
    def __init__(
        self,
        pattern: str,
        params: list[OperationParam],
        return_type_category: DataTypeCategory,
        args_validators: Optional[set[OperationArgsValidator]] = None,
        commutative: bool = False,
    ):
        if args_validators is None:
            args_validators = set()

        self.pattern = pattern
        self.params = params
        self.param_count = len(params)
        self.return_type_category = return_type_category
        self.commutative = commutative
        self.args_validators: set[OperationArgsValidator] = args_validators

        if self.param_count != self.pattern.count(EXPRESSION_PLACEHOLDER):
            raise RuntimeError(
                f"Operation has pattern {self.pattern} but has only  {self.param_count} parameters"
            )


class DbFunction(DbOperation):
    def __init__(
        self,
        name: str,
        params: list[OperationParam],
        return_type_category: DataTypeCategory,
        args_validators: Optional[set[OperationArgsValidator]] = None,
        commutative: bool = False,
    ):
        args_pattern = ", ".join(["$"] * len(params))
        super().__init__(
            f"{name}({args_pattern})",
            params,
            return_type_category,
            args_validators,
            commutative,
        )

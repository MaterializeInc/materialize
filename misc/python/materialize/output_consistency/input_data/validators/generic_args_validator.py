# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import List

from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.operation.operation_args_validator import (
    OperationArgsValidator,
)


class DataTypeCategoryMatchesArgsValidator(OperationArgsValidator):
    def __init__(
        self,
        param_index1: int,
        param_index2: int,
    ):
        self.param_index1 = param_index1
        self.param_index2 = param_index2

    def is_expected_to_cause_error(self, args: List[Expression]) -> bool:
        if self.param_index1 >= len(args) or self.param_index2 >= len(args):
            return False

        return (
            args[self.param_index1].resolve_return_type_category()
            != args[self.param_index2].resolve_return_type_category()
        )

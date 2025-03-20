# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.generators.arg_context import ArgContext
from materialize.output_consistency.operation.volatile_data_operation_param import (
    VolatileDataOperationParam,
)
from materialize.output_consistency.selection.randomized_picker import RandomizedPicker


class SameOperationParam(VolatileDataOperationParam):
    def __init__(
        self,
        index_of_previous_param: int,
        optional: bool = False,
    ):
        super().__init__(
            DataTypeCategory.ANY,
            optional,
        )
        self.index_of_previous_param = index_of_previous_param

    def supports_type(
        self, data_type: DataType, previous_args: list[Expression]
    ) -> bool:
        return True

    def generate_expression(
        self, arg_context: ArgContext, randomized_picker: RandomizedPicker
    ) -> Expression:
        expression_to_use = arg_context.args[self.index_of_previous_param]
        expression_to_use.recursively_mark_as_shared()
        return expression_to_use

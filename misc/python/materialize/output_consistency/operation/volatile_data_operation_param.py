# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.generators.arg_context import ArgContext
from materialize.output_consistency.operation.operation_param import OperationParam
from materialize.output_consistency.selection.randomized_picker import RandomizedPicker


class VolatileDataOperationParam(OperationParam):

    def __init__(
        self,
        type_category: DataTypeCategory,
        optional: bool = False,
    ):
        super().__init__(type_category, optional)

    def generate_expression(
        self, arg_context: ArgContext, randomized_picker: RandomizedPicker
    ) -> Expression:
        raise NotImplementedError

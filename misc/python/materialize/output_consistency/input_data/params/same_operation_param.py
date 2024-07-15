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
from materialize.output_consistency.operation.operation_param import OperationParam


class SameOperationParam(OperationParam):
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

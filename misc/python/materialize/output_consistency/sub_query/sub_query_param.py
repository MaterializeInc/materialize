# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.operation.operation_param import OperationParam


class SubQueryParam(OperationParam):
    def __init__(
        self,
        data_type: DataType,
        optional: bool = False,
        incompatibilities: set[ExpressionCharacteristics] | None = None,
    ):
        super().__init__(
            data_type.category,
            optional,
            incompatibilities,
            incompatibility_combinations=None,
        )
        self.data_type = data_type

    def supports_type(
        self, data_type: DataType, previous_args: list[Expression]
    ) -> bool:
        return data_type.identifier == self.data_type.identifier

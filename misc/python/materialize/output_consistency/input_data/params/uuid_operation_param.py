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
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.input_data.types.uuid_type_provider import (
    UUID_TYPE_IDENTIFIER,
)
from materialize.output_consistency.operation.operation_param import OperationParam


class UuidOperationParam(OperationParam):
    def __init__(
        self,
        optional: bool = False,
        incompatibilities: set[ExpressionCharacteristics] | None = None,
    ):
        super().__init__(
            DataTypeCategory.UUID,
            optional,
            incompatibilities,
            incompatibility_combinations=None,
        )

    def supports_type(
        self, data_type: DataType, previous_args: list[Expression]
    ) -> bool:
        return data_type.internal_identifier == UUID_TYPE_IDENTIFIER

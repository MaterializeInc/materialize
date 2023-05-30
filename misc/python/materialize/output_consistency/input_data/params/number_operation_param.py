# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List, Optional, Set

from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.input_data.types.number_types_provider import (
    INT8_TYPE,
    UINT4_TYPE,
    UINT8_TYPE,
    NumberDataType,
)
from materialize.output_consistency.operation.operation_param import OperationParam


class NumericOperationParam(OperationParam):
    def __init__(
        self,
        optional: bool = False,
        incompatibilities: Optional[Set[ExpressionCharacteristics]] = None,
        incompatibility_combinations: Optional[
            List[Set[ExpressionCharacteristics]]
        ] = None,
        only_int_type: bool = False,
        no_int_type_larger_int4: bool = False,
        no_floating_point_type: bool = False,
        no_unsigned_type: bool = False,
    ):
        if incompatibilities is None:
            incompatibilities = set()

        # expect all numeric operations to have issues with an oversize input
        incompatibilities.add(ExpressionCharacteristics.OVERSIZE)

        super().__init__(
            DataTypeCategory.NUMERIC,
            optional,
            incompatibilities,
            incompatibility_combinations,
        )
        self.only_int_type = only_int_type
        self.no_int_type_larger_int4 = no_int_type_larger_int4
        self.no_floating_point_type = no_floating_point_type
        self.no_unsigned_type = no_unsigned_type

    def supports_type(self, data_type: DataType) -> bool:
        if not isinstance(data_type, NumberDataType):
            return False

        if self.only_int_type and data_type.is_decimal:
            return False

        if self.no_int_type_larger_int4 and data_type in {
            INT8_TYPE,
            UINT4_TYPE,
            UINT8_TYPE,
        }:
            return False

        if self.no_floating_point_type and data_type.is_floating_point_type:
            return False

        if self.no_unsigned_type and not data_type.is_signed:
            return False

        return True

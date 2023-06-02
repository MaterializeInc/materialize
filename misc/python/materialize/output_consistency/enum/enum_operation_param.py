# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import List, Set

from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.enum.enum_constant import EnumConstant
from materialize.output_consistency.enum.enum_data_type import EnumDataType
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.operation.operation_param import OperationParam


class EnumConstantOperationParam(OperationParam):
    def __init__(self, values: Set[str], add_quotes: bool, optional: bool = False):
        super().__init__(
            DataTypeCategory.ENUM,
            optional=optional,
            incompatibilities=None,
            incompatibility_combinations=None,
        )
        self.values = list(values)
        self.add_quotes = add_quotes

    def supports_type(
        self, data_type: DataType, previous_args: List[Expression]
    ) -> bool:
        return isinstance(data_type, EnumDataType)

    def get_enum_constant(self, index: int) -> EnumConstant:
        assert (
            0 <= index < len(self.values)
        ), f"Index {index} out of range in list with {len(self.values)} values"
        value = self.values[index]
        return EnumConstant(f"'{value}'" if self.add_quotes else value)

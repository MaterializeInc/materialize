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
from materialize.output_consistency.enum.enum_constant import EnumConstant
from materialize.output_consistency.enum.enum_data_type import EnumDataType
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.operation.operation_param import OperationParam


class EnumConstantOperationParam(OperationParam):
    def __init__(
        self,
        values: list[str],
        add_quotes: bool,
        add_invalid_value: bool = True,
        optional: bool = False,
        invalid_value: str = "invalid_value_123",
    ):
        super().__init__(
            DataTypeCategory.ENUM,
            optional=optional,
            incompatibilities=None,
            incompatibility_combinations=None,
        )
        assert len(values) == len(set(values)), f"Values contain duplicates {values}"
        self.values = values

        if add_invalid_value:
            self.invalid_value = invalid_value
            self.values.append(invalid_value)
        else:
            self.invalid_value = None

        self.add_quotes = add_quotes
        self.characteristics_per_index: list[set[ExpressionCharacteristics]] = [
            set() for _ in values
        ]

    def supports_type(
        self, data_type: DataType, previous_args: list[Expression]
    ) -> bool:
        return isinstance(data_type, EnumDataType)

    def get_enum_constant(self, index: int) -> EnumConstant:
        assert (
            0 <= index < len(self.values)
        ), f"Index {index} out of range in list with {len(self.values)} values: {self.values}"
        value = self.values[index]
        characteristics = self.characteristics_per_index[index]
        return EnumConstant(value, self.add_quotes, characteristics)

    def get_valid_values(self) -> list[str]:
        if self.invalid_value is None:
            return self.values

        return [value for value in self.values if value != self.invalid_value]

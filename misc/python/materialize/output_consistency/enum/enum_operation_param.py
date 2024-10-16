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
from materialize.output_consistency.generators.arg_context import ArgContext
from materialize.output_consistency.operation.volatile_data_operation_param import (
    VolatileDataOperationParam,
)
from materialize.output_consistency.selection.randomized_picker import RandomizedPicker

_INDEX_OF_NULL_VALUE = 0


class EnumConstantOperationParam(VolatileDataOperationParam):
    def __init__(
        self,
        values: list[str],
        add_quotes: bool,
        add_invalid_value: bool = True,
        add_null_value: bool = True,
        optional: bool = False,
        invalid_value: str = "invalid_value_123",
        tags: set[str] | None = None,
    ):
        super().__init__(
            DataTypeCategory.ENUM,
            optional=optional,
        )
        assert len(values) == len(set(values)), f"Values contain duplicates {values}"
        self.values = values

        if add_invalid_value:
            self.invalid_value = invalid_value
            self.values.append(invalid_value)
        else:
            self.invalid_value = None

        self.add_null_value = add_null_value
        null_value = "NULL"
        if add_null_value:
            # NULL value must be at the beginning
            self.values.insert(
                _INDEX_OF_NULL_VALUE,
                null_value,
            )

        self.add_quotes = add_quotes
        self.characteristics_per_value: dict[str, set[ExpressionCharacteristics]] = (
            dict()
        )

        for value in self.values:
            self.characteristics_per_value[value] = set()

        if add_invalid_value:
            self.characteristics_per_value[invalid_value].add(
                ExpressionCharacteristics.ENUM_INVALID
            )

        if add_null_value:
            self.characteristics_per_value[null_value].add(
                ExpressionCharacteristics.NULL
            )

        self.tags = tags

    def supports_type(
        self, data_type: DataType, previous_args: list[Expression]
    ) -> bool:
        return isinstance(data_type, EnumDataType)

    def get_enum_constant(self, index: int) -> EnumConstant:
        assert (
            0 <= index < len(self.values)
        ), f"Index {index} out of range in list with {len(self.values)} values: {self.values}"
        value = self.values[index]
        characteristics = self.characteristics_per_value[value]

        quote_value = self.add_quotes
        if self.add_null_value and index == 0:
            quote_value = False

        return EnumConstant(value, quote_value, characteristics, self.tags)

    def get_valid_values(self) -> list[str]:
        return [
            value
            for index, value in enumerate(self.values)
            if value != self.invalid_value
            and (index != _INDEX_OF_NULL_VALUE or not self.add_null_value)
        ]

    def generate_expression(
        self, arg_context: ArgContext, randomized_picker: RandomizedPicker
    ) -> Expression:
        enum_constant_index = randomized_picker.random_number(0, len(self.values) - 1)
        return self.get_enum_constant(enum_constant_index)

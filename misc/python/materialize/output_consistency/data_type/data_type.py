# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.data_type.value_characteristics import (
    ValueCharacteristics,
)
from materialize.output_consistency.expressions.expression import Expression


class DataType:
    def __init__(self, identifier: str, type_name: str, category: DataTypeCategory):
        self.identifier = identifier
        self.type_name = type_name
        self.category = category
        self.raw_values: list[RawValue] = []

    def add_raw_value(
        self,
        value: str,
        column_name: str,
        characteristics: set[ValueCharacteristics],
    ) -> None:
        self.raw_values.append(RawValue(value, self, column_name, characteristics))


class NumberDataType(DataType):
    def __init__(
        self,
        identifier: str,
        type_name: str,
        is_signed: bool,
        is_decimal: bool,
        tiny_value: str,
        max_value: str,
    ):
        super().__init__(identifier, type_name, DataTypeCategory.NUMERIC)
        self.is_signed = is_signed
        self.is_decimal = is_decimal
        self.tiny_value = tiny_value
        self.max_value = max_value


class RawValue(Expression):
    def __init__(
        self,
        value: str,
        data_type: DataType,
        value_identifier: str,
        characteristics: set[ValueCharacteristics],
    ):
        super().__init__(characteristics)
        self.value = value
        self.data_type = data_type
        self.column_name = f"{data_type.identifier.lower()}_{value_identifier.lower()}"

    def resolve_data_type_category(self) -> DataTypeCategory:
        return self.data_type.category

    def to_sql(self) -> str:
        return self.to_sql_as_column()

    def to_sql_as_column(self) -> str:
        return self.column_name

    def to_sql_as_value(self) -> str:
        return f"{self.value}::{self.data_type.type_name}"

    def __str__(self) -> str:
        return f"{self.column_name} ({self.value})"

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.data_value.data_value import DataValue
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)


class DataTypeWithValues:
    """Data type and its specified values"""

    def __init__(self, data_type: DataType):
        self.data_type = data_type
        self.raw_values: list[DataValue] = []

    def add_raw_value(
        self,
        value: str,
        column_name: str,
        characteristics: set[ExpressionCharacteristics],
    ) -> None:
        self.raw_values.append(
            DataValue(value, self.data_type, column_name, characteristics)
        )

    def add_characteristic_to_all_values(
        self, characteristic: ExpressionCharacteristics
    ) -> None:
        for raw_value in self.raw_values:
            raw_value.characteristics.add(characteristic)

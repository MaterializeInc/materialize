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
from materialize.output_consistency.data_value.data_column import DataColumn
from materialize.output_consistency.data_value.data_value import DataValue
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)


class DataTypeWithValues:
    """Data type and its specified values"""

    def __init__(self, data_type: DataType):
        """Creates a new instance and prefills the values with a NULL value"""
        self.data_type = data_type
        self.null_value = self._create_raw_value(
            "NULL", "NULL", {ExpressionCharacteristics.NULL}
        )
        # values (and implicitly a column for each value for horizontal storage)
        self.raw_values: List[DataValue] = [self.null_value]

    def _create_raw_value(
        self,
        value: str,
        column_name: str,
        characteristics: Set[ExpressionCharacteristics],
    ) -> DataValue:
        return DataValue(value, self.data_type, column_name, characteristics)

    def add_raw_value(
        self,
        value: str,
        column_name: str,
        characteristics: Set[ExpressionCharacteristics],
    ) -> None:
        self.raw_values.append(
            self._create_raw_value(value, column_name, characteristics)
        )

    def add_characteristic_to_all_values(
        self, characteristic: ExpressionCharacteristics
    ) -> None:
        for raw_value in self.raw_values:
            raw_value.own_characteristics.add(characteristic)

    def create_vertical_storage_column(self) -> DataColumn:
        return DataColumn(self.data_type, self.raw_values)

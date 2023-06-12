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
from materialize.output_consistency.data_value.data_value import DataValue
from materialize.output_consistency.execution.value_storage_layout import (
    ValueStorageLayout,
)
from materialize.output_consistency.expression.expression import LeafExpression
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec
from materialize.output_consistency.selection.selection import DataRowSelection


class DataColumn(LeafExpression):
    """A column with a value per row (in contrast to an `ExpressionWithArgs`) for VERTICAL storage"""

    def __init__(self, data_type: DataType, row_values_of_column: List[DataValue]):
        column_name = f"{data_type.identifier.lower()}_val"
        super().__init__(
            column_name, data_type, set(), ValueStorageLayout.VERTICAL, False, False
        )
        self.values = row_values_of_column

    def resolve_return_type_spec(self) -> ReturnTypeSpec:
        # do not provide characteristics on purpose, the spec of this class is not value-specific
        return self.data_type.resolve_return_type_spec(set())

    def resolve_return_type_category(self) -> DataTypeCategory:
        return self.data_type.category

    def recursively_collect_involved_characteristics(
        self, row_selection: DataRowSelection
    ) -> Set[ExpressionCharacteristics]:
        involved_characteristics: Set[ExpressionCharacteristics] = set()

        selected_values = self.get_values_at_rows(row_selection)
        for value in selected_values:
            characteristics_of_value = (
                value.recursively_collect_involved_characteristics(row_selection)
            )
            involved_characteristics = involved_characteristics.union(
                characteristics_of_value
            )

        return involved_characteristics

    def get_filtered_values(self, row_selection: DataRowSelection) -> List[DataValue]:
        if row_selection.includes_all():
            return self.values

        selected_rows = []

        for row_index, row_value in enumerate(self.values):
            if row_selection.is_included(row_index):
                selected_rows.append(row_value)

        return selected_rows

    def get_values_at_rows(self, row_selection: DataRowSelection) -> List[DataValue]:
        if row_selection.keys is None:
            return self.values

        values = []
        for row_index in row_selection.keys:
            values.append(self.get_value_at_row(row_index))

        return values

    def get_value_at_row(self, row_index: int) -> DataValue:
        """All types need to have the same number of rows, but not all have the same number of distinct values. After
        having iterated through of all values of the given type, begin repeating values but skip the NULL value, which
        is known to be the first value of all types.
        :param row_index: an arbitrary, positive number, may be out of the value range
        """

        value_index = row_index

        if value_index >= len(self.values):
            null_value_offset = 1
            available_value_count_without_null = len(self.values) - 1
            value_index = null_value_offset + (
                (value_index - null_value_offset) % available_value_count_without_null
            )

        return self.values[value_index]

    def __str__(self) -> str:
        return self.to_sql(False)

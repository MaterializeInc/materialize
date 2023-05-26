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
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.expression.leaf_expression import LeafExpression
from materialize.output_consistency.selection.selection import DataRowSelection


class DataColumn(LeafExpression):
    """A column with a value per row (in contrast to an `ExpressionWithArgs`) for VERTICAL storage"""

    def __init__(self, data_type: DataType, row_values_of_column: List[DataValue]):
        column_name = f"{data_type.identifier.lower()}_val"
        super().__init__(column_name, set(), ValueStorageLayout.VERTICAL, False, False)
        self.data_type = data_type
        self.row_values_of_column = row_values_of_column

    def resolve_data_type_category(self) -> DataTypeCategory:
        return self.data_type.category

    def collect_involved_characteristics(
        self, row_selection: DataRowSelection
    ) -> Set[ExpressionCharacteristics]:
        involved_characteristics: Set[ExpressionCharacteristics] = set()

        for index, value in enumerate(self.row_values_of_column):
            if row_selection.is_included(index):
                involved_characteristics = involved_characteristics.union(
                    value.collect_involved_characteristics(row_selection)
                )

        return involved_characteristics

    def get_filtered_values(self, row_selection: DataRowSelection) -> List[DataValue]:
        if row_selection.includes_all():
            return self.row_values_of_column

        selected_rows = []

        for row_index, row_value in enumerate(self.row_values_of_column):
            if row_selection.is_included(row_index):
                selected_rows.append(row_value)

        return selected_rows

    def __str__(self) -> str:
        return self.to_sql()

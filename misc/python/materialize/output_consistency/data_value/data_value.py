# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import Set

from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.execution.value_storage_layout import (
    ValueStorageLayout,
)
from materialize.output_consistency.expression.expression import LeafExpression
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec
from materialize.output_consistency.selection.selection import DataRowSelection


class DataValue(LeafExpression):
    """A simple value (in contrast to an `ExpressionWithArgs`) for HORIZONTAL storage"""

    def __init__(
        self,
        value: str,
        data_type: DataType,
        value_identifier: str,
        characteristics: Set[ExpressionCharacteristics],
    ):
        column_name = f"{data_type.identifier.lower()}_{value_identifier.lower()}"
        super().__init__(
            column_name,
            data_type,
            characteristics,
            ValueStorageLayout.HORIZONTAL,
            False,
            False,
        )
        self.value = value

    def resolve_return_type_spec(self) -> ReturnTypeSpec:
        return self.data_type.resolve_return_type_spec(self.own_characteristics)

    def resolve_return_type_category(self) -> DataTypeCategory:
        return self.data_type.category

    def to_sql_as_value(self) -> str:
        return self.data_type.value_to_sql(self.value)

    def recursively_collect_involved_characteristics(
        self, row_selection: DataRowSelection
    ) -> Set[ExpressionCharacteristics]:
        return self.own_characteristics

    def __str__(self) -> str:
        return f"{self.column_name} (={self.value})"

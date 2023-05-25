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
from materialize.output_consistency.data_value.data_row_selection import (
    DataRowSelection,
)
from materialize.output_consistency.execution.value_storage_layout import (
    ValueStorageLayout,
)
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)


class DataValue(Expression):
    """A simple value (in contrast to an `ExpressionWithArgs`) for HORIZONTAL storage"""

    def __init__(
        self,
        value: str,
        data_type: DataType,
        value_identifier: str,
        characteristics: Set[ExpressionCharacteristics],
    ):
        super().__init__(characteristics, ValueStorageLayout.HORIZONTAL, False, False)
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

    def collect_involved_characteristics(
        self, row_selection: DataRowSelection
    ) -> Set[ExpressionCharacteristics]:
        return self.own_characteristics

    def __str__(self) -> str:
        return f"{self.column_name} ({self.value})"

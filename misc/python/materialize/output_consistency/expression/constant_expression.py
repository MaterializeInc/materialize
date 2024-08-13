# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from __future__ import annotations

from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.execution.sql_dialect_adjuster import (
    SqlDialectAdjuster,
)
from materialize.output_consistency.execution.value_storage_layout import (
    ValueStorageLayout,
)
from materialize.output_consistency.expression.expression import LeafExpression
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec
from materialize.output_consistency.query.data_source import DataSource
from materialize.output_consistency.selection.selection import (
    DataRowSelection,
)


class ConstantExpression(LeafExpression):

    def __init__(
        self,
        value: str,
        data_type: DataType,
        characteristics: set[ExpressionCharacteristics] = set(),
        is_aggregate: bool = False,
    ):
        column_name = "<none>"
        super().__init__(
            column_name,
            data_type,
            characteristics,
            ValueStorageLayout.ANY,
            is_aggregate,
            False,
        )
        self.value = value

    def resolve_return_type_spec(self) -> ReturnTypeSpec:
        return self.data_type.resolve_return_type_spec(self.own_characteristics)

    def resolve_return_type_category(self) -> DataTypeCategory:
        return self.data_type.category

    def to_sql_as_value(self, sql_adjuster: SqlDialectAdjuster) -> str:
        return self.data_type.value_to_sql(self.value, sql_adjuster)

    def recursively_collect_involved_characteristics(
        self, row_selection: DataRowSelection
    ) -> set[ExpressionCharacteristics]:
        return self.own_characteristics

    def collect_vertical_table_indices(self) -> set[int]:
        return set()

    def get_data_source(self) -> DataSource:
        return DataSource(table_index=None)

    def __str__(self) -> str:
        return f"ConstantExpression (value={self.value}, type={self.data_type})"

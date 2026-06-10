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
from materialize.output_consistency.data_value.source_column_identifier import (
    SourceColumnIdentifier,
)
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
from materialize.output_consistency.selection.row_selection import DataRowSelection


class DataValue(LeafExpression):
    """A simple value (in contrast to an `ExpressionWithArgs`) for HORIZONTAL storage"""

    def __init__(
        self,
        value: str,
        data_type: DataType,
        value_identifier: str,
        characteristics: set[ExpressionCharacteristics],
        is_null_value: bool,
        is_pg_compatible: bool = True,
    ):
        column_name = (
            f"{data_type.internal_identifier.lower()}_{value_identifier.lower()}"
        )
        super().__init__(
            column_name,
            data_type,
            characteristics,
            ValueStorageLayout.HORIZONTAL,
            data_source=DataSource(table_index=None),
            is_aggregate=False,
            is_expect_error=False,
        )
        self.value = value
        self.vertical_table_indices: set[int] = set()
        self.is_null_value = is_null_value
        self.is_pg_compatible = is_pg_compatible

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
        return self.vertical_table_indices

    def get_source_column_identifier(self) -> SourceColumnIdentifier:
        source_column_identifier = super().get_source_column_identifier()
        assert source_column_identifier is not None
        return source_column_identifier

    def __str__(self) -> str:
        return f"DataValue (column='{self.column_name}', value={self.value}, type={self.data_type})"

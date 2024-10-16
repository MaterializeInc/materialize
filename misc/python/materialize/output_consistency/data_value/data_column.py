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
from materialize.output_consistency.data_value.data_value import DataValue
from materialize.output_consistency.data_value.source_column_identifier import (
    SourceColumnIdentifier,
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


class DataColumn(LeafExpression):
    """A column with a value per row (in contrast to an `ExpressionWithArgs`) for VERTICAL storage"""

    def __init__(self, data_type: DataType, row_values_of_column: list[DataValue]):
        column_name = f"{data_type.internal_identifier.lower()}_val"
        # data_source will be assigned later
        super().__init__(
            column_name,
            data_type,
            set(),
            ValueStorageLayout.VERTICAL,
            data_source=None,
            is_aggregate=False,
            is_expect_error=False,
        )
        self.values = row_values_of_column

    def assign_data_source(self, data_source: DataSource, force: bool) -> None:
        if self.data_source is not None:
            if self.is_shared:
                # the source has already been set
                return
            if not force:
                raise RuntimeError("Data source already assigned")

        self.data_source = data_source

    def resolve_return_type_spec(self) -> ReturnTypeSpec:
        # do not provide characteristics on purpose, the spec of this class is not value-specific
        return self.data_type.resolve_return_type_spec(set())

    def resolve_return_type_category(self) -> DataTypeCategory:
        return self.data_type.category

    def recursively_collect_involved_characteristics(
        self, row_selection: DataRowSelection
    ) -> set[ExpressionCharacteristics]:
        involved_characteristics: set[ExpressionCharacteristics] = set()

        selected_values = self.get_values_at_rows(
            row_selection,
            table_index=(
                self.data_source.table_index if self.data_source is not None else None
            ),
        )
        for value in selected_values:
            characteristics_of_value = (
                value.recursively_collect_involved_characteristics(row_selection)
            )
            involved_characteristics = involved_characteristics.union(
                characteristics_of_value
            )

        return involved_characteristics

    def collect_vertical_table_indices(self) -> set[int]:
        return set()

    def get_filtered_values(self, row_selection: DataRowSelection) -> list[DataValue]:
        assert self.data_source is not None

        if row_selection.includes_all_of_source(self.data_source):
            return self.values

        selected_rows = []

        for row_index, row_value in enumerate(self.values):
            if row_selection.is_included_in_source(self.data_source, row_index):
                selected_rows.append(row_value)

        return selected_rows

    def get_values_at_rows(
        self, row_selection: DataRowSelection, table_index: int | None
    ) -> list[DataValue]:
        if row_selection.includes_all_of_all_sources():
            return self.values

        if self.data_source is None:
            # still unknown, provide all values
            return self.values

        if row_selection.includes_all_of_source(self.data_source):
            return self.values

        values = []
        for row_index in row_selection.get_row_indices(self.data_source):
            values.append(self.get_value_at_row(row_index, table_index))

        return values

    def get_value_at_row(
        self,
        row_index: int,
        table_index: int | None,
    ) -> DataValue:
        """All types need to have the same number of rows, but not all have the same number of distinct values. After
        having iterated through of all values of the given type, begin repeating values but skip the NULL value, which
        is known to be the first value of all types.
        :param row_index: an arbitrary, positive number, may be out of the value range
        """

        values_of_table = self._get_values_of_table(table_index)
        assert len(values_of_table) > 0, f"No values for table index {table_index}"

        # if there is a NULL value, it will always be at position 0; we can only exclude it if we have other values
        has_null_value_to_exclude = (
            values_of_table[0].is_null_value and len(values_of_table) > 1
        )
        value_index = row_index

        if value_index >= len(values_of_table):
            null_value_offset = 1 if has_null_value_to_exclude else 0
            available_value_count = len(values_of_table) - (
                1 if has_null_value_to_exclude else 0
            )
            value_index = null_value_offset + (
                (value_index - null_value_offset) % available_value_count
            )

        return values_of_table[value_index]

    def _get_values_of_table(self, table_index: int | None) -> list[DataValue]:
        return [
            value
            for value in self.values
            if table_index is None or table_index in value.vertical_table_indices
        ]

    def get_data_source(self) -> DataSource | None:
        assert self.data_source is not None, "Data source not assigned"
        return self.data_source

    def get_source_column_identifier(self) -> SourceColumnIdentifier:
        source_column_identifier = super().get_source_column_identifier()
        assert source_column_identifier is not None
        return source_column_identifier

    def __str__(self) -> str:
        return f"DataValue (column='{self.column_name}', type={self.data_type})"

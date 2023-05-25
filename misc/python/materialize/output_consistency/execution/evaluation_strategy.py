# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List

from materialize.output_consistency.data_type.data_type_with_values import (
    DataTypeWithValues,
)
from materialize.output_consistency.execution.value_storage_layout import (
    VERTICAL_LAYOUT_ROW_INDEX_COL_NAME,
    ValueStorageLayout,
)
from materialize.output_consistency.selection.selection import (
    ALL_ROWS_SELECTION,
    ALL_TABLE_COLUMNS_BY_NAME_SELECTION,
    DataRowSelection,
    TableColumnByNameSelection,
)


class EvaluationStrategy:
    """Strategy how to execute a `QueryTemplate`"""

    def __init__(self, key: str, name: str):
        self.key = key
        self.name = name

    def generate_sources(
        self, data_type_with_values: List[DataTypeWithValues]
    ) -> List[str]:
        statements = []
        statements.extend(
            self.generate_source_for_storage_layout(
                data_type_with_values,
                ValueStorageLayout.HORIZONTAL,
                ALL_ROWS_SELECTION,
                ALL_TABLE_COLUMNS_BY_NAME_SELECTION,
            )
        )
        statements.extend(
            self.generate_source_for_storage_layout(
                data_type_with_values,
                ValueStorageLayout.VERTICAL,
                ALL_ROWS_SELECTION,
                ALL_TABLE_COLUMNS_BY_NAME_SELECTION,
            )
        )
        return statements

    def generate_source_for_storage_layout(
        self,
        data_type_with_values: List[DataTypeWithValues],
        storage_layout: ValueStorageLayout,
        row_selection: DataRowSelection,
        table_column_selection: TableColumnByNameSelection,
    ) -> List[str]:
        raise RuntimeError("Not implemented")

    def get_db_object_name(self, storage_layout: ValueStorageLayout) -> str:
        storage_suffix = (
            "horiz" if storage_layout == ValueStorageLayout.HORIZONTAL else "vert"
        )
        return f"{self.key}_{storage_suffix}"

    def __str__(self) -> str:
        return self.name


class DummyEvaluation(EvaluationStrategy):
    def __init__(self) -> None:
        super().__init__("<source>", "Dummy")

    def generate_sources(
        self,
        data_type_with_values: List[DataTypeWithValues],
    ) -> List[str]:
        return []


class DataFlowRenderingEvaluation(EvaluationStrategy):
    def __init__(self) -> None:
        super().__init__("t_dfr", "Dataflow rendering")

    def generate_source_for_storage_layout(
        self,
        data_type_with_values: List[DataTypeWithValues],
        storage_layout: ValueStorageLayout,
        row_selection: DataRowSelection,
        table_column_selection: TableColumnByNameSelection,
    ) -> List[str]:
        db_object_name = self.get_db_object_name(storage_layout)
        statements = []

        column_specs = _create_column_specs(
            data_type_with_values, storage_layout, True, table_column_selection
        )
        statements.append(f"DROP TABLE IF EXISTS {db_object_name};")
        statements.append(f"CREATE TABLE {db_object_name} ({', '.join(column_specs)});")

        value_rows = _create_value_rows(
            data_type_with_values, storage_layout, row_selection, table_column_selection
        )

        for value_row in value_rows:
            statements.append(f"INSERT INTO {db_object_name} VALUES ({value_row});")

        return statements


class ConstantFoldingEvaluation(EvaluationStrategy):
    def __init__(self) -> None:
        super().__init__("v_ctf", "Constant folding")

    def generate_source_for_storage_layout(
        self,
        data_type_with_values: List[DataTypeWithValues],
        storage_layout: ValueStorageLayout,
        row_selection: DataRowSelection,
        table_column_selection: TableColumnByNameSelection,
    ) -> List[str]:
        column_specs = _create_column_specs(
            data_type_with_values, storage_layout, False, table_column_selection
        )

        value_rows = _create_value_rows(
            data_type_with_values, storage_layout, row_selection, table_column_selection
        )
        value_specification = "\n    UNION SELECT ".join(value_rows)

        create_view_statement = (
            f"CREATE OR REPLACE VIEW {self.get_db_object_name(storage_layout)} ({', '.join(column_specs)})\n"
            f" AS SELECT {value_specification};"
        )

        return [create_view_statement]


def _create_column_specs(
    data_type_with_values: List[DataTypeWithValues],
    storage_layout: ValueStorageLayout,
    include_type: bool,
    table_column_selection: TableColumnByNameSelection,
) -> List[str]:
    column_specs = []

    if storage_layout == ValueStorageLayout.VERTICAL:
        type_info = " INT" if include_type else ""
        column_specs.append(f"{VERTICAL_LAYOUT_ROW_INDEX_COL_NAME}{type_info}")

    for type_with_values in data_type_with_values:
        type_info = f" {type_with_values.data_type.type_name}" if include_type else ""

        if storage_layout == ValueStorageLayout.HORIZONTAL:
            for data_value in type_with_values.raw_values:
                if table_column_selection.is_included(data_value.column_name):
                    column_specs.append(f"{data_value.column_name}{type_info}")
        elif storage_layout == ValueStorageLayout.VERTICAL:
            column_name = (
                type_with_values.create_vertical_storage_column_expression().column_name
            )
            if table_column_selection.is_included(column_name):
                column_specs.append(f"{column_name}{type_info}")
        else:
            raise RuntimeError(f"Unexpected storage layout: {storage_layout}")

    return column_specs


def _create_value_rows(
    data_type_with_values: List[DataTypeWithValues],
    storage_layout: ValueStorageLayout,
    row_selection: DataRowSelection,
    table_column_selection: TableColumnByNameSelection,
) -> List[str]:
    if storage_layout == ValueStorageLayout.HORIZONTAL:
        return [
            __create_horizontal_value_row(data_type_with_values, table_column_selection)
        ]
    elif storage_layout == ValueStorageLayout.VERTICAL:
        return __create_vertical_value_rows(
            data_type_with_values, row_selection, table_column_selection
        )
    else:
        raise RuntimeError(f"Unexpected storage layout: {storage_layout}")


def __create_horizontal_value_row(
    data_type_with_values: List[DataTypeWithValues],
    table_column_selection: TableColumnByNameSelection,
) -> str:
    row_values = []

    for type_with_values in data_type_with_values:
        for data_value in type_with_values.raw_values:
            if table_column_selection.is_included(data_value.column_name):
                row_values.append(data_value.to_sql_as_value())

    return f"{', '.join(row_values)}"


def __create_vertical_value_rows(
    data_type_with_values: List[DataTypeWithValues],
    row_selection: DataRowSelection,
    table_column_selection: TableColumnByNameSelection,
) -> List[str]:
    """Creates table rows with the values of each type in a column. For types with fewer values, values are repeated."""
    rows = []

    value_index = 0
    while True:
        # the first column holds the row index
        row_values = [str(value_index)]
        new_value_encountered = False

        for type_with_values in data_type_with_values:
            column_name = (
                type_with_values.create_vertical_storage_column_expression().column_name
            )

            value_in_type_index = value_index
            if value_index < len(type_with_values.raw_values):
                new_value_encountered = True
            else:
                # Different types have a different number of values (= rows). After having consumed all values of a
                # type, begin repeating the values of types that have fewer values than others, but don't repeat the
                # NULL value which is known to be the first value of all types.
                null_value_offset = 1
                value_count_without_null_value = len(type_with_values.raw_values) - 1
                value_in_type_index = null_value_offset + (
                    (value_index - null_value_offset) % value_count_without_null_value
                )

            if table_column_selection.is_included(column_name):
                # only filter at this place to make sure that value repetitions occur for types with fewer values
                row_values.append(
                    type_with_values.raw_values[value_in_type_index].to_sql_as_value()
                )

        if new_value_encountered:
            if row_selection.is_included(value_index):
                rows.append(f"{', '.join(row_values)}")
        else:
            break

        value_index += 1

    return rows

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from enum import Enum
from typing import List, Optional

from materialize.output_consistency.data_type.data_type_with_values import (
    DataTypeWithValues,
)
from materialize.output_consistency.execution.value_storage_layout import (
    ROW_INDEX_COL_NAME,
    ValueStorageLayout,
)
from materialize.output_consistency.input_data.test_input_data import (
    ConsistencyTestInputData,
)
from materialize.output_consistency.selection.selection import (
    ALL_ROWS_SELECTION,
    ALL_TABLE_COLUMNS_BY_NAME_SELECTION,
    DataRowSelection,
    TableColumnByNameSelection,
)


class EvaluationStrategyKey(Enum):
    DUMMY = 1
    DATAFLOW_RENDERING = 2
    CONSTANT_FOLDING = 3


class EvaluationStrategy:
    """Strategy how to execute a `QueryTemplate`"""

    def __init__(
        self,
        identifier: EvaluationStrategyKey,
        name: str,
        object_name_base: str,
        simple_db_object_name: str,
    ):
        """
        :param identifier: identifier of this strategy
        :param name: readable name
        :param object_name_base: the db object name will be derived from this
        :param simple_db_object_name: only used by the reproduction code printer
        """
        self.identifier = identifier
        self.name = name
        self.object_name_base = object_name_base
        self.simple_db_object_name = simple_db_object_name

    def generate_sources(self, input_data: ConsistencyTestInputData) -> List[str]:
        statements = []
        statements.extend(
            self.generate_source_for_storage_layout(
                input_data,
                ValueStorageLayout.HORIZONTAL,
                ALL_ROWS_SELECTION,
                ALL_TABLE_COLUMNS_BY_NAME_SELECTION,
            )
        )
        statements.extend(
            self.generate_source_for_storage_layout(
                input_data,
                ValueStorageLayout.VERTICAL,
                ALL_ROWS_SELECTION,
                ALL_TABLE_COLUMNS_BY_NAME_SELECTION,
            )
        )
        return statements

    def generate_source_for_storage_layout(
        self,
        input_data: ConsistencyTestInputData,
        storage_layout: ValueStorageLayout,
        row_selection: DataRowSelection,
        table_column_selection: TableColumnByNameSelection,
        override_db_object_name: Optional[str] = None,
    ) -> List[str]:
        raise NotImplementedError

    def get_db_object_name(
        self,
        storage_layout: ValueStorageLayout,
        override_db_object_name: Optional[str] = None,
    ) -> str:
        if storage_layout == ValueStorageLayout.ANY:
            raise RuntimeError(f"{storage_layout} has not been resolved to a real one")

        if override_db_object_name is not None:
            return override_db_object_name

        storage_suffix = (
            "horiz" if storage_layout == ValueStorageLayout.HORIZONTAL else "vert"
        )
        return f"{self.object_name_base}_{storage_suffix}"

    def __str__(self) -> str:
        return self.name


class DummyEvaluation(EvaluationStrategy):
    def __init__(self) -> None:
        super().__init__(EvaluationStrategyKey.DUMMY, "Dummy", "<source>", "dummy")

    def generate_sources(
        self,
        input_data: ConsistencyTestInputData,
    ) -> List[str]:
        return []


class DataFlowRenderingEvaluation(EvaluationStrategy):
    def __init__(self) -> None:
        super().__init__(
            EvaluationStrategyKey.DATAFLOW_RENDERING,
            "Dataflow rendering",
            "t_dfr",
            "dataflow_rendering",
        )

    def generate_source_for_storage_layout(
        self,
        input_data: ConsistencyTestInputData,
        storage_layout: ValueStorageLayout,
        row_selection: DataRowSelection,
        table_column_selection: TableColumnByNameSelection,
        override_db_object_name: Optional[str] = None,
    ) -> List[str]:
        db_object_name = self.get_db_object_name(
            storage_layout, override_db_object_name
        )

        statements = []

        column_specs = _create_column_specs(
            input_data, storage_layout, True, table_column_selection
        )
        statements.append(f"DROP TABLE IF EXISTS {db_object_name};")
        statements.append(f"CREATE TABLE {db_object_name} ({', '.join(column_specs)});")

        value_rows = _create_value_rows(
            input_data, storage_layout, row_selection, table_column_selection
        )

        for value_row in value_rows:
            statements.append(f"INSERT INTO {db_object_name} VALUES ({value_row});")

        return statements


class ConstantFoldingEvaluation(EvaluationStrategy):
    def __init__(self) -> None:
        super().__init__(
            EvaluationStrategyKey.CONSTANT_FOLDING,
            "Constant folding",
            "v_ctf",
            "constant_folding",
        )

    def generate_source_for_storage_layout(
        self,
        input_data: ConsistencyTestInputData,
        storage_layout: ValueStorageLayout,
        row_selection: DataRowSelection,
        table_column_selection: TableColumnByNameSelection,
        override_db_object_name: Optional[str] = None,
    ) -> List[str]:
        db_object_name = self.get_db_object_name(
            storage_layout, override_db_object_name
        )

        column_specs = _create_column_specs(
            input_data, storage_layout, False, table_column_selection
        )

        value_rows = _create_value_rows(
            input_data, storage_layout, row_selection, table_column_selection
        )
        value_specification = "\n    UNION SELECT ".join(value_rows)

        create_view_statement = (
            f"CREATE OR REPLACE VIEW {db_object_name} ({', '.join(column_specs)})\n"
            f" AS SELECT {value_specification};"
        )

        return [create_view_statement]


def _create_column_specs(
    input_data: ConsistencyTestInputData,
    storage_layout: ValueStorageLayout,
    include_type: bool,
    table_column_selection: TableColumnByNameSelection,
) -> List[str]:
    column_specs = []

    # row index as first column (also for horizontal layout helpful to simplify aggregate functions with order spec)
    type_info = " INT" if include_type else ""
    column_specs.append(f"{ROW_INDEX_COL_NAME}{type_info}")

    for type_with_values in input_data.all_data_types_with_values:
        type_info = f" {type_with_values.data_type.type_name}" if include_type else ""

        if storage_layout == ValueStorageLayout.HORIZONTAL:
            for data_value in type_with_values.raw_values:
                if table_column_selection.is_included(data_value.column_name):
                    column_specs.append(f"{data_value.column_name}{type_info}")
        elif storage_layout == ValueStorageLayout.VERTICAL:
            column_name = type_with_values.create_vertical_storage_column().column_name
            if table_column_selection.is_included(column_name):
                column_specs.append(f"{column_name}{type_info}")
        else:
            raise RuntimeError(f"Unsupported storage layout: {storage_layout}")

    return column_specs


def _create_value_rows(
    input_data: ConsistencyTestInputData,
    storage_layout: ValueStorageLayout,
    row_selection: DataRowSelection,
    table_column_selection: TableColumnByNameSelection,
) -> List[str]:
    if storage_layout == ValueStorageLayout.HORIZONTAL:
        return [
            __create_horizontal_value_row(
                input_data.all_data_types_with_values, table_column_selection
            )
        ]
    elif storage_layout == ValueStorageLayout.VERTICAL:
        return __create_vertical_value_rows(
            input_data.all_data_types_with_values,
            input_data.max_value_count,
            row_selection,
            table_column_selection,
        )
    else:
        raise RuntimeError(f"Unsupported storage layout: {storage_layout}")


def __create_horizontal_value_row(
    data_type_with_values: List[DataTypeWithValues],
    table_column_selection: TableColumnByNameSelection,
) -> str:
    row_values = []

    # row index
    row_values.append("0")

    for type_with_values in data_type_with_values:
        for data_value in type_with_values.raw_values:
            if table_column_selection.is_included(data_value.column_name):
                row_values.append(data_value.to_sql_as_value())

    return f"{', '.join(row_values)}"


def __create_vertical_value_rows(
    data_type_with_values: List[DataTypeWithValues],
    row_count: int,
    row_selection: DataRowSelection,
    table_column_selection: TableColumnByNameSelection,
) -> List[str]:
    """Creates table rows with the values of each type in a column. For types with fewer values, values are repeated."""
    rows = []

    for row_index in range(0, row_count):
        # the first column holds the row index
        row_values = [str(row_index)]

        for type_with_values in data_type_with_values:
            data_column = type_with_values.create_vertical_storage_column()
            column_name = data_column.column_name

            if not table_column_selection.is_included(column_name):
                continue

            data_value = data_column.get_value_at_row(row_index)
            row_values.append(data_value.to_sql_as_value())

        if row_selection.is_included(row_index):
            rows.append(f"{', '.join(row_values)}")

    return rows

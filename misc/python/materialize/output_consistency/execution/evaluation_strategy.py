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


class EvaluationStrategy:
    """Strategy how to execute a `QueryTemplate`"""

    def __init__(self, key: str, name: str):
        self.key = key
        self.name = name

    def generate_source(
        self, data_type_with_values: List[DataTypeWithValues]
    ) -> List[str]:
        statements = []
        statements.extend(
            self._generate_source(data_type_with_values, ValueStorageLayout.HORIZONTAL)
        )
        statements.extend(
            self._generate_source(data_type_with_values, ValueStorageLayout.VERTICAL)
        )
        return statements

    def _generate_source(
        self,
        data_type_with_values: List[DataTypeWithValues],
        storage_layout: ValueStorageLayout,
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

    def generate_source(
        self, data_type_with_values: List[DataTypeWithValues]
    ) -> List[str]:
        return []


class DataFlowRenderingEvaluation(EvaluationStrategy):
    def __init__(self) -> None:
        super().__init__("t_dfr", "Dataflow rendering")

    def _generate_source(
        self,
        data_type_with_values: List[DataTypeWithValues],
        storage_layout: ValueStorageLayout,
    ) -> List[str]:
        db_object_name = self.get_db_object_name(storage_layout)
        statements = []

        column_specs = _create_column_specs(data_type_with_values, storage_layout, True)
        statements.append(f"DROP TABLE IF EXISTS {db_object_name};")
        statements.append(f"CREATE TABLE {db_object_name} ({', '.join(column_specs)});")

        value_rows = _create_value_rows(data_type_with_values, storage_layout)

        for value_row in value_rows:
            statements.append(f"INSERT INTO {db_object_name} VALUES ({value_row});")

        return statements


class ConstantFoldingEvaluation(EvaluationStrategy):
    def __init__(self) -> None:
        super().__init__("v_ctf", "Constant folding")

    def _generate_source(
        self,
        data_type_with_values: List[DataTypeWithValues],
        storage_layout: ValueStorageLayout,
    ) -> List[str]:
        column_specs = _create_column_specs(
            data_type_with_values, storage_layout, False
        )

        value_rows = _create_value_rows(data_type_with_values, storage_layout)
        value_specification = " UNION SELECT ".join(value_rows)

        create_view_statement = (
            f"CREATE OR REPLACE VIEW {self.get_db_object_name(storage_layout)} ({', '.join(column_specs)})"
            f" AS SELECT {value_specification};"
        )

        return [create_view_statement]


def _create_column_specs(
    data_type_with_values: List[DataTypeWithValues],
    storage_layout: ValueStorageLayout,
    include_type: bool,
) -> List[str]:
    column_specs = []

    if storage_layout == ValueStorageLayout.VERTICAL:
        type_info = " INT" if include_type else ""
        column_specs.append(f"{VERTICAL_LAYOUT_ROW_INDEX_COL_NAME}{type_info}")

    for type_with_values in data_type_with_values:
        type_info = f" {type_with_values.data_type.type_name}" if include_type else ""

        if storage_layout == ValueStorageLayout.HORIZONTAL:
            for data_value in type_with_values.raw_values:
                column_specs.append(f"{data_value.column_name}{type_info}")
        elif storage_layout == ValueStorageLayout.VERTICAL:
            column_specs.append(
                f"{type_with_values.vertical_storage_data_column.column_name}{type_info}"
            )
        else:
            raise RuntimeError(f"Unexpected storage layout: {storage_layout}")

    return column_specs


def _create_value_rows(
    data_type_with_values: List[DataTypeWithValues], storage_layout: ValueStorageLayout
) -> List[str]:
    if storage_layout == ValueStorageLayout.HORIZONTAL:
        return [__create_horizontal_value_row(data_type_with_values)]
    elif storage_layout == ValueStorageLayout.VERTICAL:
        return __create_vertical_value_rows(data_type_with_values)
    else:
        raise RuntimeError(f"Unexpected storage layout: {storage_layout}")


def __create_horizontal_value_row(
    data_type_with_values: List[DataTypeWithValues],
) -> str:
    row_values = []

    for type_with_values in data_type_with_values:
        for data_value in type_with_values.raw_values:
            row_values.append(data_value.to_sql_as_value())

    return f"{', '.join(row_values)}"


def __create_vertical_value_rows(
    data_type_with_values: List[DataTypeWithValues],
) -> List[str]:
    """Creates table rows with the values of each type in a column. For types with fewer values, values are repeated."""
    rows = []

    value_index = 0
    while True:
        # the first column holds the row index
        row_values = [str(value_index)]
        new_value_encountered = False

        for type_with_values in data_type_with_values:
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

            row_values.append(
                type_with_values.raw_values[value_in_type_index].to_sql_as_value()
            )

        if new_value_encountered:
            rows.append(f"{', '.join(row_values)}")
        else:
            break

        value_index += 1

    return rows

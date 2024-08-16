# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from collections.abc import Callable

from materialize.output_consistency.common import probability
from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.data_type.data_type_with_values import (
    DataTypeWithValues,
)
from materialize.output_consistency.data_value.data_value import DataValue
from materialize.output_consistency.input_data.values.all_values_provider import (
    ALL_DATA_TYPES_WITH_VALUES,
)
from materialize.output_consistency.selection.randomized_picker import RandomizedPicker


class ConsistencyTestTypesInput:
    def __init__(
        self,
    ) -> None:
        self.all_data_types_with_values: list[DataTypeWithValues] = (
            ALL_DATA_TYPES_WITH_VALUES
        )
        self.cached_max_value_count_per_table_index: dict[int | None, int] = dict()

    def remove_types(self, shall_remove: Callable[[DataType], bool]) -> None:
        self.all_data_types_with_values = [
            x for x in self.all_data_types_with_values if not shall_remove(x.data_type)
        ]

    def remove_values(self, shall_remove: Callable[[DataValue], bool]) -> None:
        for data_type_with_values in self.all_data_types_with_values:
            data_type_with_values.raw_values = [
                value
                for value in data_type_with_values.raw_values
                if not shall_remove(value)
            ]

    def get_max_value_count_of_all_types(self, table_index: int | None) -> int:
        if table_index in self.cached_max_value_count_per_table_index.keys():
            return self.cached_max_value_count_per_table_index[table_index]

        count = max(
            len(
                [
                    value
                    for value in type_with_values.raw_values
                    if table_index is None
                    or table_index in value.vertical_table_indices
                ]
            )
            for type_with_values in self.all_data_types_with_values
        )

        self.cached_max_value_count_per_table_index[table_index] = count

        return count

    def assign_columns_to_tables(
        self, vertical_tables: int, randomized_picker: RandomizedPicker
    ) -> None:
        for data_type_with_values in self.all_data_types_with_values:
            for value in data_type_with_values.raw_values:
                assigned_table_indices = self._choose_table_indices_for_value(
                    value.is_null_value, vertical_tables, randomized_picker
                )
                assert len(assigned_table_indices) > 0, "Value is in no table"
                value.vertical_table_indices.update(assigned_table_indices)

    def _choose_table_indices_for_value(
        self,
        is_null_value: bool,
        vertical_tables: int,
        randomized_picker: RandomizedPicker,
    ) -> set[int]:
        table_indices = set()

        for table_index in range(0, vertical_tables):
            is_first_table = table_index == 0

            if is_null_value:
                # always add the null value to all tables to have at least one value per table
                table_indices.add(table_index)
                continue

            # put most columns into the first table
            selected_probability = (
                probability.INCLUDE_COLUMN_IN_FIRST_TABLE
                if is_first_table
                else probability.INCLUDE_COLUMN_IN_OTHER_TABLE
            )
            if randomized_picker.random_boolean(selected_probability):
                table_indices.add(table_index)

        if len(table_indices) == 0:
            # assign the value to the first table if it has not been assigned to any table
            table_indices.add(0)

        return table_indices

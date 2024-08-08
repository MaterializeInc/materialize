# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from collections.abc import Callable

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
        self.max_value_count = self._get_max_value_count_of_all_types()

    def remove_types(self, shall_remove: Callable[[DataType], bool]) -> None:
        self.all_data_types_with_values = [
            x for x in self.all_data_types_with_values if not shall_remove(x.data_type)
        ]

        self.max_value_count = self._get_max_value_count_of_all_types()

    def remove_values(self, shall_remove: Callable[[DataValue], bool]) -> None:
        for data_type_with_values in self.all_data_types_with_values:
            data_type_with_values.raw_values = [
                value
                for value in data_type_with_values.raw_values
                if not shall_remove(value)
            ]

    def _get_max_value_count_of_all_types(self) -> int:
        return max(
            len(type_with_values.raw_values)
            for type_with_values in self.all_data_types_with_values
        )

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
            if is_null_value:
                # always add the null value to all tables to have at least one value per table
                table_indices.add(table_index)
                continue

            # put most columns into the first table (so that not all queries require a join)
            probability = 0.7 if table_index == 0 else 0.1
            if randomized_picker.random_boolean(probability):
                table_indices.add(table_index)

        if len(table_indices) == 0:
            # assign the value to the first table if it has not been assigned to any table
            table_indices.add(0)

        return table_indices

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

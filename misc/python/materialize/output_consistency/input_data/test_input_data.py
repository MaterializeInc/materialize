# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.data_type.data_type_with_values import (
    DataTypeWithValues,
)
from materialize.output_consistency.input_data.operations.all_operations_provider import (
    ALL_OPERATION_TYPES,
)
from materialize.output_consistency.input_data.values.all_values_provider import (
    ALL_DATA_TYPES_WITH_VALUES,
)
from materialize.output_consistency.operation.operation import DbOperationOrFunction


class ConsistencyTestInputData:
    """Provides input data for the test execution"""

    def __init__(
        self,
    ) -> None:
        self.all_data_types_with_values: list[
            DataTypeWithValues
        ] = ALL_DATA_TYPES_WITH_VALUES
        self.all_operation_types: list[
            DbOperationOrFunction
        ] = self._get_without_disabled_operations(ALL_OPERATION_TYPES)
        self.max_value_count = self._get_max_value_count_of_all_types()

    def _get_without_disabled_operations(
        self, operations: list[DbOperationOrFunction]
    ) -> list[DbOperationOrFunction]:
        filtered_operations = []

        for operation in operations:
            if operation.is_enabled:
                filtered_operations.append(operation)

        return filtered_operations

    def remove_postgres_incompatible_data(self) -> None:
        self._remove_postgres_incompatible_types()
        self._remove_postgres_incompatible_values()
        self._remove_postgres_incompatible_functions()

    def _remove_postgres_incompatible_types(self) -> None:
        self.all_data_types_with_values = [
            x for x in self.all_data_types_with_values if x.data_type.is_pg_compatible
        ]

        self.max_value_count = self._get_max_value_count_of_all_types()

    def _remove_postgres_incompatible_values(self) -> None:
        for data_type_with_values in self.all_data_types_with_values:
            data_type_with_values.raw_values = [
                value
                for value in data_type_with_values.raw_values
                if value.is_postgres_compatible
            ]

    def _remove_postgres_incompatible_functions(self) -> None:
        self.all_operation_types = [
            op for op in self.all_operation_types if op.is_pg_compatible
        ]

    def _get_max_value_count_of_all_types(self) -> int:
        return max(
            len(type_with_values.raw_values)
            for type_with_values in self.all_data_types_with_values
        )

    def get_stats(self) -> str:
        count_data_types = len(self.all_data_types_with_values)
        count_ops = len(self.all_operation_types)
        return (
            f"Input stats: count_data_types={count_data_types}, count_ops={count_ops}"
        )

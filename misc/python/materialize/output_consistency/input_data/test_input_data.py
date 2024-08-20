# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.input_data.test_input_operations import (
    ConsistencyTestOperationsInput,
)
from materialize.output_consistency.input_data.test_input_types import (
    ConsistencyTestTypesInput,
)
from materialize.output_consistency.query.query_template import QueryTemplate
from materialize.output_consistency.selection.randomized_picker import RandomizedPicker


class ConsistencyTestInputData:
    """Provides input data for the test execution"""

    def __init__(
        self,
    ) -> None:
        self.types_input = ConsistencyTestTypesInput()
        self.operations_input = ConsistencyTestOperationsInput()
        self.predefined_queries: list[QueryTemplate] = []

    def assign_columns_to_tables(
        self, vertical_tables: int, randomized_picker: RandomizedPicker
    ) -> None:
        self.types_input.assign_columns_to_tables(vertical_tables, randomized_picker)

    def get_stats(self) -> str:
        return (
            f"Input stats:"
            f" count_data_types={self.count_available_data_types()},"
            f" count_ops={self.count_available_ops()} (with variants {self.count_available_op_variants()}),"
            f" count_predefined_queries={self.count_predefined_queries()}"
        )

    def count_available_data_types(self) -> int:
        return len(self.types_input.all_data_types_with_values)

    def count_available_ops(self) -> int:
        return len(self.operations_input.all_operation_types)

    def count_available_op_variants(self) -> int:
        count = 0
        for operation in self.operations_input.all_operation_types:
            count = count + operation.count_variants()
        return count

    def count_predefined_queries(self) -> int:
        return len(self.predefined_queries)

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from collections.abc import Callable

from materialize.output_consistency.input_data.operations.all_operations_provider import (
    ALL_OPERATION_TYPES,
)
from materialize.output_consistency.operation.operation import DbOperationOrFunction


class ConsistencyTestOperationsInput:
    def __init__(
        self,
    ) -> None:
        self.all_operation_types: list[DbOperationOrFunction] = (
            self._get_without_disabled_operations(ALL_OPERATION_TYPES)
        )

    def _get_without_disabled_operations(
        self, operations: list[DbOperationOrFunction]
    ) -> list[DbOperationOrFunction]:
        filtered_operations = []

        for operation in operations:
            if operation.is_enabled:
                filtered_operations.append(operation)

        return filtered_operations

    def remove_functions(
        self, shall_remove: Callable[[DbOperationOrFunction], bool]
    ) -> None:
        self.all_operation_types = [
            op for op in self.all_operation_types if not shall_remove(op)
        ]

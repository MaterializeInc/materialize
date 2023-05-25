# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import Set

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.execution.value_storage_layout import (
    ValueStorageLayout,
)
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)


class Expression:
    """An expression is either a `DataValue` or a `ExpressionWithArgs`"""

    def __init__(
        self,
        characteristics: Set[ExpressionCharacteristics],
        storage_layout: ValueStorageLayout,
        is_aggregate: bool,
        is_expect_error: bool,
    ):
        self.characteristics = characteristics
        self.storage_layout = storage_layout
        self.is_aggregate = is_aggregate
        self.is_expect_error = is_expect_error

    def to_sql(self) -> str:
        raise RuntimeError("Not implemented")

    def resolve_data_type_category(self) -> DataTypeCategory:
        raise RuntimeError("Not implemented")

    def __str__(self) -> str:
        raise RuntimeError("Not implemented")

    def has_all_characteristics(
        self, characteristics: Set[ExpressionCharacteristics]
    ) -> bool:
        overlap = self.characteristics & characteristics
        return len(overlap) == len(characteristics)

    def has_any_characteristic(
        self,
        characteristics: Set[ExpressionCharacteristics],
    ) -> bool:
        overlap = self.characteristics & characteristics
        return len(overlap) > 0

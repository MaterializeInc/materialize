# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.execution.value_storage_layout import (
    ValueStorageLayout,
)
from materialize.output_consistency.expression.expression import Expression


class DataColumn(Expression):
    """A column with a value per row (in contrast to an `ExpressionWithArgs`) for VERTICAL storage"""

    def __init__(
        self,
        data_type: DataType,
    ):
        super().__init__(set(), ValueStorageLayout.VERTICAL, False, False)
        self.data_type = data_type
        self.column_name = f"{data_type.identifier.lower()}_val"

    def resolve_data_type_category(self) -> DataTypeCategory:
        return self.data_type.category

    def to_sql(self) -> str:
        return self.column_name

    def __str__(self) -> str:
        return self.to_sql()

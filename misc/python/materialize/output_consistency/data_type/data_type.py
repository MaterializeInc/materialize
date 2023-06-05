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
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec


class DataType:
    """Defines a SQL data type"""

    def __init__(self, identifier: str, type_name: str, category: DataTypeCategory):
        self.identifier = identifier
        self.type_name = type_name
        self.category = category

    def resolve_return_type_spec(
        self, characteristics: Set[ExpressionCharacteristics]
    ) -> ReturnTypeSpec:
        return ReturnTypeSpec(self.category)

    def value_to_sql(self, string_value: str) -> str:
        return f"{string_value}::{self.type_name}"

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.execution.sql_dialect_adjuster import (
    SqlDialectAdjuster,
)
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec


class DataType:
    """Defines a SQL data type"""

    def __init__(
        self,
        internal_identifier: str,
        type_name: str,
        category: DataTypeCategory,
        is_pg_compatible: bool = True,
    ):
        self.internal_identifier = internal_identifier
        self.type_name = type_name
        self.category = category
        self.is_pg_compatible = is_pg_compatible

    def resolve_return_type_spec(
        self, characteristics: set[ExpressionCharacteristics]
    ) -> ReturnTypeSpec:
        return ReturnTypeSpec(self.category)

    def get_type_name(self, sql_adjuster: SqlDialectAdjuster) -> str:
        return sql_adjuster.adjust_type(self.type_name)

    def value_to_sql(self, string_value: str, sql_adjuster: SqlDialectAdjuster) -> str:
        adjusted_string_value = sql_adjuster.adjust_value(
            string_value, self.internal_identifier, self.type_name
        )
        return f"{adjusted_string_value}::{self.get_type_name(sql_adjuster)}"

    def __str__(self) -> str:
        return self.type_name

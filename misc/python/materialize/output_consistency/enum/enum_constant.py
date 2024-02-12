# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from __future__ import annotations

from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.enum.enum_data_type import EnumDataType
from materialize.output_consistency.execution.sql_dialect_adjuster import (
    SqlDialectAdjuster,
)
from materialize.output_consistency.execution.value_storage_layout import (
    ValueStorageLayout,
)
from materialize.output_consistency.expression.expression import (
    Expression,
    LeafExpression,
)
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec
from materialize.output_consistency.selection.selection import DataRowSelection

ENUM_RETURN_TYPE_SPEC = ReturnTypeSpec(DataTypeCategory.ENUM)


class EnumConstant(Expression):
    """A constant SQL value"""

    def __init__(
        self,
        value: str,
        add_quotes: bool,
        characteristics: set[ExpressionCharacteristics],
    ):
        super().__init__(characteristics, ValueStorageLayout.ANY, False, False)
        self.value = value
        self.add_quotes = add_quotes
        self.data_type = EnumDataType()

    def resolve_return_type_category(self) -> DataTypeCategory:
        return DataTypeCategory.ENUM

    def resolve_return_type_spec(self) -> ReturnTypeSpec:
        return ENUM_RETURN_TYPE_SPEC

    def try_resolve_exact_data_type(self) -> DataType | None:
        return self.data_type

    def is_leaf(self) -> bool:
        return True

    def contains_leaf_not_directly_consumed_by_aggregation(self) -> bool:
        return False

    def recursively_collect_involved_characteristics(
        self, row_selection: DataRowSelection
    ) -> set[ExpressionCharacteristics]:
        return self.own_characteristics

    def __str__(self) -> str:
        return self.to_sql(SqlDialectAdjuster(), False)

    def to_sql(self, sql_adjuster: SqlDialectAdjuster, is_root_level: bool) -> str:
        sql_value = self.data_type.value_to_sql(self.value, sql_adjuster)
        if self.add_quotes:
            return f"'{sql_value}'"

        return sql_value

    def collect_leaves(self) -> list[LeafExpression]:
        return []

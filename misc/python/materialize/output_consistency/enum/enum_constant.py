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
from materialize.output_consistency.selection.row_selection import DataRowSelection
from materialize.util import stable_int_hash

ENUM_RETURN_TYPE_SPEC = ReturnTypeSpec(DataTypeCategory.ENUM)


class EnumConstant(Expression):
    """A constant SQL value"""

    def __init__(
        self,
        value: str,
        add_quotes: bool,
        characteristics: set[ExpressionCharacteristics],
        tags: set[str] | None = None,
    ):
        super().__init__(characteristics, ValueStorageLayout.ANY, False, False)
        self.value = value
        self.add_quotes = add_quotes
        self.data_type = EnumDataType()
        self.tags = tags

    def hash(self) -> int:
        return stable_int_hash(self.value)

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
        return f"EnumConstant (value={self.value})"

    def to_sql(
        self, sql_adjuster: SqlDialectAdjuster, include_alias: bool, is_root_level: bool
    ) -> str:
        sql_value = self.data_type.value_to_sql(self.value, sql_adjuster)
        if self.add_quotes:
            return f"'{sql_value}'"

        return sql_value

    def collect_leaves(self) -> list[LeafExpression]:
        return []

    def collect_vertical_table_indices(self) -> set[int]:
        return set()

    def is_tagged(self, tag: str) -> bool:
        if self.tags is None:
            return False

        return tag in self.tags


class StringConstant(EnumConstant):
    def __init__(
        self,
        value: str,
        characteristics: set[ExpressionCharacteristics] = set(),
    ):
        super().__init__(value, add_quotes=True, characteristics=characteristics)

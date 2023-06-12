# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from __future__ import annotations

from typing import List, Optional, Set

from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.execution.value_storage_layout import (
    ValueStorageLayout,
)
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec
from materialize.output_consistency.selection.selection import DataRowSelection


class Expression:
    """An expression is either a `LeafExpression` or a `ExpressionWithArgs`"""

    def __init__(
        self,
        characteristics: Set[ExpressionCharacteristics],
        storage_layout: ValueStorageLayout,
        is_aggregate: bool,
        is_expect_error: bool,
    ):
        # own characteristics without the ones of children
        self.own_characteristics = characteristics
        self.storage_layout = storage_layout
        self.is_aggregate = is_aggregate
        self.is_expect_error = is_expect_error

    def to_sql(self, is_root_level: bool) -> str:
        raise NotImplementedError

    def resolve_return_type_spec(self) -> ReturnTypeSpec:
        raise NotImplementedError

    def resolve_return_type_category(self) -> DataTypeCategory:
        raise NotImplementedError

    def try_resolve_exact_data_type(self) -> Optional[DataType]:
        raise NotImplementedError

    def recursively_collect_involved_characteristics(
        self, row_selection: DataRowSelection
    ) -> Set[ExpressionCharacteristics]:
        """Get all involved characteristics through recursion"""
        raise NotImplementedError

    def collect_leaves(self) -> List[LeafExpression]:
        raise NotImplementedError

    def __str__(self) -> str:
        raise NotImplementedError

    def has_all_characteristics(
        self, characteristics: Set[ExpressionCharacteristics]
    ) -> bool:
        """True if this expression itself exhibits all characteristics. Child expressions are not considered."""
        overlap = self.own_characteristics & characteristics
        return len(overlap) == len(characteristics)

    def has_any_characteristic(
        self,
        characteristics: Set[ExpressionCharacteristics],
    ) -> bool:
        """True if this expression itself exhibits any of the characteristics. Child expressions are not considered."""
        overlap = self.own_characteristics & characteristics
        return len(overlap) > 0

    def is_leaf(self) -> bool:
        raise NotImplementedError

    def contains_leaf_not_directly_consumed_by_aggregation(self) -> bool:
        """
        True if any leaf is not directly consumed by an aggregation,
        hence false if all leaves of this expression are directly consumed by an aggregation.
        This is relevant because when using non-aggregate functions on multiple rows, different evaluation strategies may yield different error messages due to a different row processing order."""
        raise NotImplementedError


class LeafExpression(Expression):
    def __init__(
        self,
        column_name: str,
        data_type: DataType,
        characteristics: Set[ExpressionCharacteristics],
        storage_layout: ValueStorageLayout,
        is_aggregate: bool,
        is_expect_error: bool,
    ):
        super().__init__(characteristics, storage_layout, is_aggregate, is_expect_error)
        self.column_name = column_name
        self.data_type = data_type

    def resolve_data_type_category(self) -> DataTypeCategory:
        return self.data_type.category

    def try_resolve_exact_data_type(self) -> Optional[DataType]:
        return self.data_type

    def to_sql(self, is_root_level: bool) -> str:
        return self.to_sql_as_column()

    def to_sql_as_column(self) -> str:
        return self.column_name

    def collect_leaves(self) -> List[LeafExpression]:
        return [self]

    def is_leaf(self) -> bool:
        return True

    def contains_leaf_not_directly_consumed_by_aggregation(self) -> bool:
        # This is not decided at leaf level.
        return False

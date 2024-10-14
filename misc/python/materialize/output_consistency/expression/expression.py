# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from __future__ import annotations

from collections.abc import Callable

from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.data_value.source_column_identifier import (
    SourceColumnIdentifier,
)
from materialize.output_consistency.execution.sql_dialect_adjuster import (
    SqlDialectAdjuster,
)
from materialize.output_consistency.execution.value_storage_layout import (
    ValueStorageLayout,
)
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec
from materialize.output_consistency.query.data_source import DataSource
from materialize.output_consistency.selection.row_selection import (
    ALL_ROWS_SELECTION,
    DataRowSelection,
)
from materialize.util import stable_int_hash


class Expression:
    """An expression is either a `LeafExpression` or a `ExpressionWithArgs`"""

    def __init__(
        self,
        characteristics: set[ExpressionCharacteristics],
        storage_layout: ValueStorageLayout,
        is_aggregate: bool,
        is_expect_error: bool,
    ):
        # own characteristics without the ones of children
        self.own_characteristics = characteristics
        self.storage_layout = storage_layout
        self.is_aggregate = is_aggregate
        self.is_expect_error = is_expect_error
        self.is_shared = False

    def to_sql(
        self, sql_adjuster: SqlDialectAdjuster, include_alias: bool, is_root_level: bool
    ) -> str:
        raise NotImplementedError

    def hash(self) -> int:
        """
        The primary purpose of this method is to allow conditional breakpoints when debugging.
        """
        raise NotImplementedError

    def resolve_return_type_spec(self) -> ReturnTypeSpec:
        """
        Ignore filters should favor #resolve_return_type_category over this method whenever possible because that method
        resolves dynamic types.
        :return: the return type spec
        """
        raise NotImplementedError

    def resolve_return_type_category(self) -> DataTypeCategory:
        """
        :return: the data type category of this value
        """
        raise NotImplementedError

    def resolve_resulting_return_type_category(self) -> DataTypeCategory:
        """
        :return: the data type category that the use of this value will lead to
        """
        return self.resolve_return_type_category()

    def try_resolve_exact_data_type(self) -> DataType | None:
        raise NotImplementedError

    def recursively_collect_involved_characteristics(
        self, row_selection: DataRowSelection
    ) -> set[ExpressionCharacteristics]:
        """Get all involved characteristics through recursion"""
        raise NotImplementedError

    def collect_leaves(self) -> list[LeafExpression]:
        raise NotImplementedError

    def collect_data_sources(self) -> list[DataSource]:
        data_sources = []
        for leaf in self.collect_leaves():
            data_source = leaf.get_data_source()
            if data_source is not None:
                data_sources.append(data_source)

        return data_sources

    def collect_vertical_table_indices(self) -> set[int]:
        raise NotImplementedError

    def __str__(self) -> str:
        raise NotImplementedError

    def has_all_characteristics(
        self,
        characteristics: set[ExpressionCharacteristics],
        recursive: bool = True,
        row_selection: DataRowSelection = ALL_ROWS_SELECTION,
    ) -> bool:
        """True if this expression itself exhibits all characteristics."""
        present_characteristics = (
            self.own_characteristics
            if not recursive
            else self.recursively_collect_involved_characteristics(row_selection)
        )
        overlap = present_characteristics & characteristics
        return len(overlap) == len(characteristics)

    def has_any_characteristic(
        self,
        characteristics: set[ExpressionCharacteristics],
        recursive: bool = True,
        row_selection: DataRowSelection = ALL_ROWS_SELECTION,
    ) -> bool:
        """True if this expression itself exhibits any of the characteristics."""
        present_characteristics = (
            self.own_characteristics
            if not recursive
            else self.recursively_collect_involved_characteristics(row_selection)
        )
        overlap = present_characteristics & characteristics
        return len(overlap) > 0

    def is_leaf(self) -> bool:
        raise NotImplementedError

    def contains_leaf_not_directly_consumed_by_aggregation(self) -> bool:
        """
        True if any leaf is not directly consumed by an aggregation,
        hence false if all leaves of this expression are directly consumed by an aggregation.
        This is relevant because when using non-aggregate functions on multiple rows, different evaluation strategies may yield different error messages due to a different row processing order.
        """
        raise NotImplementedError

    def matches(
        self, predicate: Callable[[Expression], bool], apply_recursively: bool
    ) -> bool:
        # recursion is implemented in ExpressionWithArgs
        return predicate(self)

    def contains(
        self, predicate: Callable[[Expression], bool], check_recursively: bool
    ) -> bool:
        return self.matches(predicate, check_recursively)

    def recursively_mark_as_shared(self) -> None:
        """
        Mark that this expression is used multiple times within a query.
        All instances will use the same data source.
        """
        self.is_shared = True


class LeafExpression(Expression):
    def __init__(
        self,
        column_name: str,
        data_type: DataType,
        characteristics: set[ExpressionCharacteristics],
        storage_layout: ValueStorageLayout,
        data_source: DataSource | None,
        is_aggregate: bool = False,
        is_expect_error: bool = False,
    ):
        super().__init__(characteristics, storage_layout, is_aggregate, is_expect_error)
        self.column_name = column_name
        self.data_type = data_type
        self.data_source = data_source

    def hash(self) -> int:
        return stable_int_hash(self.column_name)

    def resolve_data_type_category(self) -> DataTypeCategory:
        return self.data_type.category

    def try_resolve_exact_data_type(self) -> DataType | None:
        return self.data_type

    def to_sql(
        self, sql_adjuster: SqlDialectAdjuster, include_alias: bool, is_root_level: bool
    ) -> str:
        return self.to_sql_as_column(
            sql_adjuster, include_alias, self.column_name, self.get_data_source()
        )

    def to_sql_as_column(
        self,
        sql_adjuster: SqlDialectAdjuster,
        include_alias: bool,
        column_name: str,
        data_source: DataSource | None,
    ) -> str:
        if include_alias:
            assert data_source is not None, "data source is None"
            return f"{data_source.alias()}.{column_name}"
        return column_name

    def collect_leaves(self) -> list[LeafExpression]:
        return [self]

    def is_leaf(self) -> bool:
        return True

    def contains_leaf_not_directly_consumed_by_aggregation(self) -> bool:
        # This is not decided at leaf level.
        return False

    def recursively_collect_involved_characteristics(
        self, row_selection: DataRowSelection
    ) -> set[ExpressionCharacteristics]:
        return self.own_characteristics

    def get_data_source(self) -> DataSource | None:
        return self.data_source

    def get_source_column_identifier(self) -> SourceColumnIdentifier | None:
        data_source = self.get_data_source()

        if data_source is None:
            return None

        return SourceColumnIdentifier(
            data_source_alias=data_source.alias(),
            column_name=self.column_name,
        )

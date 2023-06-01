# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import List, Optional, Set

from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
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
from materialize.output_consistency.operation.operation import (
    EXPRESSION_PLACEHOLDER,
    DbOperationOrFunction,
)
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec
from materialize.output_consistency.selection.selection import DataRowSelection


class ExpressionWithArgs(Expression):
    """An expression representing a usage of a database operation or function"""

    def __init__(
        self,
        operation: DbOperationOrFunction,
        args: List[Expression],
        is_aggregate: bool,
        is_expect_error: bool,
    ):
        super().__init__(
            operation.derive_characteristics(args),
            _determine_storage_layout(args),
            is_aggregate,
            is_expect_error,
        )
        self.operation = operation
        self.pattern = operation.to_pattern(len(args))
        self.return_type_spec = operation.return_type_spec
        self.args = args

    def has_args(self) -> bool:
        return len(self.args) > 0

    def to_sql(self, is_root_level: bool) -> str:
        sql: str = self.pattern

        for arg in self.args:
            sql = sql.replace(EXPRESSION_PLACEHOLDER, arg.to_sql(False), 1)

        if len(self.args) != self.pattern.count(EXPRESSION_PLACEHOLDER):
            raise RuntimeError(
                f"Not enough arguments to fill all placeholders in pattern {self.pattern}"
            )

        if (
            is_root_level
            and self.resolve_return_type_category() == DataTypeCategory.DATE_TIME
        ):
            # workaround because the max date type in python is smaller than values supported by mz
            sql = f"({sql})::TEXT"

        return sql

    def resolve_return_type_spec(self) -> ReturnTypeSpec:
        return self.return_type_spec

    def resolve_return_type_category(self) -> DataTypeCategory:
        first_arg_type_category_hint = None

        if self.return_type_spec.type_category == DataTypeCategory.DYNAMIC:
            # Only compute the hint for this category
            first_arg_type_category_hint = (
                self.args[0].resolve_return_type_category() if self.has_args() else None
            )

        return self.return_type_spec.resolve_type_category(first_arg_type_category_hint)

    def try_resolve_exact_data_type(self) -> Optional[DataType]:
        return self.operation.try_resolve_exact_data_type(self.args)

    def __str__(self) -> str:
        args_desc = ", ".join(arg.__str__() for arg in self.args)
        return f"ExpressionWithArgs with pattern {self.pattern} and args {args_desc}"

    def recursively_collect_involved_characteristics(
        self, row_selection: DataRowSelection
    ) -> Set[ExpressionCharacteristics]:
        involved_characteristics: Set[ExpressionCharacteristics] = set()
        involved_characteristics = involved_characteristics.union(
            self.own_characteristics
        )

        for arg in self.args:
            involved_characteristics = involved_characteristics.union(
                arg.recursively_collect_involved_characteristics(row_selection)
            )

        return involved_characteristics

    def collect_leaves(self) -> List[LeafExpression]:
        leaves = []

        for arg in self.args:
            leaves.extend(arg.collect_leaves())

        return leaves

    def is_leaf(self) -> bool:
        return False

    def contains_leaf_not_directly_consumed_by_aggregation(self) -> bool:
        for arg in self.args:
            if arg.is_leaf() and not self.is_aggregate:
                return True
            elif (
                not arg.is_leaf()
                and arg.contains_leaf_not_directly_consumed_by_aggregation()
            ):
                return True

        return False


def _determine_storage_layout(args: List[Expression]) -> ValueStorageLayout:
    storage_layout: Optional[ValueStorageLayout] = None

    for arg in args:
        if arg.storage_layout == ValueStorageLayout.ANY:
            continue

        if storage_layout is None:
            storage_layout = arg.storage_layout
        elif storage_layout != arg.storage_layout:
            raise RuntimeError(
                f"It is not allowed to mix storage layouts in an expression (current={storage_layout}, got={arg.storage_layout})"
            )

    if storage_layout is None:
        # use this as default (but it should not matter as expressions are expected to always have at least one arg)
        return ValueStorageLayout.HORIZONTAL

    return storage_layout

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from collections.abc import Callable

from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
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
from materialize.output_consistency.operation.operation import (
    EXPRESSION_PLACEHOLDER,
    DbOperationOrFunction,
)
from materialize.output_consistency.operation.return_type_spec import (
    InputArgTypeHints,
    ReturnTypeSpec,
)
from materialize.output_consistency.selection.row_selection import DataRowSelection
from materialize.util import stable_int_hash


class ExpressionWithArgs(Expression):
    """An expression representing a usage of a database operation or function"""

    def __init__(
        self,
        operation: DbOperationOrFunction,
        args: list[Expression],
        is_aggregate: bool = False,
        is_expect_error: bool = False,
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

    def hash(self) -> int:
        return stable_int_hash(
            self.operation.to_pattern(self.count_args()),
            *[str(arg.hash()) for arg in self.args],
        )

    def count_args(self) -> int:
        return len(self.args)

    def has_args(self) -> bool:
        return self.count_args() > 0

    def to_sql(
        self, sql_adjuster: SqlDialectAdjuster, include_alias: bool, is_root_level: bool
    ) -> str:
        sql: str = self.pattern

        for arg in self.args:
            sql = sql.replace(
                EXPRESSION_PLACEHOLDER,
                arg.to_sql(sql_adjuster, include_alias, False),
                1,
            )

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
        input_type_hints = InputArgTypeHints()

        if self.return_type_spec.indices_of_required_input_type_hints is not None:
            # provide input types that are required as hints to determine the output type
            for arg_index in self.return_type_spec.indices_of_required_input_type_hints:
                assert (
                    0 <= arg_index <= len(self.args)
                ), f"Invalid requested index: {arg_index} as hint for {self.operation}"
                input_type_hints.type_category_of_requested_args[arg_index] = self.args[
                    arg_index
                ].resolve_resulting_return_type_category()

                if self.return_type_spec.requires_return_type_spec_hints:
                    input_type_hints.return_type_spec_of_requested_args[arg_index] = (
                        self.args[arg_index].resolve_return_type_spec()
                    )

        return self.return_type_spec.resolve_type_category(input_type_hints)

    def try_resolve_exact_data_type(self) -> DataType | None:
        return self.operation.try_resolve_exact_data_type(self.args)

    def __str__(self) -> str:
        args_desc = ", ".join(arg.__str__() for arg in self.args)
        return f"ExpressionWithArgs (pattern='{self.pattern}', args=[{args_desc}])"

    def recursively_collect_involved_characteristics(
        self, row_selection: DataRowSelection
    ) -> set[ExpressionCharacteristics]:
        involved_characteristics: set[ExpressionCharacteristics] = set()
        involved_characteristics = involved_characteristics.union(
            self.own_characteristics
        )

        for arg in self.args:
            involved_characteristics = involved_characteristics.union(
                arg.recursively_collect_involved_characteristics(row_selection)
            )

        return involved_characteristics

    def collect_leaves(self) -> list[LeafExpression]:
        leaves = []

        for arg in self.args:
            leaves.extend(arg.collect_leaves())

        return leaves

    def collect_vertical_table_indices(self) -> set[int]:
        vertical_table_indices = set()

        for arg in self.args:
            vertical_table_indices.update(arg.collect_vertical_table_indices())

        return vertical_table_indices

    def is_leaf(self) -> bool:
        return False

    def matches(
        self, predicate: Callable[[Expression], bool], apply_recursively: bool
    ) -> bool:
        if super().matches(predicate, apply_recursively):
            return True

        if apply_recursively:
            for arg in self.args:
                if arg.matches(predicate, apply_recursively):
                    return True

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

    def operation_to_pattern(self) -> str:
        return self.operation.to_pattern(self.count_args())

    def recursively_mark_as_shared(self) -> None:
        super().recursively_mark_as_shared()

        for arg in self.args:
            arg.recursively_mark_as_shared()


def _determine_storage_layout(args: list[Expression]) -> ValueStorageLayout:
    mutual_storage_layout: ValueStorageLayout | None = None

    for arg in args:
        if (
            mutual_storage_layout is None
            or mutual_storage_layout == ValueStorageLayout.ANY
        ):
            mutual_storage_layout = arg.storage_layout
        elif arg.storage_layout == ValueStorageLayout.ANY:
            continue
        elif mutual_storage_layout != arg.storage_layout:
            raise RuntimeError(
                f"It is not allowed to mix storage layouts in an expression (current={mutual_storage_layout}, got={arg.storage_layout})"
            )

    if mutual_storage_layout is None:
        # use this as default (in case there are no args)
        return ValueStorageLayout.ANY

    return mutual_storage_layout

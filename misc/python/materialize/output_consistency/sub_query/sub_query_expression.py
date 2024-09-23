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


class SubQuery(Expression):
    """A sub query"""

    def __init__(
        self,
        subquery_expression: Expression,
        storage_layout: ValueStorageLayout,
        characteristics: set[ExpressionCharacteristics],
        is_aggregate: bool,
        alias: str | None = None,
        where_filter: Expression | None = None,
        limit: int | None = None,
    ):
        super().__init__(
            characteristics,
            storage_layout,
            is_aggregate,
            False,
        )
        self.subquery_expression = subquery_expression
        self.alias = alias
        self.where_filter = where_filter
        self.limit = limit

    def to_sql(
        self, sql_adjuster: SqlDialectAdjuster, include_alias: bool, is_root_level: bool
    ) -> str:
        space_separator = " "
        db_object_name = evaluation_strategy.get_db_object_name(
            self.storage_layout,
        )
        alias_spec = f" {self.alias}" if self.alias is not None else ""
        where_spec = (
            f"\nWHERE {self.where_filter.to_sql(sql_adjuster=sql_adjuster, is_root_level=True, include_alias=include_alias)}"
            if self.where_filter is not None
            else ""
        )
        limit_spec = f"\nLIMIT {self.limit}" if self.limit is not None else ""

        sql = f"""
(SELECT{space_separator}{self.subquery_expression.to_sql(sql_adjuster=sql_adjuster, is_root_level=True, include_alias=include_alias)}
FROM {db_object_name}{alias_spec}{where_spec}{limit_spec})
""".strip()
        return sql

    def resolve_return_type_spec(self) -> ReturnTypeSpec:
        return self.subquery_expression.resolve_return_type_spec()

    def resolve_return_type_category(self) -> DataTypeCategory:
        return self.subquery_expression.resolve_return_type_category()

    def try_resolve_exact_data_type(self) -> DataType | None:
        return self.subquery_expression.try_resolve_exact_data_type()

    def recursively_collect_involved_characteristics(
        self, row_selection: DataRowSelection
    ) -> set[ExpressionCharacteristics]:
        return self.subquery_expression.recursively_collect_involved_characteristics(
            row_selection
        )

    def collect_leaves(self) -> list[LeafExpression]:
        return self.subquery_expression.collect_leaves()

    def __str__(self) -> str:
        return f"SubQuery (expression={self.subquery_expression})"

    def is_leaf(self) -> bool:
        return False

    def contains_leaf_not_directly_consumed_by_aggregation(self) -> bool:
        return self.subquery_expression.contains_leaf_not_directly_consumed_by_aggregation()

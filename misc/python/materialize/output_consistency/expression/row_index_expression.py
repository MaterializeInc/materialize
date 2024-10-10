# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from __future__ import annotations

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
from materialize.output_consistency.input_data.types.number_types_provider import (
    INT8_TYPE,
)
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec
from materialize.output_consistency.query.data_source import DataSource


class RowIndexExpression(LeafExpression):

    def __init__(
        self, expression_to_share_data_source: Expression, allow_multi_columns: bool
    ):
        super().__init__(
            column_name="row_index",
            data_type=INT8_TYPE,
            characteristics=set(),
            storage_layout=ValueStorageLayout.ANY,
        )
        self.expression_to_share_data_source = expression_to_share_data_source
        self.allow_multi_columns = allow_multi_columns

    def resolve_return_type_spec(self) -> ReturnTypeSpec:
        return self.data_type.resolve_return_type_spec(self.own_characteristics)

    def resolve_return_type_category(self) -> DataTypeCategory:
        return self.data_type.category

    def get_data_source(self) -> DataSource | None:
        data_sources = self.expression_to_share_data_source.collect_data_sources()

        if len(data_sources) == 0:
            # this happens when the expression is a constant
            return None

        # we can only return one data source here but that does not really matter because we only reuse already used
        # data sources
        return data_sources[0]

    def to_sql(
        self, sql_adjuster: SqlDialectAdjuster, include_alias: bool, is_root_level: bool
    ) -> str:
        data_sources = self.expression_to_share_data_source.collect_data_sources()

        if len(data_sources) == 0:
            # We won't use row_index in this case but a constant instead to avoid a potentially ambiguous column
            # reference
            return "0"

        if not self.allow_multi_columns:
            # only pick one data source in this case
            data_sources = data_sources[:1]

        expressions = []
        for data_source in data_sources:
            expressions.append(
                super().to_sql_as_column(sql_adjuster, include_alias, data_source)
            )

        return ", ".join(expressions)

    def collect_vertical_table_indices(self) -> set[int]:
        # not relevant because this is already handled by the column sharing the data source
        return set()

    def __str__(self) -> str:
        return f"RowIndexExpression (expression_to_share_data_source={self.expression_to_share_data_source})"

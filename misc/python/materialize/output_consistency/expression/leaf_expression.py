# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import List, Set

from materialize.output_consistency.execution.value_storage_layout import (
    ValueStorageLayout,
)
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)


class LeafExpression(Expression):
    def __init__(
        self,
        column_name: str,
        characteristics: Set[ExpressionCharacteristics],
        storage_layout: ValueStorageLayout,
        is_aggregate: bool,
        is_expect_error: bool,
    ):
        super().__init__(characteristics, storage_layout, is_aggregate, is_expect_error)
        self.column_name = column_name

    def to_sql(self) -> str:
        return self.to_sql_as_column()

    def to_sql_as_column(self) -> str:
        return self.column_name

    def collect_leaves(self) -> List[Expression]:
        return [self]

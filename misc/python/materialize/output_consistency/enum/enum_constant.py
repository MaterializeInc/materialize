# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from __future__ import annotations

from materialize.output_consistency.enum.enum_data_type import EnumDataType
from materialize.output_consistency.expression.constant_expression import (
    ConstantExpression,
)
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.util import stable_int_hash

ENUM_DATA_TYPE = EnumDataType()


class EnumConstant(ConstantExpression):
    """A constant SQL value"""

    def __init__(
        self,
        value: str,
        add_quotes: bool,
        characteristics: set[ExpressionCharacteristics],
        tags: set[str] | None = None,
    ):
        super().__init__(
            value, ENUM_DATA_TYPE, add_quotes, characteristics, is_aggregate=False
        )
        self.tags = tags

    def hash(self) -> int:
        return stable_int_hash(self.value)

    def __str__(self) -> str:
        return f"EnumConstant (value={self.value})"

    def is_tagged(self, tag: str) -> bool:
        if self.tags is None:
            return False

        return tag in self.tags

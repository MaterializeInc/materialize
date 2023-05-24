# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import List

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.operation.operation import (
    EXPRESSION_PLACEHOLDER,
    DbOperationOrFunction,
)


class ExpressionWithArgs(Expression):
    """An expression representing a usage of a database operation or function"""

    def __init__(
        self,
        operation: DbOperationOrFunction,
        args: List[Expression],
        is_expect_error: bool,
    ):
        super().__init__(operation.derive_characteristics(args), is_expect_error)
        self.pattern = operation.to_pattern(len(args))
        self.return_type_category = operation.return_type_category
        self.args = args

    def to_sql(self) -> str:
        sql: str = self.pattern

        for arg in self.args:
            sql = sql.replace(EXPRESSION_PLACEHOLDER, arg.to_sql(), 1)

        if len(self.args) != self.pattern.count(EXPRESSION_PLACEHOLDER):
            raise RuntimeError(
                f"Not enough arguments to fill all placeholders in pattern {self.pattern}"
            )

        return sql

    def resolve_data_type_category(self) -> DataTypeCategory:
        if self.return_type_category == DataTypeCategory.DYNAMIC:
            if len(self.args) == 0:
                raise RuntimeError(
                    f"Expression {self.pattern} uses {DataTypeCategory.ANY} as return type, which is not allowed"
                )

        if self.return_type_category == DataTypeCategory.DYNAMIC:
            if len(self.args) == 0:
                raise RuntimeError(
                    f"Expression {self.pattern} uses return {DataTypeCategory.DYNAMIC} as return type but has no "
                    "arguments"
                )
            else:
                return self.args[0].resolve_data_type_category()

        return self.return_type_category

    def __str__(self) -> str:
        args_desc = ", ".join(arg.__str__() for arg in self.args)
        return f"ExpressionWithArgs with pattern {self.pattern} and args {args_desc}"

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.execution.evaluation_strategy import (
    EvaluationStrategy,
)
from materialize.output_consistency.expressions.expression import Expression


class QueryTemplate:
    def __init__(self) -> None:
        self.select_expressions: list[Expression] = []

    def add_select_exp(self, expr: Expression) -> None:
        self.select_expressions.append(expr)

    def to_sql(self, strategy: EvaluationStrategy) -> str:
        expressions_as_sql = [expr.to_sql() for expr in self.select_expressions]
        return f"""
            SELECT {', '.join(expressions_as_sql)}
            FROM {strategy.db_object_name};""".strip()

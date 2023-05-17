# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.output_consistency.common.configuration import (
    ConsistencyTestConfiguration,
)
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.query.query_template import QueryTemplate


class QueryGenerator:
    """Generates query templates based on expressions"""

    def __init__(self, config: ConsistencyTestConfiguration):
        self.config = config
        self.current_expressions: list[Expression] = []
        self.has_expression_expecting_error = True

    def push_expression(self, expression: Expression) -> None:
        self.current_expressions.append(expression)

        if expression.is_expect_error:
            self.has_expression_expecting_error = True

    def can_consume_query(self) -> bool:
        return len(self.current_expressions) > 0

    def shall_consume_query(self) -> bool:
        if not self.can_consume_query():
            return False

        if self.has_expression_expecting_error:
            # create single-column queries when an error is expected
            return True

        return len(self.current_expressions) >= self.config.max_cols_per_query

    def consume_query(self) -> QueryTemplate:
        if len(self.current_expressions) == 0:
            raise RuntimeError("No expressions present")

        query = QueryTemplate()
        query.add_multiple_select_expressions(self.current_expressions)

        self.reset_state()

        return query

    def reset_state(self) -> None:
        self.current_expressions = []
        self.has_expression_expecting_error = False

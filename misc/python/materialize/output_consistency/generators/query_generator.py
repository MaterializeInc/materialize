# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import List

from materialize.output_consistency.common.configuration import (
    ConsistencyTestConfiguration,
)
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.query.query_template import QueryTemplate


class QueryGenerator:
    """Generates query templates based on expressions"""

    def __init__(self, config: ConsistencyTestConfiguration):
        self.config = config
        # put all of these expressions into a single query
        self.current_presumably_succeeding_expressions: List[Expression] = []
        # put these expressions in separate queries
        self.current_presumably_failing_expressions: List[Expression] = []

    def push_expression(self, expression: Expression) -> None:
        if expression.is_expect_error:
            self.current_presumably_failing_expressions.append(expression)
        else:
            self.current_presumably_succeeding_expressions.append(expression)

    def shall_consume_queries(self) -> bool:
        return (
            len(self.current_presumably_succeeding_expressions)
            >= self.config.max_cols_per_query
        )

    def consume_queries(self) -> List[QueryTemplate]:
        queries = []

        if len(self.current_presumably_succeeding_expressions) > 0:
            # one query for all presumably succeeding expressions
            queries.append(
                QueryTemplate(False, self.current_presumably_succeeding_expressions)
            )

        for failing_expression in self.current_presumably_failing_expressions:
            # one query for each failing expression
            queries.append(QueryTemplate(True, [failing_expression]))

        self.reset_state()

        return queries

    def reset_state(self) -> None:
        self.current_presumably_succeeding_expressions = []
        self.current_presumably_failing_expressions = []

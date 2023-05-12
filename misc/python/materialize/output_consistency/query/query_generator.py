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
from materialize.output_consistency.expressions.expression import Expression
from materialize.output_consistency.query.query_template import QueryTemplate


class QueryGenerator:
    def __init__(self, config: ConsistencyTestConfiguration):
        self.config = config

    def generate_queries(self, expressions: list[Expression]) -> list[QueryTemplate]:
        if len(expressions) == 0:
            return []

        queries: list[QueryTemplate] = []

        current_query = QueryTemplate()
        for index, expr in enumerate(expressions):
            if index % self.config.max_cols_per_query == 0 and index > 0:
                queries.append(current_query)
                current_query = QueryTemplate()

            current_query.add_select_exp(expr)

        if len(current_query.select_expressions) > 0:
            queries.append(current_query)

        return queries

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.expression.expression_with_args import (
    ExpressionWithArgs,
)
from materialize.output_consistency.expression.leaf_expression import LeafExpression
from materialize.output_consistency.operation.operation import DbFunction
from materialize.output_consistency.selection.selection import DataRowSelection


class KnownOutputInconsistenciesFilter:
    """Allows specifying and excluding expressions with known output inconsistencies"""

    def matches(self, expression: Expression, row_selection: DataRowSelection) -> bool:
        if isinstance(expression, LeafExpression):
            return False
        elif isinstance(expression, ExpressionWithArgs):
            if not expression.operation.is_aggregation:
                # Optimization: currently no issues without aggregation are known
                return False

            return self._matches_expression_with_args(expression, row_selection)
        else:
            raise RuntimeError(f"Unsupported expression type: {type(expression)}")

    def _matches_expression_with_args(
        self,
        expression: ExpressionWithArgs,
        row_selection: DataRowSelection,
    ) -> bool:
        # check if any argument is already problematic (relevant for nested expressions)
        for arg in expression.args:
            if isinstance(arg, ExpressionWithArgs):
                if self.matches(arg, row_selection):
                    return True

        if isinstance(expression.operation, DbFunction):
            return self._matches_db_function(
                expression, expression.operation, row_selection
            )

        return False

    def _matches_db_function(
        self,
        expression: ExpressionWithArgs,
        db_function: DbFunction,
        row_selection: DataRowSelection,
    ) -> bool:
        for arg in expression.args:
            expression_characteristics = arg.collect_involved_characteristics(
                row_selection
            )

            # Note that function names will always be provided in lower case.
            if db_function.function_name in {"sum", "avg", "stddev_samp", "stddev_pop"}:
                if ExpressionCharacteristics.MAX_VALUE in expression_characteristics:
                    return True

        return False

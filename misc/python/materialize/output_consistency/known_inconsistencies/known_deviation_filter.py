# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import Set

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
            return self._matches_expression_with_args(expression, row_selection)
        else:
            raise RuntimeError(f"Unsupported expression type: {type(expression)}")

    def _matches_expression_with_args(
        self,
        expression: ExpressionWithArgs,
        row_selection: DataRowSelection,
    ) -> bool:
        # check expression itself
        if self._matches_expression_with_args_no_recursion(expression, row_selection):
            return True

        # recursively check arguments
        for arg in expression.args:
            if self.matches(arg, row_selection):
                return True

        return False

    def _matches_expression_with_args_no_recursion(
        self,
        expression: ExpressionWithArgs,
        row_selection: DataRowSelection,
    ) -> bool:
        if not expression.operation.is_aggregation:
            # currently no issues without aggregation are known
            return False

        if isinstance(expression.operation, DbFunction):
            # currently only issues with functions are known, therefore ignore operations
            db_function = expression.operation
            expression_characteristics = (
                expression.recursively_collect_involved_characteristics(row_selection)
            )

            if self._matches_problematic_function_invocation(
                db_function, expression_characteristics
            ):
                return True

        return False

    def _matches_problematic_function_invocation(
        self,
        db_function: DbFunction,
        all_involved_characteristics: Set[ExpressionCharacteristics],
    ) -> bool:
        # Note that function names are always provided in lower case.
        if db_function.function_name in {"sum", "avg", "stddev_samp", "stddev_pop"}:
            if ExpressionCharacteristics.MAX_VALUE in all_involved_characteristics:
                # tracked with https://github.com/MaterializeInc/materialize/issues/19511
                return True

        return False

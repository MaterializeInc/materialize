# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Optional, Set

from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_with_args import (
    ExpressionWithArgs,
)


class KnownOutputInconsistenciesFilter:
    """Allows specifying and excluding expressions with known output inconsistencies"""

    def matches(
        self, expression: Expression, restriction_to_row_indices: Optional[Set[int]]
    ) -> bool:
        if isinstance(expression, ExpressionWithArgs):
            return self._matches_expression_with_args(
                expression, restriction_to_row_indices
            )
        return False

    def _matches_expression_with_args(
        self,
        expression: ExpressionWithArgs,
        restriction_to_row_indices: Optional[Set[int]],
    ) -> bool:
        return False

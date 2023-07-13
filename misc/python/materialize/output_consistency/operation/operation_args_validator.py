# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Callable, List, Optional, Set

from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)


class OperationArgsValidator:
    """Validator that performs heuristic checks to determine if a database error is to be expected"""

    def is_expected_to_cause_error(self, args: List[Expression]) -> bool:
        raise NotImplementedError

    def index_of(
        self,
        args: List[Expression],
        match_argument_fn: Callable[
            [Expression, Set[ExpressionCharacteristics], int], bool
        ],
        skip_argument_indices: Optional[Set[int]] = None,
    ) -> int:
        if skip_argument_indices is None:
            skip_argument_indices = set()

        for index, arg in enumerate(args):
            if index in skip_argument_indices:
                continue

            if match_argument_fn(arg, arg.own_characteristics, index):
                return index

        return -1

    def index_of_characteristic_combination(
        self,
        args: List[Expression],
        characteristic_combination: Set[ExpressionCharacteristics],
        skip_argument_indices: Optional[Set[int]] = None,
    ) -> int:
        def match_fn(
            _arg: Expression,
            arg_characteristics: Set[ExpressionCharacteristics],
            _index: int,
        ) -> bool:
            return len(characteristic_combination & arg_characteristics) == len(
                characteristic_combination
            )

        return self.index_of(args, match_fn, skip_argument_indices)

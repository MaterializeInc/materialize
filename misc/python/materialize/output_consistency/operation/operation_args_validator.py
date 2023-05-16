# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Callable, Optional

from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)


class OperationArgsValidator:
    def is_expected_to_cause_error(self, args: list[Expression]) -> bool:
        raise RuntimeError("Not implemented")

    def has_all_characteristics(
        self,
        arg: Expression,
        characteristics: set[ExpressionCharacteristics],
    ) -> bool:
        overlap = arg.characteristics & characteristics
        return len(overlap) == len(characteristics)

    def index_of_characteristic(
        self,
        args: list[Expression],
        characteristic: ExpressionCharacteristics,
        skip_argument_indices: Optional[set[int]] = None,
        skip_argument_fn: Callable[
            [set[ExpressionCharacteristics], int], bool
        ] = lambda chars, index: False,
    ) -> int:
        if skip_argument_indices is None:
            skip_argument_indices = set()

        for index, arg in enumerate(args):
            if index in skip_argument_indices:
                continue

            if skip_argument_fn(arg.characteristics, index):
                continue

            if characteristic in arg.characteristics:
                return index

        return -1

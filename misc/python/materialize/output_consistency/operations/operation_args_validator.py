# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Callable

from materialize.output_consistency.data_type.value_characteristics import (
    ValueCharacteristics,
)
from materialize.output_consistency.expressions.expression import Expression


class OperationArgsValidator:
    def is_expected_to_cause_error(self, args: list[Expression]) -> bool:
        raise RuntimeError("Not implemented")


class ValueGrowsArgsValidator(OperationArgsValidator):
    def is_expected_to_cause_error(self, args: list[Expression]) -> bool:
        index_of_max_value = index_of_characteristic(
            args, ValueCharacteristics.MAX_VALUE
        )

        if index_of_max_value == -1:
            return False

        index_of_further_inc_value = index_of_characteristic(
            args,
            ValueCharacteristics.NON_EMPTY,
            lambda chars, index: index != index_of_max_value,
        )

        return index_of_further_inc_value >= 0


def index_of_characteristic(
    args: list[Expression],
    characteristic: ValueCharacteristics,
    skip_argument: Callable[
        [set[ValueCharacteristics], int], bool
    ] = lambda chars, index: True,
) -> int:
    for index, arg in enumerate(args):
        if skip_argument(arg.characteristics, index):
            continue

        if characteristic in arg.characteristics:
            return index

    return -1

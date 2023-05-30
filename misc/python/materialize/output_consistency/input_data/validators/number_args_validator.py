# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import List

from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.operation.operation_args_validator import (
    OperationArgsValidator,
)


class Uint8MixedWithTypedArgsValidator(OperationArgsValidator):
    """To identify expressions that contain a value of UINT8 type and a value of an unsigned numeric type (heuristic to identify no matching operators)"""

    def is_expected_to_cause_error(self, args: List[Expression]) -> bool:
        if len(args) < 2:
            return False

        index_of_uint8 = self.index_of_characteristic_combination(
            args,
            {
                ExpressionCharacteristics.TYPE_INT8_SIZED,
                ExpressionCharacteristics.UNSIGNED_TYPED,
            },
        )

        index_of_signed = self.index_of(
            args,
            lambda arg, arg_characteristics, index: ExpressionCharacteristics.UNSIGNED_TYPED
            not in arg_characteristics,
        )

        return index_of_uint8 != -1 and index_of_signed != -1


class SingleParamValueGrowsArgsValidator(OperationArgsValidator):
    """To identify single-value expressions holding a large value (basic heuristic to predict numeric overflows)"""

    def is_expected_to_cause_error(self, args: List[Expression]) -> bool:
        return args[0].has_any_characteristic(
            {
                ExpressionCharacteristics.LARGE_VALUE,
                ExpressionCharacteristics.MAX_VALUE,
            },
        )


class MultiParamValueGrowsArgsValidator(OperationArgsValidator):
    """To identify expressions that contain a MAX_VALUE and another non-empty value (basic heuristic to predict numeric overflows)"""

    def is_expected_to_cause_error(self, args: List[Expression]) -> bool:
        index_of_max_value = self.index_of_characteristic_combination(
            args, {ExpressionCharacteristics.MAX_VALUE}
        )

        if index_of_max_value == -1:
            return False

        index_of_further_inc_value = self.index_of_characteristic_combination(
            args,
            {ExpressionCharacteristics.NON_EMPTY},
            skip_argument_indices={index_of_max_value},
        )

        return index_of_further_inc_value >= 0


class MaxMinusNegMaxArgsValidator(OperationArgsValidator):
    """To identify expressions that contain a MAX_VALUE and a negative MAX_VALUE (basic heuristic to predict numeric overflows)"""

    def is_expected_to_cause_error(self, args: List[Expression]) -> bool:
        if len(args) != 2:
            return False

        return args[0].has_all_characteristics(
            {ExpressionCharacteristics.MAX_VALUE}
        ) and args[1].has_all_characteristics(
            {ExpressionCharacteristics.MAX_VALUE, ExpressionCharacteristics.NEGATIVE},
        )

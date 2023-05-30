# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import Callable, List, Set

from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.input_data.types.number_types_provider import (
    UINT8_TYPE,
    NumberDataType,
)
from materialize.output_consistency.operation.operation_args_validator import (
    OperationArgsValidator,
)


class NumericArgsValidator(OperationArgsValidator):
    def index_of_type_with_properties(
        self,
        args: List[Expression],
        match_type_properties: Callable[[NumberDataType], bool],
    ) -> int:
        def has_numeric_type_properties(
            arg: Expression,
            _arg_characteristics: Set[ExpressionCharacteristics],
            _index: int,
        ) -> bool:
            data_type = arg.try_resolve_exact_data_type()

            if data_type is not None and isinstance(data_type, NumberDataType):
                return match_type_properties(data_type)
            return False

        return self.index_of(args, has_numeric_type_properties)


class Uint8MixedWithTypedArgsValidator(NumericArgsValidator):
    """To identify expressions that contain a value of UINT8 type and a value of an unsigned numeric type (heuristic to identify no matching operators)"""

    def is_expected_to_cause_error(self, args: List[Expression]) -> bool:
        if len(args) < 2:
            return False

        index_of_uint8 = self.index_of_type_with_properties(
            args, lambda number_type: number_type == UINT8_TYPE
        )
        index_of_signed = self.index_of_type_with_properties(
            args, lambda number_type: number_type.is_signed
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

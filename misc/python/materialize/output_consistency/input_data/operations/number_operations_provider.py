# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List

from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.input_data.params.number_operation_param import (
    NumericOperationParam,
)
from materialize.output_consistency.input_data.return_specs.number_return_spec import (
    NumericReturnTypeSpec,
)
from materialize.output_consistency.input_data.validators.number_args_validator import (
    MaxMinusNegMaxArgsValidator,
    MultiParamValueGrowsArgsValidator,
    SingleParamValueGrowsArgsValidator,
    Uint8MixedWithTypedArgsValidator,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperation,
    DbOperationOrFunction,
    OperationRelevance,
)

NUMERIC_OPERATION_TYPES: List[DbOperationOrFunction] = []

NUMERIC_OPERATION_TYPES.append(
    DbOperation(
        "$ + $",
        [NumericOperationParam(), NumericOperationParam()],
        NumericReturnTypeSpec(),
        {MultiParamValueGrowsArgsValidator()},
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbOperation(
        "$ - $",
        [NumericOperationParam(), NumericOperationParam()],
        NumericReturnTypeSpec(),
        {MaxMinusNegMaxArgsValidator()},
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbOperation(
        "$ * $",
        [NumericOperationParam(), NumericOperationParam()],
        NumericReturnTypeSpec(),
        {MultiParamValueGrowsArgsValidator()},
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbOperation(
        "$ / $",
        [
            NumericOperationParam(),
            NumericOperationParam(incompatibilities={ExpressionCharacteristics.ZERO}),
        ],
        NumericReturnTypeSpec(),
        relevance=OperationRelevance.HIGH,
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbOperation(
        "$ % $",
        [
            NumericOperationParam(),
            NumericOperationParam(incompatibilities={ExpressionCharacteristics.ZERO}),
        ],
        NumericReturnTypeSpec(),
        relevance=OperationRelevance.HIGH,
    )
)
# Bitwise AND
NUMERIC_OPERATION_TYPES.append(
    DbOperation(
        "$ & $",
        [
            NumericOperationParam(only_int_type=True),
            NumericOperationParam(only_int_type=True),
        ],
        NumericReturnTypeSpec(),
        {Uint8MixedWithTypedArgsValidator()},
        OperationRelevance.LOW,
    )
)
# Bitwise OR
NUMERIC_OPERATION_TYPES.append(
    DbOperation(
        "$ | $",
        [
            NumericOperationParam(only_int_type=True),
            NumericOperationParam(only_int_type=True),
        ],
        NumericReturnTypeSpec(),
        {Uint8MixedWithTypedArgsValidator()},
        OperationRelevance.LOW,
    )
)
# Bitwise XOR
NUMERIC_OPERATION_TYPES.append(
    DbOperation(
        "$ # $",
        [
            NumericOperationParam(only_int_type=True),
            NumericOperationParam(only_int_type=True),
        ],
        NumericReturnTypeSpec(),
        {Uint8MixedWithTypedArgsValidator()},
        OperationRelevance.LOW,
    )
)
# Bitwise NOT
NUMERIC_OPERATION_TYPES.append(
    DbOperation(
        "~$",
        [
            NumericOperationParam(only_int_type=True),
        ],
        NumericReturnTypeSpec(),
        relevance=OperationRelevance.LOW,
    )
)
# Bitwise left shift
NUMERIC_OPERATION_TYPES.append(
    DbOperation(
        "$ << $",
        [
            NumericOperationParam(only_int_type=True),
            NumericOperationParam(only_int_type=True, no_int_type_larger_int4=True),
        ],
        NumericReturnTypeSpec(),
        {Uint8MixedWithTypedArgsValidator()},
        OperationRelevance.LOW,
    )
)
# Bitwise right shift
NUMERIC_OPERATION_TYPES.append(
    DbOperation(
        "$ >> $",
        [
            NumericOperationParam(only_int_type=True),
            NumericOperationParam(only_int_type=True, no_int_type_larger_int4=True),
        ],
        NumericReturnTypeSpec(),
        {Uint8MixedWithTypedArgsValidator()},
        OperationRelevance.LOW,
    )
)

# ===== END NUMBER OPERATORS =====

# ===== BEGIN NUMBER FUNCTIONS =====

NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "ABS",
        [NumericOperationParam()],
        NumericReturnTypeSpec(),
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "CBRT",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
# CEIL == CEILING
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "CEIL",
        [NumericOperationParam()],
        NumericReturnTypeSpec(only_integer=True),
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "EXP",
        [NumericOperationParam()],
        NumericReturnTypeSpec(),
        {SingleParamValueGrowsArgsValidator()},
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "FLOOR",
        [NumericOperationParam()],
        NumericReturnTypeSpec(only_integer=True),
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "LN",
        [
            NumericOperationParam(
                incompatibilities={
                    ExpressionCharacteristics.NEGATIVE,
                    ExpressionCharacteristics.ZERO,
                }
            )
        ],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "LOG10",
        [
            NumericOperationParam(
                incompatibilities={
                    ExpressionCharacteristics.NEGATIVE,
                    ExpressionCharacteristics.ZERO,
                }
            )
        ],
        NumericReturnTypeSpec(),
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "LOG",
        [
            # first param is the base
            NumericOperationParam(
                only_int_type=True,
                # simplification: float would work if this was the only param
                no_floating_point_type=True,
                incompatibilities={
                    ExpressionCharacteristics.NEGATIVE,
                    ExpressionCharacteristics.ZERO,
                    ExpressionCharacteristics.ONE,
                },
            ),
            # not marked as optional because if not present the operation is equal to LOG10, which is separate
            NumericOperationParam(
                only_int_type=True,
                no_floating_point_type=True,
                incompatibilities={
                    ExpressionCharacteristics.NEGATIVE,
                    ExpressionCharacteristics.ZERO,
                },
            ),
        ],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "MOD",
        [
            NumericOperationParam(),
            NumericOperationParam(incompatibilities={ExpressionCharacteristics.ZERO}),
        ],
        NumericReturnTypeSpec(),
    )
)
# POW == POWER
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "POW",
        [
            NumericOperationParam(),
            NumericOperationParam(
                incompatibilities={ExpressionCharacteristics.MAX_VALUE}
            ),
        ],
        NumericReturnTypeSpec(),
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "ROUND",
        [
            NumericOperationParam(),
            # negative values are allowed
            NumericOperationParam(
                optional=True,
                only_int_type=True,
                no_int_type_larger_int4=True,
                incompatibilities={
                    ExpressionCharacteristics.LARGE_VALUE,
                },
            ),
        ],
        NumericReturnTypeSpec(),
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "SQRT",
        [NumericOperationParam(incompatibilities={ExpressionCharacteristics.NEGATIVE})],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "TRUNC",
        [NumericOperationParam()],
        # Unlike one might expect, this is not guaranteed to return an integer. Postgres allows specifying the precision.
        NumericReturnTypeSpec(),
    )
)

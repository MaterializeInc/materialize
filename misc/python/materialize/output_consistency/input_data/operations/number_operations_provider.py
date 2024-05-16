# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


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

TAG_BITWISE_OP = "bitwise"

NUMERIC_OPERATION_TYPES: list[DbOperationOrFunction] = []

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
        relevance=OperationRelevance.LOW,
        tags={TAG_BITWISE_OP},
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
        relevance=OperationRelevance.LOW,
        tags={TAG_BITWISE_OP},
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
        relevance=OperationRelevance.LOW,
        tags={TAG_BITWISE_OP},
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
        tags={TAG_BITWISE_OP},
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
        relevance=OperationRelevance.LOW,
        tags={TAG_BITWISE_OP},
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
        relevance=OperationRelevance.LOW,
        tags={TAG_BITWISE_OP},
    )
)

# ===== END NUMBER OPERATORS =====

# ===== BEGIN NUMBER FUNCTIONS =====

NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "abs",
        [NumericOperationParam()],
        NumericReturnTypeSpec(),
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "cbrt",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
# CEIL == CEILING
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "ceil",
        [NumericOperationParam()],
        NumericReturnTypeSpec(only_integer=True),
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "exp",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
        {SingleParamValueGrowsArgsValidator()},
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "floor",
        [NumericOperationParam()],
        NumericReturnTypeSpec(only_integer=True),
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "ln",
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
        "log10",
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
        "log",
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
        "mod",
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
        "pow",
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
        "round",
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
        "sqrt",
        [NumericOperationParam(incompatibilities={ExpressionCharacteristics.NEGATIVE})],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "trunc",
        [NumericOperationParam()],
        # Unlike one might expect, this is not guaranteed to return an integer. Postgres allows specifying the precision.
        NumericReturnTypeSpec(),
    )
)

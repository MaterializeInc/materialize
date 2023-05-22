# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import List

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.input_data.params.number_operation_param import (
    NumericOperationParam,
)
from materialize.output_consistency.input_data.validators.number_args_validator import (
    MaxMinusNegMaxArgsValidator,
    MultiParamValueGrowsArgsValidator,
    SingleParamValueGrowsArgsValidator,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperation,
    DbOperationOrFunction,
)

NUMERIC_OPERATION_TYPES: List[DbOperationOrFunction] = []

# ===== BEGIN NUMBER OPERATORS =====

NUMERIC_OPERATION_TYPES.append(
    DbOperation(
        "$ + $",
        [NumericOperationParam(), NumericOperationParam()],
        DataTypeCategory.NUMERIC,
        {MultiParamValueGrowsArgsValidator()},
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbOperation(
        "$ - $",
        [NumericOperationParam(), NumericOperationParam()],
        DataTypeCategory.NUMERIC,
        {MaxMinusNegMaxArgsValidator()},
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbOperation(
        "$ * $",
        [NumericOperationParam(), NumericOperationParam()],
        DataTypeCategory.NUMERIC,
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
        DataTypeCategory.NUMERIC,
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbOperation(
        "$ % $",
        [
            NumericOperationParam(),
            NumericOperationParam(incompatibilities={ExpressionCharacteristics.ZERO}),
        ],
        DataTypeCategory.NUMERIC,
    )
)
# Bitwise AND
NUMERIC_OPERATION_TYPES.append(
    DbOperation(
        "$ & $",
        [
            NumericOperationParam(
                incompatibilities={ExpressionCharacteristics.DECIMAL_OR_FLOAT_TYPED}
            ),
            NumericOperationParam(
                incompatibilities={ExpressionCharacteristics.DECIMAL_OR_FLOAT_TYPED}
            ),
        ],
        DataTypeCategory.NUMERIC,
    )
)
# Bitwise OR
NUMERIC_OPERATION_TYPES.append(
    DbOperation(
        "$ | $",
        [
            NumericOperationParam(
                incompatibilities={ExpressionCharacteristics.DECIMAL_OR_FLOAT_TYPED}
            ),
            NumericOperationParam(
                incompatibilities={ExpressionCharacteristics.DECIMAL_OR_FLOAT_TYPED}
            ),
        ],
        DataTypeCategory.NUMERIC,
    )
)
# Bitwise XOR
NUMERIC_OPERATION_TYPES.append(
    DbOperation(
        "$ # $",
        [
            NumericOperationParam(
                incompatibilities={ExpressionCharacteristics.DECIMAL_OR_FLOAT_TYPED}
            ),
            NumericOperationParam(
                incompatibilities={ExpressionCharacteristics.DECIMAL_OR_FLOAT_TYPED}
            ),
        ],
        DataTypeCategory.NUMERIC,
    )
)
# Bitwise NOT
NUMERIC_OPERATION_TYPES.append(
    DbOperation(
        "~$",
        [
            NumericOperationParam(
                incompatibilities={ExpressionCharacteristics.DECIMAL_OR_FLOAT_TYPED}
            )
        ],
        DataTypeCategory.NUMERIC,
    )
)
# Bitwise left shift
NUMERIC_OPERATION_TYPES.append(
    DbOperation(
        "$ << $",
        [
            NumericOperationParam(
                incompatibilities={ExpressionCharacteristics.DECIMAL_OR_FLOAT_TYPED}
            ),
            NumericOperationParam(
                incompatibilities={
                    ExpressionCharacteristics.DECIMAL_OR_FLOAT_TYPED,
                    ExpressionCharacteristics.LARGER_THAN_INT4_TYPED,
                }
            ),
        ],
        DataTypeCategory.NUMERIC,
    )
)
# Bitwise right shift
NUMERIC_OPERATION_TYPES.append(
    DbOperation(
        "$ >> $",
        [
            NumericOperationParam(
                incompatibilities={ExpressionCharacteristics.DECIMAL_OR_FLOAT_TYPED}
            ),
            NumericOperationParam(
                incompatibilities={
                    ExpressionCharacteristics.DECIMAL_OR_FLOAT_TYPED,
                    ExpressionCharacteristics.LARGER_THAN_INT4_TYPED,
                }
            ),
        ],
        DataTypeCategory.NUMERIC,
    )
)

# ===== END NUMBER OPERATORS =====

# ===== BEGIN NUMBER FUNCTIONS =====

NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "ABS",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "CBRT",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)
# CEIL == CEILING
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "CEIL",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "EXP",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
        {SingleParamValueGrowsArgsValidator()},
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "FLOOR",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
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
        DataTypeCategory.NUMERIC,
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
        DataTypeCategory.NUMERIC,
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "LOG",
        [
            # first param is the base
            NumericOperationParam(
                incompatibilities={
                    ExpressionCharacteristics.NEGATIVE,
                    ExpressionCharacteristics.ZERO,
                    ExpressionCharacteristics.ONE,
                    ExpressionCharacteristics.FLOAT_TYPED,
                }
            ),
            # not marked as optional because if not present the operation is equal to LOG10, which is separate
            NumericOperationParam(
                incompatibilities={
                    ExpressionCharacteristics.NEGATIVE,
                    ExpressionCharacteristics.ZERO,
                    ExpressionCharacteristics.FLOAT_TYPED,
                }
            ),
        ],
        DataTypeCategory.NUMERIC,
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "MOD",
        [
            NumericOperationParam(),
            NumericOperationParam(incompatibilities={ExpressionCharacteristics.ZERO}),
        ],
        DataTypeCategory.NUMERIC,
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
        DataTypeCategory.NUMERIC,
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
                incompatibilities={
                    ExpressionCharacteristics.DECIMAL_OR_FLOAT_TYPED,
                    ExpressionCharacteristics.LARGER_THAN_INT4_TYPED,
                    ExpressionCharacteristics.LARGE_VALUE,
                },
            ),
        ],
        DataTypeCategory.NUMERIC,
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "SQRT",
        [NumericOperationParam(incompatibilities={ExpressionCharacteristics.NEGATIVE})],
        DataTypeCategory.NUMERIC,
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "TRUNC",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)

# ===== END NUMBER FUNCTIONS =====

# ===== BEGIN TRIGONOMETRIC =====
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "COS",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)
# only for numbers [-1, +1]
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "ACOS",
        [
            NumericOperationParam(
                incompatibilities={
                    ExpressionCharacteristics.LARGE_VALUE,
                }
            )
        ],
        DataTypeCategory.NUMERIC,
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "COSH",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
        {SingleParamValueGrowsArgsValidator()},
    )
)
# only for numbers [1,)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "ACOSH",
        [
            NumericOperationParam(
                incompatibilities={
                    ExpressionCharacteristics.ZERO,
                    ExpressionCharacteristics.NEGATIVE,
                },
                incompatibility_combinations=[
                    {
                        ExpressionCharacteristics.DECIMAL,
                        ExpressionCharacteristics.TINY_VALUE,
                    }
                ],
            )
        ],
        DataTypeCategory.NUMERIC,
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction("COT", [NumericOperationParam()], DataTypeCategory.NUMERIC)
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction("SIN", [NumericOperationParam()], DataTypeCategory.NUMERIC)
)
# only for numbers [-1, +1]
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "ASIN",
        [
            NumericOperationParam(
                incompatibilities={
                    ExpressionCharacteristics.LARGE_VALUE,
                }
            )
        ],
        DataTypeCategory.NUMERIC,
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "SINH",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
        {SingleParamValueGrowsArgsValidator()},
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "ASINH",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction("TAN", [NumericOperationParam()], DataTypeCategory.NUMERIC)
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "ATAN",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "TANH",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)
# only for numbers [-1, +1]
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "ATANH",
        [
            NumericOperationParam(
                incompatibilities={
                    ExpressionCharacteristics.LARGE_VALUE,
                }
            )
        ],
        DataTypeCategory.NUMERIC,
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "RADIANS",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)
NUMERIC_OPERATION_TYPES.append(
    DbFunction(
        "DEGREES",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)

# ===== END TRIGONOMETRIC =====

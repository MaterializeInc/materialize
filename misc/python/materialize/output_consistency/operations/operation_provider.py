# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.data_type.value_characteristics import (
    ValueCharacteristics,
)
from materialize.output_consistency.operations.operation import (
    BinaryFunction,
    OperationWithNParams,
    OperationWithOneParam,
    OperationWithTwoParams,
    UnaryFunction,
)
from materialize.output_consistency.operations.operation_args_validator import (
    ValueGrowsArgsValidator,
)
from materialize.output_consistency.operations.operation_param import (
    NumericOperationParam,
)

OPERATION_TYPES: list[OperationWithNParams] = []

# ===== BEGIN GENERIC =====

OPERATION_TYPES.append(
    BinaryFunction(
        "GREATEST",
        NumericOperationParam(),
        NumericOperationParam(),
        DataTypeCategory.DYNAMIC,
    )
)
OPERATION_TYPES.append(
    BinaryFunction(
        "LEAST",
        NumericOperationParam(),
        NumericOperationParam(),
        DataTypeCategory.DYNAMIC,
    )
)
OPERATION_TYPES.append(
    OperationWithNParams(
        "GREATEST($, $, $)",
        [NumericOperationParam(), NumericOperationParam(), NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    OperationWithNParams(
        "LEAST($, $, $)",
        [NumericOperationParam(), NumericOperationParam(), NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)

# ===== END GENERIC =====

# ===== BEGIN NUMBER OPERATORS =====

OPERATION_TYPES.append(
    OperationWithTwoParams(
        "$ + $",
        NumericOperationParam(),
        NumericOperationParam(),
        DataTypeCategory.NUMERIC,
        {ValueGrowsArgsValidator()},
        commutative=True,
    )
)
OPERATION_TYPES.append(
    OperationWithTwoParams(
        "$ - $",
        NumericOperationParam(),
        NumericOperationParam(),
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    OperationWithTwoParams(
        "$ * $",
        NumericOperationParam(),
        NumericOperationParam(),
        DataTypeCategory.NUMERIC,
        {ValueGrowsArgsValidator()},
        commutative=True,
    )
)
OPERATION_TYPES.append(
    OperationWithTwoParams(
        "$ / $",
        NumericOperationParam(),
        NumericOperationParam(incompatibilities={ValueCharacteristics.ZERO}),
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    OperationWithTwoParams(
        "$ % $",
        NumericOperationParam(),
        NumericOperationParam(incompatibilities={ValueCharacteristics.ZERO}),
        DataTypeCategory.NUMERIC,
    )
)
# Bitwise AND
OPERATION_TYPES.append(
    OperationWithTwoParams(
        "$ & $",
        NumericOperationParam(incompatibilities={ValueCharacteristics.DECIMAL}),
        NumericOperationParam(incompatibilities={ValueCharacteristics.DECIMAL}),
        DataTypeCategory.NUMERIC,
    )
)
# Bitwise OR
OPERATION_TYPES.append(
    OperationWithTwoParams(
        "$ | $",
        NumericOperationParam(incompatibilities={ValueCharacteristics.DECIMAL}),
        NumericOperationParam(incompatibilities={ValueCharacteristics.DECIMAL}),
        DataTypeCategory.NUMERIC,
    )
)
# Bitwise XOR
OPERATION_TYPES.append(
    OperationWithTwoParams(
        "$ # $",
        NumericOperationParam(incompatibilities={ValueCharacteristics.DECIMAL}),
        NumericOperationParam(incompatibilities={ValueCharacteristics.DECIMAL}),
        DataTypeCategory.NUMERIC,
    )
)
# Bitwise NOT
OPERATION_TYPES.append(
    OperationWithOneParam(
        "~$",
        NumericOperationParam(incompatibilities={ValueCharacteristics.DECIMAL}),
        DataTypeCategory.NUMERIC,
    )
)
# Bitwise left shift
OPERATION_TYPES.append(
    OperationWithTwoParams(
        "$ << $",
        NumericOperationParam(incompatibilities={ValueCharacteristics.DECIMAL}),
        NumericOperationParam(incompatibilities={ValueCharacteristics.DECIMAL}),
        DataTypeCategory.NUMERIC,
    )
)
# Bitwise right shift
OPERATION_TYPES.append(
    OperationWithTwoParams(
        "$ >> $",
        NumericOperationParam(incompatibilities={ValueCharacteristics.DECIMAL}),
        NumericOperationParam(incompatibilities={ValueCharacteristics.DECIMAL}),
        DataTypeCategory.NUMERIC,
    )
)

# ===== END NUMBER OPERATORS =====

# ===== BEGIN AGGREGATES =====

OPERATION_TYPES.append(
    UnaryFunction("SUM", NumericOperationParam(), DataTypeCategory.NUMERIC)
)
OPERATION_TYPES.append(
    UnaryFunction("MIN", NumericOperationParam(), DataTypeCategory.NUMERIC)
)
OPERATION_TYPES.append(
    UnaryFunction("MAX", NumericOperationParam(), DataTypeCategory.NUMERIC)
)

# ===== END AGGREGATES =====

# ===== BEGIN NUMBERS =====

OPERATION_TYPES.append(
    UnaryFunction(
        "ABS",
        NumericOperationParam(),
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    UnaryFunction(
        "CBRT",
        NumericOperationParam(),
        DataTypeCategory.NUMERIC,
    )
)
# CEIL == CEILING
OPERATION_TYPES.append(
    UnaryFunction(
        "CEIL",
        NumericOperationParam(),
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    UnaryFunction(
        "EXP",
        NumericOperationParam(),
        DataTypeCategory.NUMERIC,
        {ValueGrowsArgsValidator()},
    )
)
OPERATION_TYPES.append(
    UnaryFunction(
        "FLOOR",
        NumericOperationParam(),
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    UnaryFunction(
        "LN",
        NumericOperationParam(
            incompatibilities={ValueCharacteristics.NEGATIVE, ValueCharacteristics.ZERO}
        ),
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    UnaryFunction(
        "LOG10",
        NumericOperationParam(
            incompatibilities={ValueCharacteristics.NEGATIVE, ValueCharacteristics.ZERO}
        ),
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    BinaryFunction(
        "LOG",
        NumericOperationParam(
            incompatibilities={ValueCharacteristics.NEGATIVE, ValueCharacteristics.ZERO}
        ),
        # do not mark this param as optional because if not present the operation is equal to LOG10, which is separate
        NumericOperationParam(
            incompatibilities={ValueCharacteristics.NEGATIVE, ValueCharacteristics.ZERO}
        ),
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    BinaryFunction(
        "MOD",
        NumericOperationParam(),
        NumericOperationParam(incompatibilities={ValueCharacteristics.ZERO}),
        DataTypeCategory.NUMERIC,
    )
)
# POW == POWER
OPERATION_TYPES.append(
    BinaryFunction(
        "POW",
        NumericOperationParam(),
        NumericOperationParam(),
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    BinaryFunction(
        "ROUND",
        NumericOperationParam(),
        # negative values are allowed
        NumericOperationParam(
            optional=True, incompatibilities={ValueCharacteristics.DECIMAL}
        ),
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    UnaryFunction(
        "SQRT",
        NumericOperationParam(incompatibilities={ValueCharacteristics.NEGATIVE}),
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    UnaryFunction(
        "TRUNC",
        NumericOperationParam(),
        DataTypeCategory.NUMERIC,
    )
)

# ===== END NUMERS =====

# ===== BEGIN TRIGONOMETRIC =====
OPERATION_TYPES.append(
    UnaryFunction(
        "COS",
        NumericOperationParam(),
        DataTypeCategory.NUMERIC,
    )
)
# only for numbers [-1, +1]
OPERATION_TYPES.append(
    UnaryFunction(
        "ACOS",
        NumericOperationParam(
            incompatibilities={
                ValueCharacteristics.LARGE_VALUE,
                ValueCharacteristics.MAX_VALUE,
            }
        ),
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    UnaryFunction(
        "COSH",
        NumericOperationParam(),
        DataTypeCategory.NUMERIC,
        {ValueGrowsArgsValidator()},
    )
)
# only for numbers [1,)
OPERATION_TYPES.append(
    UnaryFunction(
        "ACOSH",
        NumericOperationParam(
            incompatibilities={ValueCharacteristics.ZERO, ValueCharacteristics.NEGATIVE}
        ),
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    UnaryFunction("COT", NumericOperationParam(), DataTypeCategory.NUMERIC)
)
OPERATION_TYPES.append(
    UnaryFunction("SIN", NumericOperationParam(), DataTypeCategory.NUMERIC)
)
# only for numbers [-1, +1]
OPERATION_TYPES.append(
    UnaryFunction(
        "ASIN",
        NumericOperationParam(
            incompatibilities={
                ValueCharacteristics.LARGE_VALUE,
                ValueCharacteristics.MAX_VALUE,
            }
        ),
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    UnaryFunction(
        "SINH",
        NumericOperationParam(),
        DataTypeCategory.NUMERIC,
        {ValueGrowsArgsValidator()},
    )
)
OPERATION_TYPES.append(
    UnaryFunction(
        "ASINH",
        NumericOperationParam(),
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    UnaryFunction("TAN", NumericOperationParam(), DataTypeCategory.NUMERIC)
)
OPERATION_TYPES.append(
    UnaryFunction(
        "ATAN",
        NumericOperationParam(),
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    UnaryFunction(
        "TANH",
        NumericOperationParam(),
        DataTypeCategory.NUMERIC,
    )
)
# only for numbers [-1, +1]
OPERATION_TYPES.append(
    UnaryFunction(
        "ATANH",
        NumericOperationParam(
            incompatibilities={
                ValueCharacteristics.LARGE_VALUE,
                ValueCharacteristics.MAX_VALUE,
            }
        ),
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    UnaryFunction(
        "RADIANS",
        NumericOperationParam(),
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    UnaryFunction(
        "DEGREES",
        NumericOperationParam(),
        DataTypeCategory.NUMERIC,
    )
)

# ===== END TRIGONOMETRIC =====

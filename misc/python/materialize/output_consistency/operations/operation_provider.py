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
from materialize.output_consistency.operations.operation import DbFunction, DbOperation
from materialize.output_consistency.operations.operation_args_validator import (
    ValueGrowsArgsValidator,
)
from materialize.output_consistency.operations.operation_param import (
    NumericOperationParam,
)

OPERATION_TYPES: list[DbOperation] = []

# ===== BEGIN GENERIC =====

OPERATION_TYPES.append(
    DbFunction(
        "GREATEST",
        [
            NumericOperationParam(),
            NumericOperationParam(),
            NumericOperationParam(optional=True),
        ],
        DataTypeCategory.DYNAMIC,
        commutative=True,
    )
)
OPERATION_TYPES.append(
    DbFunction(
        "LEAST",
        [
            NumericOperationParam(),
            NumericOperationParam(),
            NumericOperationParam(optional=True),
        ],
        DataTypeCategory.DYNAMIC,
        commutative=True,
    )
)

# ===== END GENERIC =====

# ===== BEGIN NUMBER OPERATORS =====

OPERATION_TYPES.append(
    DbOperation(
        "$ + $",
        [NumericOperationParam(), NumericOperationParam()],
        DataTypeCategory.NUMERIC,
        {ValueGrowsArgsValidator()},
        commutative=True,
    )
)
OPERATION_TYPES.append(
    DbOperation(
        "$ - $",
        [NumericOperationParam(), NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    DbOperation(
        "$ * $",
        [NumericOperationParam(), NumericOperationParam()],
        DataTypeCategory.NUMERIC,
        {ValueGrowsArgsValidator()},
        commutative=True,
    )
)
OPERATION_TYPES.append(
    DbOperation(
        "$ / $",
        [
            NumericOperationParam(),
            NumericOperationParam(incompatibilities={ValueCharacteristics.ZERO}),
        ],
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    DbOperation(
        "$ % $",
        [
            NumericOperationParam(),
            NumericOperationParam(incompatibilities={ValueCharacteristics.ZERO}),
        ],
        DataTypeCategory.NUMERIC,
    )
)
# Bitwise AND
OPERATION_TYPES.append(
    DbOperation(
        "$ & $",
        [
            NumericOperationParam(incompatibilities={ValueCharacteristics.DECIMAL}),
            NumericOperationParam(incompatibilities={ValueCharacteristics.DECIMAL}),
        ],
        DataTypeCategory.NUMERIC,
        commutative=True,
    )
)
# Bitwise OR
OPERATION_TYPES.append(
    DbOperation(
        "$ | $",
        [
            NumericOperationParam(incompatibilities={ValueCharacteristics.DECIMAL}),
            NumericOperationParam(incompatibilities={ValueCharacteristics.DECIMAL}),
        ],
        DataTypeCategory.NUMERIC,
        commutative=True,
    )
)
# Bitwise XOR
OPERATION_TYPES.append(
    DbOperation(
        "$ # $",
        [
            NumericOperationParam(incompatibilities={ValueCharacteristics.DECIMAL}),
            NumericOperationParam(incompatibilities={ValueCharacteristics.DECIMAL}),
        ],
        DataTypeCategory.NUMERIC,
        commutative=True,
    )
)
# Bitwise NOT
OPERATION_TYPES.append(
    DbOperation(
        "~$",
        [NumericOperationParam(incompatibilities={ValueCharacteristics.DECIMAL})],
        DataTypeCategory.NUMERIC,
    )
)
# Bitwise left shift
OPERATION_TYPES.append(
    DbOperation(
        "$ << $",
        [
            NumericOperationParam(incompatibilities={ValueCharacteristics.DECIMAL}),
            NumericOperationParam(incompatibilities={ValueCharacteristics.DECIMAL}),
        ],
        DataTypeCategory.NUMERIC,
    )
)
# Bitwise right shift
OPERATION_TYPES.append(
    DbOperation(
        "$ >> $",
        [
            NumericOperationParam(incompatibilities={ValueCharacteristics.DECIMAL}),
            NumericOperationParam(incompatibilities={ValueCharacteristics.DECIMAL}),
        ],
        DataTypeCategory.NUMERIC,
    )
)

# ===== END NUMBER OPERATORS =====

# ===== BEGIN AGGREGATES =====

OPERATION_TYPES.append(
    DbFunction("SUM", [NumericOperationParam()], DataTypeCategory.NUMERIC)
)
OPERATION_TYPES.append(
    DbFunction("MIN", [NumericOperationParam()], DataTypeCategory.NUMERIC)
)
OPERATION_TYPES.append(
    DbFunction("MAX", [NumericOperationParam()], DataTypeCategory.NUMERIC)
)

# ===== END AGGREGATES =====

# ===== BEGIN NUMBERS =====

OPERATION_TYPES.append(
    DbFunction(
        "ABS",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    DbFunction(
        "CBRT",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)
# CEIL == CEILING
OPERATION_TYPES.append(
    DbFunction(
        "CEIL",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    DbFunction(
        "EXP",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
        {ValueGrowsArgsValidator()},
    )
)
OPERATION_TYPES.append(
    DbFunction(
        "FLOOR",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    DbFunction(
        "LN",
        [
            NumericOperationParam(
                incompatibilities={
                    ValueCharacteristics.NEGATIVE,
                    ValueCharacteristics.ZERO,
                }
            )
        ],
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    DbFunction(
        "LOG10",
        [
            NumericOperationParam(
                incompatibilities={
                    ValueCharacteristics.NEGATIVE,
                    ValueCharacteristics.ZERO,
                }
            )
        ],
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    DbFunction(
        "LOG",
        [
            NumericOperationParam(
                incompatibilities={
                    ValueCharacteristics.NEGATIVE,
                    ValueCharacteristics.ZERO,
                }
            ),
            # not marked as optional because if not present the operation is equal to LOG10, which is separate
            NumericOperationParam(
                incompatibilities={
                    ValueCharacteristics.NEGATIVE,
                    ValueCharacteristics.ZERO,
                }
            ),
        ],
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    DbFunction(
        "MOD",
        [
            NumericOperationParam(),
            NumericOperationParam(incompatibilities={ValueCharacteristics.ZERO}),
        ],
        DataTypeCategory.NUMERIC,
    )
)
# POW == POWER
OPERATION_TYPES.append(
    DbFunction(
        "POW",
        [NumericOperationParam(), NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    DbFunction(
        "ROUND",
        [
            NumericOperationParam(),
            # negative values are allowed
            NumericOperationParam(
                optional=True, incompatibilities={ValueCharacteristics.DECIMAL}
            ),
        ],
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    DbFunction(
        "SQRT",
        [NumericOperationParam(incompatibilities={ValueCharacteristics.NEGATIVE})],
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    DbFunction(
        "TRUNC",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)

# ===== END NUMERS =====

# ===== BEGIN TRIGONOMETRIC =====
OPERATION_TYPES.append(
    DbFunction(
        "COS",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)
# only for numbers [-1, +1]
OPERATION_TYPES.append(
    DbFunction(
        "ACOS",
        [
            NumericOperationParam(
                incompatibilities={
                    ValueCharacteristics.LARGE_VALUE,
                    ValueCharacteristics.MAX_VALUE,
                }
            )
        ],
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    DbFunction(
        "COSH",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
        {ValueGrowsArgsValidator()},
    )
)
# only for numbers [1,)
OPERATION_TYPES.append(
    DbFunction(
        "ACOSH",
        [
            NumericOperationParam(
                incompatibilities={
                    ValueCharacteristics.ZERO,
                    ValueCharacteristics.NEGATIVE,
                }
            )
        ],
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    DbFunction("COT", [NumericOperationParam()], DataTypeCategory.NUMERIC)
)
OPERATION_TYPES.append(
    DbFunction("SIN", [NumericOperationParam()], DataTypeCategory.NUMERIC)
)
# only for numbers [-1, +1]
OPERATION_TYPES.append(
    DbFunction(
        "ASIN",
        [
            NumericOperationParam(
                incompatibilities={
                    ValueCharacteristics.LARGE_VALUE,
                    ValueCharacteristics.MAX_VALUE,
                }
            )
        ],
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    DbFunction(
        "SINH",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
        {ValueGrowsArgsValidator()},
    )
)
OPERATION_TYPES.append(
    DbFunction(
        "ASINH",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    DbFunction("TAN", [NumericOperationParam()], DataTypeCategory.NUMERIC)
)
OPERATION_TYPES.append(
    DbFunction(
        "ATAN",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    DbFunction(
        "TANH",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)
# only for numbers [-1, +1]
OPERATION_TYPES.append(
    DbFunction(
        "ATANH",
        [
            NumericOperationParam(
                incompatibilities={
                    ValueCharacteristics.LARGE_VALUE,
                    ValueCharacteristics.MAX_VALUE,
                }
            )
        ],
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    DbFunction(
        "RADIANS",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)
OPERATION_TYPES.append(
    DbFunction(
        "DEGREES",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
    )
)

# ===== END TRIGONOMETRIC =====

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
    SingleParamValueGrowsArgsValidator,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperationOrFunction,
    OperationRelevance,
)

# note that for all types with relevance DEFAULT the relevance is reduced to LOW at the end of this file
TRIGONOMETRIC_OPERATION_TYPES: List[DbOperationOrFunction] = []

TRIGONOMETRIC_OPERATION_TYPES.append(
    DbFunction(
        "COS",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
# only for numbers [-1, +1]
TRIGONOMETRIC_OPERATION_TYPES.append(
    DbFunction(
        "ACOS",
        [
            NumericOperationParam(
                incompatibilities={
                    ExpressionCharacteristics.LARGE_VALUE,
                }
            )
        ],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
TRIGONOMETRIC_OPERATION_TYPES.append(
    DbFunction(
        "COSH",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
        {SingleParamValueGrowsArgsValidator()},
    )
)
# only for numbers [1,)
TRIGONOMETRIC_OPERATION_TYPES.append(
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
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
TRIGONOMETRIC_OPERATION_TYPES.append(
    DbFunction(
        "COT",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
TRIGONOMETRIC_OPERATION_TYPES.append(
    DbFunction(
        "SIN",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
# only for numbers [-1, +1]
TRIGONOMETRIC_OPERATION_TYPES.append(
    DbFunction(
        "ASIN",
        [
            NumericOperationParam(
                incompatibilities={
                    ExpressionCharacteristics.LARGE_VALUE,
                }
            )
        ],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
TRIGONOMETRIC_OPERATION_TYPES.append(
    DbFunction(
        "SINH",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
        {SingleParamValueGrowsArgsValidator()},
    )
)
TRIGONOMETRIC_OPERATION_TYPES.append(
    DbFunction(
        "ASINH",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
TRIGONOMETRIC_OPERATION_TYPES.append(
    DbFunction(
        "TAN",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
TRIGONOMETRIC_OPERATION_TYPES.append(
    DbFunction(
        "ATAN",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
TRIGONOMETRIC_OPERATION_TYPES.append(
    DbFunction(
        "TANH",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
# only for numbers [-1, +1]
TRIGONOMETRIC_OPERATION_TYPES.append(
    DbFunction(
        "ATANH",
        [
            NumericOperationParam(
                incompatibilities={
                    ExpressionCharacteristics.LARGE_VALUE,
                }
            )
        ],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
TRIGONOMETRIC_OPERATION_TYPES.append(
    DbFunction(
        "RADIANS",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
TRIGONOMETRIC_OPERATION_TYPES.append(
    DbFunction(
        "DEGREES",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)

for trigonometric_op in TRIGONOMETRIC_OPERATION_TYPES:
    if trigonometric_op.relevance == OperationRelevance.DEFAULT:
        trigonometric_op.relevance = OperationRelevance.LOW

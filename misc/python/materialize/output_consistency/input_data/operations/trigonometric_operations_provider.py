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
    SingleParamValueGrowsArgsValidator,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperationOrFunction,
    OperationRelevance,
)

# note that for all types with relevance DEFAULT the relevance is reduced to LOW at the end of this file
TRIGONOMETRIC_OPERATION_TYPES: list[DbOperationOrFunction] = []

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
        "acos",
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
        "cosh",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
        {SingleParamValueGrowsArgsValidator()},
    )
)
# only for numbers [1,)
TRIGONOMETRIC_OPERATION_TYPES.append(
    DbFunction(
        "acosh",
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
        "cot",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
TRIGONOMETRIC_OPERATION_TYPES.append(
    DbFunction(
        "sin",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
# only for numbers [-1, +1]
TRIGONOMETRIC_OPERATION_TYPES.append(
    DbFunction(
        "asin",
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
        "sinh",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
        {SingleParamValueGrowsArgsValidator()},
    )
)
TRIGONOMETRIC_OPERATION_TYPES.append(
    DbFunction(
        "asinh",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
TRIGONOMETRIC_OPERATION_TYPES.append(
    DbFunction(
        "tan",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
TRIGONOMETRIC_OPERATION_TYPES.append(
    DbFunction(
        "atan",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
TRIGONOMETRIC_OPERATION_TYPES.append(
    DbFunction(
        "tanh",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
# only for numbers [-1, +1]
TRIGONOMETRIC_OPERATION_TYPES.append(
    DbFunction(
        "atanh",
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
        "radians",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)
TRIGONOMETRIC_OPERATION_TYPES.append(
    DbFunction(
        "degrees",
        [NumericOperationParam()],
        NumericReturnTypeSpec(always_floating_type=True),
    )
)

for trigonometric_op in TRIGONOMETRIC_OPERATION_TYPES:
    if trigonometric_op.relevance == OperationRelevance.DEFAULT:
        trigonometric_op.relevance = OperationRelevance.LOW

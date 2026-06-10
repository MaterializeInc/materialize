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
from materialize.output_consistency.input_data.params.any_operation_param import (
    AnyLikeOtherOperationParam,
    AnyOperationParam,
)
from materialize.output_consistency.input_data.params.boolean_operation_param import (
    BooleanOperationParam,
)
from materialize.output_consistency.input_data.params.number_operation_param import (
    NumericOperationParam,
)
from materialize.output_consistency.input_data.return_specs.boolean_return_spec import (
    BooleanReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.dynamic_return_spec import (
    DynamicReturnTypeSpec,
)
from materialize.output_consistency.input_data.special.data_type_enum_param import (
    TypeEnumConstantOperationParam,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperation,
    DbOperationOrFunction,
)

GENERIC_OPERATION_TYPES: list[DbOperationOrFunction] = []

TAG_CASTING = "casting"

IS_NULL_OPERATION = DbOperation(
    "$ IS NULL",
    [AnyOperationParam()],
    BooleanReturnTypeSpec(),
)
IS_NULL_OPERATION.added_characteristics.add(ExpressionCharacteristics.NULL)
GENERIC_OPERATION_TYPES.append(IS_NULL_OPERATION)

GENERIC_OPERATION_TYPES.append(
    DbFunction(
        "greatest",
        [
            NumericOperationParam(),
            NumericOperationParam(optional=True),
            NumericOperationParam(optional=True),
        ],
        DynamicReturnTypeSpec(),
    )
)
GENERIC_OPERATION_TYPES.append(
    DbFunction(
        "least",
        [
            NumericOperationParam(),
            NumericOperationParam(optional=True),
            NumericOperationParam(optional=True),
        ],
        DynamicReturnTypeSpec(),
    )
)
GENERIC_OPERATION_TYPES.append(
    DbOperation(
        "CASE WHEN $ THEN $ ELSE $ END",
        [BooleanOperationParam(), AnyOperationParam(), AnyLikeOtherOperationParam(1)],
        DynamicReturnTypeSpec(param_index_to_take_type=1),
        # due to different evaluation order
        is_pg_compatible=False,
    )
)
GENERIC_OPERATION_TYPES.append(
    DbFunction(
        "coalesce",
        [
            AnyOperationParam(),
            AnyLikeOtherOperationParam(0, optional=True),
            AnyLikeOtherOperationParam(0, optional=True),
        ],
        DynamicReturnTypeSpec(param_index_to_take_type=0),
    )
)
GENERIC_OPERATION_TYPES.append(
    DbFunction(
        "nullif",
        [
            AnyOperationParam(),
            AnyLikeOtherOperationParam(0),
        ],
        DynamicReturnTypeSpec(param_index_to_take_type=0),
    )
)
GENERIC_OPERATION_TYPES.append(
    DbOperation(
        "CAST ($ AS $)",
        [
            AnyOperationParam(),
            TypeEnumConstantOperationParam(must_be_pg_compatible=True),
        ],
        DynamicReturnTypeSpec(param_index_to_take_type=1),
        tags={TAG_CASTING},
    )
)

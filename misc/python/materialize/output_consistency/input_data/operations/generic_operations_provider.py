# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import List

from materialize.output_consistency.input_data.params.any_operation_param import (
    AnyOperationParam,
)
from materialize.output_consistency.input_data.params.boolean_operation_param import (
    BooleanOperationParam,
)
from materialize.output_consistency.input_data.params.number_operation_param import (
    NumericOperationParam,
)
from materialize.output_consistency.input_data.return_specs.dynamic_return_spec import (
    DynamicReturnTypeSpec,
)
from materialize.output_consistency.input_data.validators.generic_args_validator import (
    DataTypeCategoryMatchesArgsValidator,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperation,
    DbOperationOrFunction,
)

GENERIC_OPERATION_TYPES: List[DbOperationOrFunction] = []

GENERIC_OPERATION_TYPES.append(
    DbFunction(
        "GREATEST",
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
        "LEAST",
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
        [BooleanOperationParam(), AnyOperationParam(), AnyOperationParam()],
        DynamicReturnTypeSpec(param_index_to_take_type=1),
        {DataTypeCategoryMatchesArgsValidator(1, 2)},
    )
)

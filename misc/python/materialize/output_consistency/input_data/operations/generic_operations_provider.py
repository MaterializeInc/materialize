# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.input_data.params.number_operation_param import (
    NumericOperationParam,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperationOrFunction,
)

GENERIC_OPERATION_TYPES: list[DbOperationOrFunction] = []

GENERIC_OPERATION_TYPES.append(
    DbFunction(
        "GREATEST",
        [
            NumericOperationParam(),
            NumericOperationParam(optional=True),
            NumericOperationParam(optional=True),
        ],
        DataTypeCategory.DYNAMIC,
        commutative=True,
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
        DataTypeCategory.DYNAMIC,
        commutative=True,
    )
)

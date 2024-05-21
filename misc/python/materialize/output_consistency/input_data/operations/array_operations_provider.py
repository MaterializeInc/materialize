# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.input_data.params.any_operation_param import (
    AnyOperationParam,
)
from materialize.output_consistency.input_data.return_specs.array_return_spec import (
    ArrayReturnTypeSpec,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbFunctionWithCustomPattern,
    DbOperationOrFunction,
)

ARRAY_OPERATION_TYPES: list[DbOperationOrFunction] = []

ARRAY_OPERATION_TYPES.append(
    DbFunction(
        "array_agg",
        [AnyOperationParam()],
        ArrayReturnTypeSpec(array_value_type_category=DataTypeCategory.DYNAMIC),
        is_aggregation=True,
        comment="without ordering",
    ),
)
ARRAY_OPERATION_TYPES.append(
    DbFunctionWithCustomPattern(
        "array_agg",
        {1: "array_agg($ ORDER BY row_index)"},
        [AnyOperationParam()],
        ArrayReturnTypeSpec(array_value_type_category=DataTypeCategory.DYNAMIC),
        is_aggregation=True,
        comment="with ordering",
    ),
)

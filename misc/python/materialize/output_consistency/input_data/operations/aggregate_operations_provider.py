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
from materialize.output_consistency.input_data.params.number_operation_param import (
    NumericOperationParam,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperationOrFunction,
)

AGGREGATE_OPERATION_TYPES: List[DbOperationOrFunction] = []

AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "SUM", [NumericOperationParam()], DataTypeCategory.NUMERIC, aggregation=True
    ),
)
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "MIN", [NumericOperationParam()], DataTypeCategory.NUMERIC, aggregation=True
    )
)
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "MAX", [NumericOperationParam()], DataTypeCategory.NUMERIC, aggregation=True
    )
)

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
from materialize.output_consistency.input_data.params.any_operation_param import (
    AnyOperationParam,
)
from materialize.output_consistency.input_data.params.boolean_operation_param import (
    BooleanOperationParam,
)
from materialize.output_consistency.input_data.params.number_operation_param import (
    NumericOperationParam,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperationOrFunction,
    OperationRelevance,
)

AGGREGATE_OPERATION_TYPES: List[DbOperationOrFunction] = []

AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "array_arg",
        [AnyOperationParam()],
        DataTypeCategory.DYNAMIC_ARRAY,
        is_aggregation=True,
    ),
)
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "avg",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
        is_aggregation=True,
        relevance=OperationRelevance.HIGH,
    ),
)
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "bool_and",
        [BooleanOperationParam()],
        DataTypeCategory.BOOLEAN,
        is_aggregation=True,
        relevance=OperationRelevance.LOW,
    ),
)
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "bool_or",
        [BooleanOperationParam()],
        DataTypeCategory.BOOLEAN,
        is_aggregation=True,
        relevance=OperationRelevance.LOW,
    ),
)
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "count",
        [AnyOperationParam()],
        DataTypeCategory.NUMERIC,
        is_aggregation=True,
        relevance=OperationRelevance.HIGH,
    ),
)
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "max",
        [AnyOperationParam()],
        DataTypeCategory.DYNAMIC,
        is_aggregation=True,
        relevance=OperationRelevance.HIGH,
    )
)
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "min",
        [AnyOperationParam()],
        DataTypeCategory.DYNAMIC,
        is_aggregation=True,
        relevance=OperationRelevance.HIGH,
    )
)
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "stddev_pop",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
        is_aggregation=True,
        relevance=OperationRelevance.LOW,
    ),
)
# equal to stddev
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "stddev_samp",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
        is_aggregation=True,
        relevance=OperationRelevance.LOW,
    ),
)
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "sum",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
        is_aggregation=True,
        relevance=OperationRelevance.HIGH,
    ),
)
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "variance_pop",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
        is_aggregation=True,
        relevance=OperationRelevance.LOW,
    ),
)
# equal to variance
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "variance_samp",
        [NumericOperationParam()],
        DataTypeCategory.NUMERIC,
        is_aggregation=True,
        relevance=OperationRelevance.LOW,
    ),
)

# TODO: requires JSON type: jsonb_agg(expression)
# TODO: requires JSON type: jsonb_object_agg(keys, values)
# TODO: requires text type: string_agg(valueext, delimiterext)

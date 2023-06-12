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
from materialize.output_consistency.input_data.return_specs.boolean_return_spec import (
    BooleanReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.dynamic_return_spec import (
    DynamicReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.number_return_spec import (
    NumericReturnTypeSpec,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbFunctionWithCustomPattern,
    DbOperationOrFunction,
    OperationRelevance,
)

AGGREGATE_OPERATION_TYPES: List[DbOperationOrFunction] = []

AGGREGATE_OPERATION_TYPES.append(
    DbFunctionWithCustomPattern(
        "array_agg",
        {1: "array_agg($ ORDER BY row_index)"},
        [AnyOperationParam()],
        DynamicReturnTypeSpec(),
        is_aggregation=True,
    ),
)
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "avg",
        [NumericOperationParam()],
        NumericReturnTypeSpec(),
        is_aggregation=True,
        relevance=OperationRelevance.HIGH,
    ),
)
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "bool_and",
        [BooleanOperationParam()],
        BooleanReturnTypeSpec(),
        is_aggregation=True,
        relevance=OperationRelevance.LOW,
    ),
)
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "bool_or",
        [BooleanOperationParam()],
        BooleanReturnTypeSpec(),
        is_aggregation=True,
        relevance=OperationRelevance.LOW,
    ),
)
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "count",
        [AnyOperationParam()],
        NumericReturnTypeSpec(only_integer=True),
        is_aggregation=True,
        relevance=OperationRelevance.HIGH,
    ),
)
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "max",
        [AnyOperationParam()],
        DynamicReturnTypeSpec(),
        is_aggregation=True,
        relevance=OperationRelevance.HIGH,
    )
)
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "min",
        [AnyOperationParam()],
        DynamicReturnTypeSpec(),
        is_aggregation=True,
        relevance=OperationRelevance.HIGH,
    )
)
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "stddev_pop",
        [NumericOperationParam()],
        NumericReturnTypeSpec(),
        is_aggregation=True,
        relevance=OperationRelevance.LOW,
    ),
)
# equal to stddev
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "stddev_samp",
        [NumericOperationParam()],
        NumericReturnTypeSpec(),
        is_aggregation=True,
        relevance=OperationRelevance.LOW,
    ),
)
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "sum",
        [NumericOperationParam()],
        NumericReturnTypeSpec(),
        is_aggregation=True,
        relevance=OperationRelevance.HIGH,
    ),
)
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "var_pop",
        [NumericOperationParam()],
        NumericReturnTypeSpec(),
        is_aggregation=True,
        relevance=OperationRelevance.LOW,
    ),
)
# equal to variance
AGGREGATE_OPERATION_TYPES.append(
    DbFunction(
        "var_samp",
        [NumericOperationParam()],
        NumericReturnTypeSpec(),
        is_aggregation=True,
        relevance=OperationRelevance.LOW,
    ),
)

# TODO: requires JSON type: jsonb_agg(expression)
# TODO: requires JSON type: jsonb_object_agg(keys, values)
# TODO: requires text type: string_agg(valueext, delimiterext)

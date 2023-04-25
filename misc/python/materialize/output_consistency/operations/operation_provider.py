# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.data_type.data_type_group import DataTypeGroup
from materialize.output_consistency.data_type.value_characteristics import (
    ValueCharacteristics,
)
from materialize.output_consistency.operations.operation import (
    BinaryFunction,
    OperationWithNParams,
    OperationWithTwoParams,
    UnaryFunction,
)
from materialize.output_consistency.operations.operation_args_validator import (
    ValueGrowsArgsValidator,
)
from materialize.output_consistency.operations.operation_param import (
    NumericOperationParam,
)

add_op = OperationWithTwoParams(
    "$ + $",
    NumericOperationParam(),
    NumericOperationParam(),
    DataTypeGroup.NUMERIC,
    {ValueGrowsArgsValidator()},
    commutative=True,
)
subt_op = OperationWithTwoParams(
    "$ - $", NumericOperationParam(), NumericOperationParam(), DataTypeGroup.NUMERIC
)
mult_op = OperationWithTwoParams(
    "$ * $",
    NumericOperationParam(),
    NumericOperationParam(),
    DataTypeGroup.NUMERIC,
    {ValueGrowsArgsValidator()},
    commutative=True,
)
div_op = OperationWithTwoParams(
    "$ / $",
    NumericOperationParam(),
    NumericOperationParam({ValueCharacteristics.ZERO}),
    DataTypeGroup.NUMERIC,
)

sum_func = UnaryFunction("SUM", NumericOperationParam(), DataTypeGroup.NUMERIC)
min_func = UnaryFunction("MIN", NumericOperationParam(), DataTypeGroup.NUMERIC)
max_func = UnaryFunction("MAX", NumericOperationParam(), DataTypeGroup.NUMERIC)

sqrt_func = UnaryFunction(
    "SQRT",
    NumericOperationParam({ValueCharacteristics.NEGATIVE}),
    DataTypeGroup.NUMERIC,
)
abs_func = UnaryFunction(
    "ABS",
    NumericOperationParam(),
    DataTypeGroup.NUMERIC,
)

greatest_func = BinaryFunction(
    "GREATEST", NumericOperationParam(), NumericOperationParam(), DataTypeGroup.DYNAMIC
)
least_func = BinaryFunction(
    "LEAST", NumericOperationParam(), NumericOperationParam(), DataTypeGroup.DYNAMIC
)
greatest3_func = OperationWithNParams(
    "GREATEST($, $, $)",
    [NumericOperationParam(), NumericOperationParam(), NumericOperationParam()],
    DataTypeGroup.NUMERIC,
)
least3_func = OperationWithNParams(
    "LEAST($, $, $)",
    [NumericOperationParam(), NumericOperationParam(), NumericOperationParam()],
    DataTypeGroup.NUMERIC,
)


OPERATION_TYPES: list[OperationWithNParams] = [
    # arithmetic operators
    add_op,
    subt_op,
    mult_op,
    div_op,
    # aggregation
    sum_func,
    min_func,
    max_func,
    # math operations
    sqrt_func,
    abs_func,
    # other
    greatest_func,
    least_func,
    greatest3_func,
    least3_func,
]

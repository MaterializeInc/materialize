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
from materialize.output_consistency.input_data.params.array_operation_param import (
    ArrayLikeOtherArrayOperationParam,
    ArrayOfOtherElementOperationParam,
    ArrayOperationParam,
)
from materialize.output_consistency.input_data.params.collection_operation_param import (
    ElementOfOtherCollectionOperationParam,
)
from materialize.output_consistency.input_data.params.enum_constant_operation_params import (
    ARRAY_DIMENSION_PARAM,
)
from materialize.output_consistency.input_data.params.number_operation_param import (
    NumericOperationParam,
)
from materialize.output_consistency.input_data.return_specs.array_return_spec import (
    ArrayReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.boolean_return_spec import (
    BooleanReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.collection_entry_return_spec import (
    CollectionEntryReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.number_return_spec import (
    NumericReturnTypeSpec,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbFunctionWithCustomPattern,
    DbOperation,
    DbOperationOrFunction,
)

ARRAY_OPERATION_TYPES: list[DbOperationOrFunction] = []

ARRAY_OPERATION_TYPES.append(
    DbOperation(
        # parentheses are needed only for Postgres when accessing an array element on the result of a function
        "($)[$]",
        [ArrayOperationParam(), NumericOperationParam(no_floating_point_type=True)],
        CollectionEntryReturnTypeSpec(param_index_to_take_type=0),
        comment="access by index",
    )
)

ARRAY_OPERATION_TYPES.append(
    DbOperation(
        # parentheses are needed only for Postgres when accessing an array element on the result of a function
        "$([$:$])",
        [
            ArrayOperationParam(),
            NumericOperationParam(no_floating_point_type=True),
            NumericOperationParam(no_floating_point_type=True),
        ],
        ArrayReturnTypeSpec(),
        comment="slice double-sided",
    )
)

ARRAY_OPERATION_TYPES.append(
    DbOperation(
        # parentheses are needed only for Postgres when accessing an array element on the result of a function
        "$([:$])",
        [ArrayOperationParam(), NumericOperationParam(no_floating_point_type=True)],
        ArrayReturnTypeSpec(),
        comment="slice left open",
    )
)

ARRAY_OPERATION_TYPES.append(
    DbOperation(
        # parentheses are needed only for Postgres when accessing an array element on the result of a function
        "$([$:])",
        [ArrayOperationParam(), NumericOperationParam(no_floating_point_type=True)],
        ArrayReturnTypeSpec(),
        comment="slice right open",
    )
)

ARRAY_OPERATION_TYPES.append(
    DbOperation(
        "$ || $",
        [
            ArrayOperationParam(),
            ArrayLikeOtherArrayOperationParam(index_of_previous_param=0),
        ],
        ArrayReturnTypeSpec(),
        comment="concatenate arrays (like array_cat)",
    )
)

ARRAY_OPERATION_TYPES.append(
    DbOperation(
        "$ @> $",
        [
            ArrayOperationParam(),
            ElementOfOtherCollectionOperationParam(index_of_previous_param=0),
        ],
        BooleanReturnTypeSpec(),
        comment="contains",
    )
)

ARRAY_OPERATION_TYPES.append(
    DbOperation(
        "$ = ANY ($)",
        [
            AnyOperationParam(),
            ArrayOfOtherElementOperationParam(index_of_previous_param=0),
        ],
        BooleanReturnTypeSpec(),
    )
)
ARRAY_OPERATION_TYPES.append(
    DbOperation(
        "$ = ALL ($)",
        [
            AnyOperationParam(),
            ArrayOfOtherElementOperationParam(index_of_previous_param=0),
        ],
        BooleanReturnTypeSpec(),
    )
)
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
ARRAY_OPERATION_TYPES.append(
    DbFunction(
        "array_upper",
        [ArrayOperationParam(), ARRAY_DIMENSION_PARAM],
        NumericReturnTypeSpec(only_integer=True),
    ),
)
ARRAY_OPERATION_TYPES.append(
    DbFunction(
        "array_upper",
        [ArrayOperationParam(), ARRAY_DIMENSION_PARAM],
        NumericReturnTypeSpec(only_integer=True),
        comment="upper bound of a specified array dimension",
    ),
)
ARRAY_OPERATION_TYPES.append(
    DbFunction(
        "array_length",
        [ArrayOperationParam(), ARRAY_DIMENSION_PARAM],
        NumericReturnTypeSpec(only_integer=True),
    ),
)
ARRAY_OPERATION_TYPES.append(
    DbFunction(
        "array_position",
        [
            ArrayOperationParam(),
            ElementOfOtherCollectionOperationParam(index_of_previous_param=0),
        ],
        NumericReturnTypeSpec(only_integer=True),
        comment="index of",
    ),
)

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from materialize.output_consistency.input_data.params.collection_operation_param import (
    ElementOfOtherCollectionOperationParam,
)
from materialize.output_consistency.input_data.params.range_operation_param import (
    RangeLikeOtherRangeOperationParam,
    RangeOperationParam,
)
from materialize.output_consistency.input_data.return_specs.boolean_return_spec import (
    BooleanReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.collection_entry_return_spec import (
    CollectionEntryReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.range_return_spec import (
    RangeReturnTypeSpec,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperation,
    DbOperationOrFunction,
)

RANGE_OPERATION_TYPES: list[DbOperationOrFunction] = []

RANGE_OPERATION_TYPES.append(
    DbOperation(
        "$ || $",
        [
            RangeOperationParam(),
            RangeLikeOtherRangeOperationParam(index_of_previous_param=0),
        ],
        RangeReturnTypeSpec(),
    )
)

RANGE_OPERATION_TYPES.append(
    DbOperation(
        "$ * $",
        [
            RangeOperationParam(),
            RangeLikeOtherRangeOperationParam(index_of_previous_param=0),
        ],
        RangeReturnTypeSpec(),
        comment="Intersection of two ranges",
    )
)

RANGE_OPERATION_TYPES.append(
    DbOperation(
        "$ && $",
        [
            RangeOperationParam(),
            RangeLikeOtherRangeOperationParam(index_of_previous_param=0),
        ],
        RangeReturnTypeSpec(),
        comment="Overlap of two ranges",
    )
)

RANGE_OPERATION_TYPES.append(
    DbOperation(
        "$ @> $",
        [
            RangeOperationParam(),
            ElementOfOtherCollectionOperationParam(index_of_previous_param=0),
        ],
        BooleanReturnTypeSpec(),
        comment="contains",
    )
)

RANGE_OPERATION_TYPES.append(
    DbFunction(
        "isempty",
        [
            RangeOperationParam(),
        ],
        BooleanReturnTypeSpec(),
    )
)

RANGE_OPERATION_TYPES.append(
    DbFunction(
        "lower",
        [
            RangeOperationParam(),
        ],
        CollectionEntryReturnTypeSpec(param_index_to_take_type=0),
    )
)

RANGE_OPERATION_TYPES.append(
    DbFunction(
        "upper",
        [
            RangeOperationParam(),
        ],
        CollectionEntryReturnTypeSpec(param_index_to_take_type=0),
    )
)

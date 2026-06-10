# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.input_data.params.any_operation_param import (
    AnyOperationParam,
)
from materialize.output_consistency.input_data.params.collection_operation_param import (
    ElementOfOtherCollectionOperationParam,
)
from materialize.output_consistency.input_data.params.enum_constant_operation_params import (
    COLLECTION_INDEX_OPTIONAL_PARAM,
    COLLECTION_INDEX_PARAM,
)
from materialize.output_consistency.input_data.params.list_operation_param import (
    ListLikeOtherListOperationParam,
    ListOfOtherElementOperationParam,
    ListOperationParam,
)
from materialize.output_consistency.input_data.params.row_indices_param import (
    RowIndicesParam,
)
from materialize.output_consistency.input_data.params.same_operation_param import (
    SameOperationParam,
)
from materialize.output_consistency.input_data.return_specs.boolean_return_spec import (
    BooleanReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.collection_entry_return_spec import (
    CollectionEntryReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.list_return_spec import (
    ListReturnTypeSpec,
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

# all operations will be disabled for Postgres at the bottom
LIST_OPERATION_TYPES: list[DbOperationOrFunction] = []

LIST_OPERATION_TYPES.append(
    DbOperation(
        "$ || $",
        [
            ListOperationParam(),
            ListLikeOtherListOperationParam(index_of_previous_param=0),
        ],
        ListReturnTypeSpec(),
        comment="concatenate lists (equal to list_cat)",
    )
)

LIST_OPERATION_TYPES.append(
    DbOperation(
        "$ || $",
        [
            ListOperationParam(),
            ElementOfOtherCollectionOperationParam(index_of_previous_param=0),
        ],
        ListReturnTypeSpec(),
        comment="append element to list (equal to list_append)",
    )
)

LIST_OPERATION_TYPES.append(
    DbOperation(
        "$ || $",
        [
            AnyOperationParam(include_record_type=False),
            ListOfOtherElementOperationParam(index_of_previous_param=0),
        ],
        ListReturnTypeSpec(),
        comment="prepend element to list (equal to list_prepend)",
    )
)

LIST_OPERATION_TYPES.append(
    DbOperation(
        "$ @> $",
        [
            ListOperationParam(),
            ElementOfOtherCollectionOperationParam(index_of_previous_param=0),
        ],
        BooleanReturnTypeSpec(),
        comment="contains",
    )
)

LIST_OPERATION_TYPES.append(
    DbFunctionWithCustomPattern(
        "list_agg",
        {
            3: "list_agg($ ORDER BY $, $)",
        },
        [
            AnyOperationParam(include_record_type=True),
            RowIndicesParam(index_of_param_to_share_data_source=0),
            SameOperationParam(index_of_previous_param=0),
        ],
        ListReturnTypeSpec(),
    )
)

LIST_OPERATION_TYPES.append(
    DbFunction(
        "list_length",
        [ListOperationParam()],
        NumericReturnTypeSpec(only_integer=True),
    )
)

LIST_OPERATION_TYPES.append(
    DbOperation(
        "$[$]",
        [ListOperationParam(), COLLECTION_INDEX_PARAM],
        CollectionEntryReturnTypeSpec(param_index_to_take_type=0),
        comment="access list element",
    )
)

LIST_OPERATION_TYPES.append(
    DbOperation(
        "$[$:$]",
        [
            ListOperationParam(),
            COLLECTION_INDEX_OPTIONAL_PARAM,
            COLLECTION_INDEX_OPTIONAL_PARAM,
        ],
        ListReturnTypeSpec(),
        comment="slice list",
    )
)
for operation in LIST_OPERATION_TYPES:
    # Postgres does not support the list type
    operation.is_pg_compatible = False

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.input_data.params.any_operation_param import (
    AnyLikeOtherOperationParam,
    AnyOperationParam,
)
from materialize.output_consistency.input_data.params.collection_operation_param import (
    ElementOfOtherCollectionOperationParam,
)
from materialize.output_consistency.input_data.params.enum_constant_operation_params import (
    COLLECTION_INDEX_PARAM,
    COLLECTION_INDEX_PARAM_OPT,
)
from materialize.output_consistency.input_data.params.list_operation_param import (
    ListLikeOtherListOperationParam,
    ListOfOtherElementOperationParam,
    ListOperationParam,
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
            AnyOperationParam(),
            ListOfOtherElementOperationParam(index_of_previous_param=0),
        ],
        ListReturnTypeSpec(),
        comment="prepend element to list (equal to list_prepend)",
    )
)

LIST_OPERATION_TYPES.append(
    DbFunction(
        "list_agg",
        [
            AnyOperationParam(),
            AnyLikeOtherOperationParam(index_of_previous_param=0, optional=True),
            AnyLikeOtherOperationParam(index_of_previous_param=0, optional=True),
            AnyLikeOtherOperationParam(index_of_previous_param=0, optional=True),
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
        [ListOperationParam(), COLLECTION_INDEX_PARAM_OPT, COLLECTION_INDEX_PARAM_OPT],
        ListReturnTypeSpec(),
        comment="slice list",
    )
)
for operation in LIST_OPERATION_TYPES:
    # Postgres does not support the list type
    operation.is_pg_compatible = False

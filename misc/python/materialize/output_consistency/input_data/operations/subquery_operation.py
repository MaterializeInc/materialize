# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.output_consistency.input_data.return_specs.dynamic_return_spec import (
    DynamicReturnTypeSpec,
)
from materialize.output_consistency.input_data.types.boolean_type_provider import (
    BOOLEAN_DATA_TYPE,
)
from materialize.output_consistency.input_data.types.number_types_provider import (
    UNSIGNED_INT_TYPES,
)
from materialize.output_consistency.input_data.types.string_type_provider import (
    STRING_DATA_TYPES,
)
from materialize.output_consistency.operation.operation import (
    DbOperation,
    DbOperationOrFunction,
)
from materialize.output_consistency.sub_query.sub_query_param import SubQueryParam

SUB_QUERY_OPERATIONS: list[DbOperationOrFunction] = []

# these are never an aggregation for the outer query
for return_type in [STRING_DATA_TYPES, BOOLEAN_DATA_TYPE, UNSIGNED_INT_TYPES[1]]:
    # subquery as simple column
    SUB_QUERY_OPERATIONS.append(
        DbOperation(
            "$",
            [SubQueryParam(return_type)],
            DynamicReturnTypeSpec(),
        )
    )

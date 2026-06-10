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
from materialize.output_consistency.input_data.params.number_operation_param import (
    NumericOperationParam,
)
from materialize.output_consistency.input_data.return_specs.string_return_spec import (
    StringReturnTypeSpec,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperationOrFunction,
)

PG_OPERATIONS: list[DbOperationOrFunction] = []

PG_OPERATIONS.append(
    DbFunction(
        "pg_typeof",
        [AnyOperationParam()],
        StringReturnTypeSpec(),
    )
)

PG_OPERATIONS.append(
    DbFunction(
        "pg_size_pretty",
        [NumericOperationParam()],
        StringReturnTypeSpec(),
    )
)

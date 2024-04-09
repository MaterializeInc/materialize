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
from materialize.output_consistency.input_data.return_specs.boolean_return_spec import (
    BooleanReturnTypeSpec,
)
from materialize.output_consistency.operation.operation import (
    DbOperation,
    DbOperationOrFunction,
)
from materialize.output_consistency.operation.operation_param import OperationParam

SET_OPERATION_TYPES: list[DbOperationOrFunction] = []


def create_in_operation(n: int) -> DbOperation:
    in_expression = ", ".join(["$" for _ in range(0, n)])
    params: list[OperationParam] = [AnyOperationParam()]
    params.extend([AnyLikeOtherOperationParam(0) for _ in range(0, n)])

    return DbOperation(
        f"$ IN ({in_expression})",
        params,
        BooleanReturnTypeSpec(),
    )


SET_OPERATION_TYPES.append(create_in_operation(1))

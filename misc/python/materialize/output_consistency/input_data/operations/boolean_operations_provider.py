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

BOOLEAN_OPERATION_TYPES: List[DbOperationOrFunction] = []

BOOLEAN_OPERATION_TYPES.append(
    DbOperation(
        "$ = $",
        [AnyOperationParam(), AnyLikeOtherOperationParam(0)],
        BooleanReturnTypeSpec(),
    )
)

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
from materialize.output_consistency.input_data.return_specs.text_return_spec import (
    TextReturnTypeSpec,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperationOrFunction,
)

SPECIAL_OPERATION_TYPES: list[DbOperationOrFunction] = []

SPECIAL_OPERATION_TYPES.append(
    DbFunction(
        "pg_typeof",
        [AnyOperationParam()],
        TextReturnTypeSpec(),
    )
)

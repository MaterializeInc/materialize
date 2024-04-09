# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.input_data.params.boolean_operation_param import (
    BooleanOperationParam,
)
from materialize.output_consistency.input_data.return_specs.boolean_return_spec import (
    BooleanReturnTypeSpec,
)
from materialize.output_consistency.operation.operation import (
    DbOperation,
    DbOperationOrFunction,
)

BOOLEAN_OPERATION_TYPES: list[DbOperationOrFunction] = []

AND_OPERATION = DbOperation(
    "$ AND $",
    [BooleanOperationParam(), BooleanOperationParam()],
    BooleanReturnTypeSpec(),
    # disable for Postgres because the evaluation order will be different
    is_pg_compatible=False,
)
BOOLEAN_OPERATION_TYPES.append(AND_OPERATION)

OR_OPERATION = DbOperation(
    "$ OR $",
    [BooleanOperationParam(), BooleanOperationParam()],
    BooleanReturnTypeSpec(),
    # disable for Postgres because the evaluation order will be different
    is_pg_compatible=False,
)

BOOLEAN_OPERATION_TYPES.append(OR_OPERATION)

NOT_OPERATION = DbOperation(
    "NOT ($)",
    [BooleanOperationParam()],
    BooleanReturnTypeSpec(),
)
BOOLEAN_OPERATION_TYPES.append(NOT_OPERATION)

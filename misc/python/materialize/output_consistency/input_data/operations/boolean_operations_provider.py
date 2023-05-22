# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import List

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.input_data.params.any_operation_param import (
    AnyOperationParam,
)
from materialize.output_consistency.input_data.params.boolean_operation_param import (
    BooleanOperationParam,
)
from materialize.output_consistency.input_data.validators.generic_args_validator import (
    DataTypeCategoryMatchesArgsValidator,
)
from materialize.output_consistency.operation.operation import (
    DbOperation,
    DbOperationOrFunction,
)

BOOLEAN_OPERATION_TYPES: List[DbOperationOrFunction] = []

BOOLEAN_OPERATION_TYPES.append(
    DbOperation(
        "CASE WHEN $ THEN $ ELSE $ END",
        [BooleanOperationParam(), AnyOperationParam(), AnyOperationParam()],
        DataTypeCategory.BOOLEAN,
        {DataTypeCategoryMatchesArgsValidator(1, 2)},
    )
)

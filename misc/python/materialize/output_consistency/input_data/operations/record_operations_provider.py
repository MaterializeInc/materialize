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
from materialize.output_consistency.input_data.params.enum_constant_operation_params import (
    RECORD_FIELD_PARAM,
)
from materialize.output_consistency.input_data.params.record_operation_param import (
    RecordOperationParam,
)
from materialize.output_consistency.input_data.params.string_operation_param import (
    StringOperationParam,
)
from materialize.output_consistency.input_data.return_specs.record_return_spec import (
    RecordReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.undetermined_return_spec import (
    UndeterminedReturnTypeSpec,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperation,
    DbOperationOrFunction,
    OperationRelevance,
)

RECORD_OPERATION_TYPES: list[DbOperationOrFunction] = []


RECORD_OPERATION_TYPES.append(
    DbFunction(
        "record",
        [StringOperationParam(), AnyOperationParam()],
        RecordReturnTypeSpec(),
        comment="variant useful for map_build",
    )
)

RECORD_OPERATION_TYPES.append(
    DbFunction(
        "record",
        [
            AnyOperationParam(),
            AnyOperationParam(optional=True),
            AnyOperationParam(optional=True),
            AnyOperationParam(optional=True),
        ],
        RecordReturnTypeSpec(),
        comment="generic variant",
        relevance=OperationRelevance.LOW,
    )
)

RECORD_OPERATION_TYPES.append(
    DbOperation(
        "$.$",
        [RecordOperationParam(), RECORD_FIELD_PARAM],
        UndeterminedReturnTypeSpec(),
    )
)

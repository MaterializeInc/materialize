# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.input_data.params.enum_constant_operation_params import (
    JSON_FIELD_INDEX_PARAM,
    JSON_FIELD_NAME_PARAM,
    JSON_PATH_PARAM,
)
from materialize.output_consistency.input_data.params.jsonb_operation_param import (
    JsonbOperationParam,
)
from materialize.output_consistency.input_data.return_specs.boolean_return_spec import (
    BooleanReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.jsonb_return_spec import (
    JsonbReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.number_return_spec import (
    NumericReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.text_return_spec import (
    TextReturnTypeSpec,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperation,
    DbOperationOrFunction,
)

JSONB_OPERATION_TYPES: list[DbOperationOrFunction] = []

TAG_JSONB_TO_TEXT = "jsonb_to_text"

JSONB_OPERATION_TYPES.append(
    DbOperation(
        "$ -> $",
        [JsonbOperationParam(), JSON_FIELD_NAME_PARAM],
        JsonbReturnTypeSpec(),
    )
)
JSONB_OPERATION_TYPES.append(
    DbOperation(
        "$ -> $",
        [JsonbOperationParam(), JSON_FIELD_INDEX_PARAM],
        JsonbReturnTypeSpec(),
    )
)
JSONB_OPERATION_TYPES.append(
    DbOperation(
        "$ ->> $",
        [JsonbOperationParam(), JSON_FIELD_NAME_PARAM],
        TextReturnTypeSpec(),
        tags={TAG_JSONB_TO_TEXT},
    )
)
JSONB_OPERATION_TYPES.append(
    DbOperation(
        "$ ->> $",
        [JsonbOperationParam(), JSON_FIELD_INDEX_PARAM],
        TextReturnTypeSpec(),
        tags={TAG_JSONB_TO_TEXT},
    )
)
JSONB_OPERATION_TYPES.append(
    DbOperation(
        "$ #> $",
        [JsonbOperationParam(), JSON_PATH_PARAM],
        JsonbReturnTypeSpec(),
    )
)
JSONB_OPERATION_TYPES.append(
    DbOperation(
        "$ #>> $",
        [JsonbOperationParam(), JSON_PATH_PARAM],
        TextReturnTypeSpec(),
        tags={TAG_JSONB_TO_TEXT},
    )
)
JSONB_OPERATION_TYPES.append(
    DbOperation(
        "$ || $",
        [JsonbOperationParam(), JsonbOperationParam()],
        JsonbReturnTypeSpec(),
    )
)
JSONB_OPERATION_TYPES.append(
    DbOperation(
        "$ - $",
        [JsonbOperationParam(), JSON_FIELD_NAME_PARAM],
        JsonbReturnTypeSpec(),
    )
)
JSONB_OPERATION_TYPES.append(
    DbOperation(
        "$ @> $",
        [JsonbOperationParam(), JsonbOperationParam()],
        BooleanReturnTypeSpec(),
    )
)
JSONB_OPERATION_TYPES.append(
    DbOperation(
        "$ <@ $",
        [JsonbOperationParam(), JsonbOperationParam()],
        BooleanReturnTypeSpec(),
    )
)
JSONB_OPERATION_TYPES.append(
    DbOperation(
        "$ ? $",
        [JsonbOperationParam(), JSON_FIELD_NAME_PARAM],
        BooleanReturnTypeSpec(),
    )
)

JSONB_OPERATION_TYPES.append(
    DbFunction(
        "jsonb_array_length",
        [JsonbOperationParam()],
        NumericReturnTypeSpec(only_integer=True),
    )
)

JSONB_OPERATION_TYPES.append(
    DbFunction(
        "jsonb_pretty",
        [JsonbOperationParam()],
        TextReturnTypeSpec(),
        tags={TAG_JSONB_TO_TEXT},
    )
)

JSONB_OPERATION_TYPES.append(
    DbFunction(
        "jsonb_typeof",
        [JsonbOperationParam()],
        TextReturnTypeSpec(),
    )
)

JSONB_OPERATION_TYPES.append(
    DbFunction(
        "jsonb_strip_nulls",
        [JsonbOperationParam()],
        JsonbReturnTypeSpec(),
    )
)

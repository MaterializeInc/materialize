# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent

from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.input_data.params.any_operation_param import (
    AnyOperationParam,
)
from materialize.output_consistency.input_data.params.date_time_operation_param import (
    DateTimeOperationParam,
)
from materialize.output_consistency.input_data.params.enum_constant_operation_params import (
    JSON_FIELD_INDEX_PARAM,
    JSON_FIELD_NAME_PARAM,
    JSON_PATH_PARAM,
)
from materialize.output_consistency.input_data.params.jsonb_operation_param import (
    JsonbOperationParam,
)
from materialize.output_consistency.input_data.params.number_operation_param import (
    NumericOperationParam,
)
from materialize.output_consistency.input_data.params.record_operation_param import (
    RecordOperationParam,
)
from materialize.output_consistency.input_data.params.same_operation_param import (
    SameOperationParam,
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
from materialize.output_consistency.input_data.return_specs.string_return_spec import (
    StringReturnTypeSpec,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbFunctionWithCustomPattern,
    DbOperation,
    DbOperationOrFunction,
    OperationRelevance,
)

JSONB_OPERATION_TYPES: list[DbOperationOrFunction] = []

TAG_JSONB_TO_TEXT = "jsonb_to_text"
TAG_JSONB_AGGREGATION = "jsonb_aggregation"
TAG_JSONB_VALUE_ACCESS = "jsonb_value_access"

JSONB_OPERATION_TYPES.append(
    DbOperation(
        "$ -> $",
        [JsonbOperationParam(), JSON_FIELD_NAME_PARAM],
        JsonbReturnTypeSpec(),
        tags={TAG_JSONB_VALUE_ACCESS},
    )
)
JSONB_OPERATION_TYPES.append(
    DbOperation(
        "$ -> $",
        [JsonbOperationParam(), JSON_FIELD_INDEX_PARAM],
        JsonbReturnTypeSpec(),
        tags={TAG_JSONB_VALUE_ACCESS},
    )
)
JSONB_OPERATION_TYPES.append(
    DbOperation(
        "$ ->> $",
        [JsonbOperationParam(), JSON_FIELD_NAME_PARAM],
        StringReturnTypeSpec(),
        tags={TAG_JSONB_TO_TEXT, TAG_JSONB_VALUE_ACCESS},
    )
)
JSONB_OPERATION_TYPES.append(
    DbOperation(
        "$ ->> $",
        [JsonbOperationParam(), JSON_FIELD_INDEX_PARAM],
        StringReturnTypeSpec(),
        tags={TAG_JSONB_TO_TEXT, TAG_JSONB_VALUE_ACCESS},
    )
)
JSONB_OPERATION_TYPES.append(
    DbOperation(
        "$ #> $",
        [JsonbOperationParam(), JSON_PATH_PARAM],
        JsonbReturnTypeSpec(),
        tags={TAG_JSONB_VALUE_ACCESS},
    )
)
JSONB_OPERATION_TYPES.append(
    DbOperation(
        "$ #>> $",
        [JsonbOperationParam(), JSON_PATH_PARAM],
        StringReturnTypeSpec(),
        tags={TAG_JSONB_TO_TEXT, TAG_JSONB_VALUE_ACCESS},
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
        StringReturnTypeSpec(),
        tags={TAG_JSONB_TO_TEXT},
    )
)

JSONB_OPERATION_TYPES.append(
    DbFunction(
        "jsonb_typeof",
        [JsonbOperationParam()],
        StringReturnTypeSpec(),
    )
)

JSONB_OPERATION_TYPES.append(
    DbFunction(
        "jsonb_strip_nulls",
        [JsonbOperationParam()],
        JsonbReturnTypeSpec(),
    )
)

JSONB_OPERATION_TYPES.append(
    DbFunctionWithCustomPattern(
        "jsonb_agg",
        {2: "jsonb_agg($ ORDER BY row_index, $)"},
        [
            AnyOperationParam(include_record_type=False),
            SameOperationParam(index_of_previous_param=0),
        ],
        JsonbReturnTypeSpec(),
        is_aggregation=True,
        relevance=OperationRelevance.LOW,
        tags={TAG_JSONB_AGGREGATION},
        comment="generic variant without records",
    ),
)

JSONB_OPERATION_TYPES.append(
    DbFunctionWithCustomPattern(
        "jsonb_agg",
        {2: "jsonb_agg($ ORDER BY row_index, $)"},
        [RecordOperationParam(), SameOperationParam(index_of_previous_param=0)],
        JsonbReturnTypeSpec(),
        is_aggregation=True,
        tags={TAG_JSONB_AGGREGATION},
        comment="additional overlapping variant only for records",
    ),
)

JSONB_OPERATION_TYPES.append(
    DbFunctionWithCustomPattern(
        "jsonb_object_agg",
        {3: "jsonb_object_agg($, $ ORDER BY row_index, $)"},
        [
            AnyOperationParam(),
            AnyOperationParam(include_record_type=False),
            SameOperationParam(index_of_previous_param=0),
        ],
        JsonbReturnTypeSpec(),
        is_aggregation=True,
        relevance=OperationRelevance.LOW,
        tags={TAG_JSONB_AGGREGATION},
        comment="generic variant without record values",
    ),
)

JSONB_OPERATION_TYPES.append(
    DbFunctionWithCustomPattern(
        "jsonb_object_agg",
        {3: "jsonb_object_agg($, $ ORDER BY row_index, $)"},
        [
            AnyOperationParam(),
            RecordOperationParam(),
            SameOperationParam(index_of_previous_param=0),
        ],
        JsonbReturnTypeSpec(),
        is_aggregation=True,
        tags={TAG_JSONB_AGGREGATION},
        comment="additional overlapping variant only for records",
    ),
)

CREATE_JSON_WITH_GEO_DATA_OP = DbOperation(
    dedent(
        """concat('{
              "@timestamp":"', $, '",
              "latitude":', $, ',
              "longitude":', $, ',
              "location":[', $, ',', $, ']
            }')::JSONB"""
    ),
    [
        DateTimeOperationParam(support_time=False),
        NumericOperationParam(),
        NumericOperationParam(),
        NumericOperationParam(),
        NumericOperationParam(),
    ],
    JsonbReturnTypeSpec(),
    comment="JSONB value with geo data",
)
CREATE_JSON_WITH_GEO_DATA_OP.added_characteristics.add(
    ExpressionCharacteristics.JSON_WITH_GEO_DATA
)
JSONB_OPERATION_TYPES.append(CREATE_JSON_WITH_GEO_DATA_OP)

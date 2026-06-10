# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.input_data.operations.string_operations_provider import (
    TAG_REGEX,
)
from materialize.output_consistency.input_data.params.array_operation_param import (
    ArrayOperationParam,
)
from materialize.output_consistency.input_data.params.date_time_operation_param import (
    DateTimeOperationParam,
    TimeIntervalOperationParam,
)
from materialize.output_consistency.input_data.params.enum_constant_operation_params import (
    REGEX_FLAG_OPTIONAL_PARAM,
    REGEX_PARAM,
    REGEX_PARAM_WITH_GROUP,
)
from materialize.output_consistency.input_data.params.list_operation_param import (
    ListOperationParam,
)
from materialize.output_consistency.input_data.params.map_operation_param import (
    MapOperationParam,
)
from materialize.output_consistency.input_data.params.number_operation_param import (
    NumericOperationParam,
)
from materialize.output_consistency.input_data.params.string_operation_param import (
    StringOperationParam,
)
from materialize.output_consistency.input_data.return_specs.collection_entry_return_spec import (
    CollectionEntryReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.date_time_return_spec import (
    DateTimeReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.number_return_spec import (
    NumericReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.record_return_spec import (
    RecordReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.string_return_spec import (
    StringReturnTypeSpec,
)
from materialize.output_consistency.input_data.types.date_time_types_provider import (
    TIMESTAMP_TYPE_IDENTIFIER,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbFunctionWithCustomPattern,
    DbOperationOrFunction,
    OperationRelevance,
)

TAG_TABLE_FUNCTION_WITH_NON_NUMERIC_SORT_ORDER = (
    "table_function_with_non_numeric_sort_order"
)

TABLE_OPERATION_TYPES: list[DbOperationOrFunction] = []

TABLE_OPERATION_TYPES.append(
    DbFunctionWithCustomPattern(
        "generate_series",
        {2: "generate_series($, min($, 30))", 3: "generate_series($, min($, 30), $)"},
        [
            NumericOperationParam(
                only_int_type=True,
                incompatibilities={
                    ExpressionCharacteristics.MAX_VALUE,
                    ExpressionCharacteristics.LARGE_VALUE,
                },
            ),
            NumericOperationParam(
                only_int_type=True,
                incompatibilities={
                    ExpressionCharacteristics.MAX_VALUE,
                    ExpressionCharacteristics.LARGE_VALUE,
                },
            ),
            NumericOperationParam(
                only_int_type=True,
                incompatibilities={
                    ExpressionCharacteristics.MAX_VALUE,
                    ExpressionCharacteristics.LARGE_VALUE,
                },
            ),
        ],
        NumericReturnTypeSpec(only_integer=True),
        is_table_function=True,
        relevance=OperationRelevance.LOW,
    ),
)

TABLE_OPERATION_TYPES.append(
    DbFunction(
        "generate_series",
        [
            DateTimeOperationParam(
                incompatibilities={ExpressionCharacteristics.MAX_VALUE}
            ),
            DateTimeOperationParam(
                incompatibilities={ExpressionCharacteristics.MAX_VALUE}
            ),
            TimeIntervalOperationParam(
                incompatibilities={ExpressionCharacteristics.MAX_VALUE}
            ),
        ],
        DateTimeReturnTypeSpec(TIMESTAMP_TYPE_IDENTIFIER),
        is_table_function=True,
        relevance=OperationRelevance.LOW,
        # this is likely to take down mz with aggressive values
        is_enabled=False,
    ),
)


TABLE_OPERATION_TYPES.append(
    DbFunction(
        "generate_subscripts",
        [
            ArrayOperationParam(),
            NumericOperationParam(only_int_type=True),
        ],
        NumericReturnTypeSpec(only_integer=True),
        is_table_function=True,
        comment="Returns indices of the specified array dimension",
        tags={TAG_TABLE_FUNCTION_WITH_NON_NUMERIC_SORT_ORDER},
    ),
)

TABLE_OPERATION_TYPES.append(
    DbFunction(
        "regexp_extract",
        [
            REGEX_PARAM_WITH_GROUP,
            StringOperationParam(),
        ],
        StringReturnTypeSpec(),
        is_table_function=True,
        tags={TAG_REGEX, TAG_TABLE_FUNCTION_WITH_NON_NUMERIC_SORT_ORDER},
    ),
)

TABLE_OPERATION_TYPES.append(
    DbFunction(
        "regexp_split_to_table",
        [StringOperationParam(), REGEX_PARAM, REGEX_FLAG_OPTIONAL_PARAM],
        StringReturnTypeSpec(),
        is_table_function=True,
        # It is not possible to specify the order so that inconsistencies are inevitable.
        is_pg_compatible=False,
        tags={TAG_REGEX, TAG_TABLE_FUNCTION_WITH_NON_NUMERIC_SORT_ORDER},
    ),
)

TABLE_OPERATION_TYPES.append(
    DbFunction(
        "unnest",
        [
            ArrayOperationParam(),
        ],
        CollectionEntryReturnTypeSpec(param_index_to_take_type=0),
        is_table_function=True,
        tags={TAG_TABLE_FUNCTION_WITH_NON_NUMERIC_SORT_ORDER},
    ),
)

TABLE_OPERATION_TYPES.append(
    DbFunction(
        "unnest",
        [
            ListOperationParam(),
        ],
        CollectionEntryReturnTypeSpec(param_index_to_take_type=0),
        is_table_function=True,
        tags={TAG_TABLE_FUNCTION_WITH_NON_NUMERIC_SORT_ORDER},
    ),
)

TABLE_OPERATION_TYPES.append(
    DbFunction(
        "unnest",
        [
            MapOperationParam(),
        ],
        RecordReturnTypeSpec(),
        is_table_function=True,
        tags={TAG_TABLE_FUNCTION_WITH_NON_NUMERIC_SORT_ORDER},
    ),
)

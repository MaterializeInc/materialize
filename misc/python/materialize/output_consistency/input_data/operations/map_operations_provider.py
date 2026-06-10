# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.input_data.params.any_operation_param import (
    AnyOperationParam,
)
from materialize.output_consistency.input_data.params.array_operation_param import (
    ArrayOperationParam,
)
from materialize.output_consistency.input_data.params.enum_constant_operation_params import (
    MAP_FIELD_NAME_PARAM,
)
from materialize.output_consistency.input_data.params.map_operation_param import (
    MapOperationParam,
)
from materialize.output_consistency.input_data.params.record_operation_param import (
    RecordOperationParam,
)
from materialize.output_consistency.input_data.params.row_indices_param import (
    RowIndicesParam,
)
from materialize.output_consistency.input_data.params.same_operation_param import (
    SameOperationParam,
)
from materialize.output_consistency.input_data.params.string_operation_param import (
    StringOperationParam,
)
from materialize.output_consistency.input_data.return_specs.boolean_return_spec import (
    BooleanReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.collection_entry_return_spec import (
    CollectionEntryReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.map_return_spec import (
    MapReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.number_return_spec import (
    NumericReturnTypeSpec,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbFunctionWithCustomPattern,
    DbOperation,
    DbOperationOrFunction,
)

# all operations will be disabled for Postgres at the bottom
MAP_OPERATION_TYPES: list[DbOperationOrFunction] = []

MAP_OPERATION_TYPES.append(
    DbOperation(
        "$ -> $",
        [MapOperationParam(), MAP_FIELD_NAME_PARAM],
        CollectionEntryReturnTypeSpec(param_index_to_take_type=0),
    )
)
MAP_OPERATION_TYPES.append(
    DbOperation(
        "$ @> $",
        [MapOperationParam(), MapOperationParam()],
        BooleanReturnTypeSpec(),
        comment="LHS contains RHS",
    )
)
MAP_OPERATION_TYPES.append(
    DbOperation(
        "$ <@ $",
        [MapOperationParam(), MapOperationParam()],
        BooleanReturnTypeSpec(),
        comment="RHS contains LHS",
    )
)
MAP_OPERATION_TYPES.append(
    DbOperation(
        "$ ? $",
        [MapOperationParam(), MAP_FIELD_NAME_PARAM],
        BooleanReturnTypeSpec(),
    )
)

MAP_OPERATION_TYPES.append(
    DbOperation(
        "MAP[$ => $]",
        [MAP_FIELD_NAME_PARAM, AnyOperationParam()],
        MapReturnTypeSpec(),
        comment="using a set of specified keys",
    )
)
MAP_OPERATION_TYPES.append(
    DbOperation(
        "MAP[$ => $]",
        [StringOperationParam(only_type_text=True), AnyOperationParam()],
        MapReturnTypeSpec(),
        comment="using arbitrary text values as keys",
    )
)

MAP_OPERATION_TYPES.append(
    DbFunction(
        "map_length",
        [MapOperationParam()],
        NumericReturnTypeSpec(only_integer=True),
    )
)
MAP_OPERATION_TYPES.append(
    DbFunctionWithCustomPattern(
        "map_agg",
        {4: "map_agg($, $ ORDER BY $, $)"},
        [
            # key
            StringOperationParam(only_type_text=True),
            # value
            AnyOperationParam(),
            RowIndicesParam(index_of_param_to_share_data_source=0),
            # order within aggregated values
            SameOperationParam(index_of_previous_param=1),
        ],
        MapReturnTypeSpec(),
        is_aggregation=True,
    )
)

MAP_OPERATION_TYPES.append(
    DbOperation(
        "$ ?& $",
        [
            MapOperationParam(),
            ArrayOperationParam(value_type_category=DataTypeCategory.STRING),
        ],
        BooleanReturnTypeSpec(),
        comment="contains all keys",
    )
)
MAP_OPERATION_TYPES.append(
    DbOperation(
        "$ ?| $",
        [
            MapOperationParam(),
            ArrayOperationParam(value_type_category=DataTypeCategory.STRING),
        ],
        BooleanReturnTypeSpec(),
        comment="contains any key",
    )
)

MAP_OPERATION_TYPES.append(
    DbFunction(
        "map_build",
        [
            RecordOperationParam(),
        ],
        MapReturnTypeSpec(map_value_type_category=DataTypeCategory.UNDETERMINED),
    )
)

for operation in MAP_OPERATION_TYPES:
    # Postgres does not support the map type
    operation.is_pg_compatible = False

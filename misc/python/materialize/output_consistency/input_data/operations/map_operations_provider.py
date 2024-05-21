# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mz_version import MzVersion
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
from materialize.output_consistency.input_data.params.text_operation_param import (
    TextOperationParam,
)
from materialize.output_consistency.input_data.return_specs.boolean_return_spec import (
    BooleanReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.map_return_spec import (
    MapReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.map_value_return_spec import (
    DynamicMapValueReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.number_return_spec import (
    NumericReturnTypeSpec,
)
from materialize.output_consistency.operation.operation import (
    DbFunction,
    DbOperation,
    DbOperationOrFunction,
)

# all operations will be disabled for Postgres at the bottom
MAP_OPERATION_TYPES: list[DbOperationOrFunction] = []

MAP_OPERATION_TYPES.append(
    DbOperation(
        "$ -> $",
        [MapOperationParam(), MAP_FIELD_NAME_PARAM],
        DynamicMapValueReturnTypeSpec(),
    )
)
MAP_OPERATION_TYPES.append(
    DbOperation(
        "$ @> $",
        [MapOperationParam(), AnyOperationParam()],
        BooleanReturnTypeSpec(),
    )
)
MAP_OPERATION_TYPES.append(
    DbOperation(
        "$ <@ $",
        [MapOperationParam(), AnyOperationParam()],
        BooleanReturnTypeSpec(),
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
        since_mz_version=MzVersion.parse_mz("v0.100.0"),
    )
)
MAP_OPERATION_TYPES.append(
    DbOperation(
        "MAP[$ => $]",
        [TextOperationParam(), AnyOperationParam()],
        MapReturnTypeSpec(),
        comment="using arbitrary text values as keys",
        since_mz_version=MzVersion.parse_mz("v0.100.0"),
    )
)

MAP_OPERATION_TYPES.append(
    DbFunction(
        "map_length",
        [MapOperationParam()],
        NumericReturnTypeSpec(only_integer=True),
    )
)
# TODO: use with multiple keys and values
MAP_OPERATION_TYPES.append(
    DbFunction(
        "map_agg",
        [TextOperationParam(), AnyOperationParam()],
        MapReturnTypeSpec(),
    )
)

MAP_OPERATION_TYPES.append(
    DbOperation(
        "$ ?& $",
        [
            MapOperationParam(),
            ArrayOperationParam(value_type_category=DataTypeCategory.TEXT),
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
            ArrayOperationParam(value_type_category=DataTypeCategory.TEXT),
        ],
        BooleanReturnTypeSpec(),
        comment="contains any key",
    )
)

# TODO: map_build operates on records

for operation in MAP_OPERATION_TYPES:
    # Postgres does not support the map type
    operation.is_pg_compatible = False

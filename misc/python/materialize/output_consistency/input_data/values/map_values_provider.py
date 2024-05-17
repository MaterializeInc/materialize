# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.data_type.data_type_with_values import (
    DataTypeWithValues,
)
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.input_data.types.map_type_provider import (
    MAP_DATA_TYPES,
    MapDataType,
    as_sql_map_string,
)

VALUES_PER_MAP_DATA_TYPE: dict[MapDataType, DataTypeWithValues] = dict()

for map_data_type in MAP_DATA_TYPES:
    values_of_type = DataTypeWithValues(map_data_type)
    VALUES_PER_MAP_DATA_TYPE[map_data_type] = values_of_type

    values_of_type.add_raw_value(
        as_sql_map_string(
            sql_value_by_key=dict(),
        ),
        "EMPTY",
        {ExpressionCharacteristics.COLLECTION_EMPTY},
    )

    # key values should be specified in MAP_FIELD_NAME_PARAM in misc/python/materialize/output_consistency/input_data/params/enum_constant_operation_params.py
    MAP_1_CONTENT = dict()
    MAP_1_CONTENT["n"] = "NULL"
    MAP_1_CONTENT["a"] = map_data_type.entry_value_1
    MAP_1_CONTENT["A"] = map_data_type.entry_value_2
    MAP_1_CONTENT["b"] = map_data_type.entry_value_2

    values_of_type.add_raw_value(
        as_sql_map_string(
            sql_value_by_key=MAP_1_CONTENT,
        ),
        "VAL",
        set(),
    )

    MAP_W_DUP_KEYS_CONTENT = dict()
    MAP_W_DUP_KEYS_CONTENT["a"] = map_data_type.entry_value_1
    MAP_W_DUP_KEYS_CONTENT["a"] = map_data_type.entry_value_2

    values_of_type.add_raw_value(
        as_sql_map_string(
            sql_value_by_key=MAP_W_DUP_KEYS_CONTENT,
        ),
        "DUP_KEYS",
        {ExpressionCharacteristics.MAP_WITH_DUP_KEYS},
    )

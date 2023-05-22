# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import Dict

from materialize.output_consistency.data_type.data_type_with_values import (
    DataTypeWithValues,
)
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.input_data.types.number_types_provider import (
    DOUBLE_TYPE,
    INT8_TYPE,
    NUMERIC_DATA_TYPES,
    REAL_TYPE,
    UINT4_TYPE,
    UINT8_TYPE,
    NumberDataType,
)

VALUES_PER_NUMERIC_DATA_TYPE: Dict[NumberDataType, DataTypeWithValues] = dict()

for num_data_type in NUMERIC_DATA_TYPES:
    values_of_type = DataTypeWithValues(num_data_type)
    VALUES_PER_NUMERIC_DATA_TYPE[num_data_type] = values_of_type

    values_of_type.add_raw_value("NULL", "NULL", {ExpressionCharacteristics.NULL})
    values_of_type.add_raw_value("0", "ZERO", {ExpressionCharacteristics.ZERO})
    values_of_type.add_raw_value(
        "1",
        "ONE",
        {
            ExpressionCharacteristics.ONE,
            ExpressionCharacteristics.TINY_VALUE,
            ExpressionCharacteristics.NON_EMPTY,
        },
    )
    values_of_type.add_raw_value(
        num_data_type.max_value,
        "MAX",
        {ExpressionCharacteristics.MAX_VALUE, ExpressionCharacteristics.NON_EMPTY},
    )

    if num_data_type.is_signed and num_data_type.max_negative_value is not None:
        values_of_type.add_raw_value(
            f"{num_data_type.max_negative_value}",
            "NEG_MAX",
            {
                ExpressionCharacteristics.NEGATIVE,
                ExpressionCharacteristics.MAX_VALUE,
                ExpressionCharacteristics.NON_EMPTY,
            },
        )

    if num_data_type.is_decimal:
        values_of_type.add_raw_value(
            num_data_type.tiny_value,
            "TINY",
            {
                ExpressionCharacteristics.TINY_VALUE,
                ExpressionCharacteristics.NON_EMPTY,
                ExpressionCharacteristics.DECIMAL,
            },
        )

for type_definition, values_of_type in VALUES_PER_NUMERIC_DATA_TYPE.items():
    if type_definition.is_decimal:
        values_of_type.add_characteristic_to_all_values(
            ExpressionCharacteristics.DECIMAL_OR_FLOAT_TYPED
        )

    for value in values_of_type.raw_values:
        if ExpressionCharacteristics.MAX_VALUE in value.characteristics:
            value.characteristics.add(ExpressionCharacteristics.LARGE_VALUE)

VALUES_PER_NUMERIC_DATA_TYPE[UINT4_TYPE].add_characteristic_to_all_values(
    ExpressionCharacteristics.LARGER_THAN_INT4_TYPED
)
VALUES_PER_NUMERIC_DATA_TYPE[INT8_TYPE].add_characteristic_to_all_values(
    ExpressionCharacteristics.LARGER_THAN_INT4_TYPED
)
VALUES_PER_NUMERIC_DATA_TYPE[UINT8_TYPE].add_characteristic_to_all_values(
    ExpressionCharacteristics.LARGER_THAN_INT4_TYPED
)
VALUES_PER_NUMERIC_DATA_TYPE[REAL_TYPE].add_characteristic_to_all_values(
    ExpressionCharacteristics.FLOAT_TYPED
)
VALUES_PER_NUMERIC_DATA_TYPE[DOUBLE_TYPE].add_characteristic_to_all_values(
    ExpressionCharacteristics.FLOAT_TYPED
)

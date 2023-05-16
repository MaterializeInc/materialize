# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.data_type.data_type import NumberDataType
from materialize.output_consistency.data_type.data_type_provider import (
    NUMERIC_DATA_TYPES,
)
from materialize.output_consistency.data_type.number_data_type_provider import (
    DOUBLE_TYPE,
    INT8_TYPE,
    REAL_TYPE,
    UINT4_TYPE,
    UINT8_TYPE,
)
from materialize.output_consistency.data_values.data_type_with_values import (
    DataTypeWithValues,
)
from materialize.output_consistency.data_values.value_characteristics import (
    ValueCharacteristics,
)

VALUES_PER_NUMERIC_DATA_TYPE: dict[NumberDataType, DataTypeWithValues] = dict()

for num_data_type in NUMERIC_DATA_TYPES:
    values_of_type = DataTypeWithValues(num_data_type)
    VALUES_PER_NUMERIC_DATA_TYPE[num_data_type] = values_of_type

    values_of_type.add_raw_value("NULL", "NULL", {ValueCharacteristics.NULL})
    values_of_type.add_raw_value("0", "ZERO", {ValueCharacteristics.ZERO})
    values_of_type.add_raw_value(
        "1",
        "ONE",
        {
            ValueCharacteristics.ONE,
            ValueCharacteristics.TINY_VALUE,
            ValueCharacteristics.NON_EMPTY,
        },
    )
    values_of_type.add_raw_value(
        num_data_type.max_value,
        "MAX",
        {ValueCharacteristics.MAX_VALUE, ValueCharacteristics.NON_EMPTY},
    )

    if num_data_type.is_signed and num_data_type.max_negative_value is not None:
        values_of_type.add_raw_value(
            f"{num_data_type.max_negative_value}",
            "NEG_MAX",
            {
                ValueCharacteristics.NEGATIVE,
                ValueCharacteristics.MAX_VALUE,
                ValueCharacteristics.NON_EMPTY,
            },
        )

    if num_data_type.is_decimal:
        values_of_type.add_raw_value(
            num_data_type.tiny_value,
            "TINY",
            {
                ValueCharacteristics.TINY_VALUE,
                ValueCharacteristics.NON_EMPTY,
                ValueCharacteristics.DECIMAL,
            },
        )

for type_definition, values_of_type in VALUES_PER_NUMERIC_DATA_TYPE.items():
    if type_definition.is_decimal:
        values_of_type.add_characteristic_to_all_values(
            ValueCharacteristics.DECIMAL_OR_FLOAT_TYPED
        )

    for value in values_of_type.raw_values:
        if ValueCharacteristics.MAX_VALUE in value.characteristics:
            value.characteristics.add(ValueCharacteristics.LARGE_VALUE)

VALUES_PER_NUMERIC_DATA_TYPE[UINT4_TYPE].add_characteristic_to_all_values(
    ValueCharacteristics.LARGER_THAN_INT4_TYPED
)
VALUES_PER_NUMERIC_DATA_TYPE[INT8_TYPE].add_characteristic_to_all_values(
    ValueCharacteristics.LARGER_THAN_INT4_TYPED
)
VALUES_PER_NUMERIC_DATA_TYPE[UINT8_TYPE].add_characteristic_to_all_values(
    ValueCharacteristics.LARGER_THAN_INT4_TYPED
)
VALUES_PER_NUMERIC_DATA_TYPE[REAL_TYPE].add_characteristic_to_all_values(
    ValueCharacteristics.FLOAT_TYPED
)
VALUES_PER_NUMERIC_DATA_TYPE[DOUBLE_TYPE].add_characteristic_to_all_values(
    ValueCharacteristics.FLOAT_TYPED
)

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.data_type.data_type import DataType, NumberDataType
from materialize.output_consistency.data_type.value_characteristics import (
    ValueCharacteristics,
)

int2_type = NumberDataType(
    "INT2", "INT2", is_signed=True, is_decimal=False, tiny_value="1", max_value="32767"
)
int4_type = NumberDataType(
    "INT4",
    "INT4",
    is_signed=True,
    is_decimal=False,
    tiny_value="1",
    max_value="2147483647",
)
int8_type = NumberDataType(
    "INT8",
    "INT8",
    is_signed=True,
    is_decimal=False,
    tiny_value="1",
    max_value="9223372036854775807",
)

uint2_type = NumberDataType(
    "UINT2",
    "UINT2",
    is_signed=False,
    is_decimal=False,
    tiny_value="1",
    max_value="65535",
)
uint4_type = NumberDataType(
    "UINT4",
    "UINT4",
    is_signed=False,
    is_decimal=False,
    tiny_value="1",
    max_value="4294967295",
)
uint8_type = NumberDataType(
    "UINT8",
    "UINT8",
    is_signed=False,
    is_decimal=False,
    tiny_value="1",
    max_value="18446744073709551615",
)

# configurable decimal digits
decimal39_0_type = NumberDataType(
    "DECIMAL_39_0",
    "DECIMAL(39)",
    is_signed=True,
    is_decimal=True,
    tiny_value="1",
    max_value="999999999999999999999999999999999999999",
)
decimal39_4_type = NumberDataType(
    "DECIMAL_39_4",
    "DECIMAL(39,4)",
    is_signed=True,
    is_decimal=True,
    tiny_value="0.0001",
    max_value="99999999999999999999999999999999999.9999",
)

real_type = NumberDataType(
    "REAL",
    "REAL",
    is_signed=True,
    is_decimal=True,
    tiny_value="0.000000000000000000000000000000000000001",
    max_value="99999999999999999999999999999999999999",
)
double_type = NumberDataType(
    "DOUBLE",
    "DOUBLE",
    is_signed=True,
    is_decimal=True,
    tiny_value="0.000000000000000000000000000000000000001",
    max_value="999999999999999999999999999999999999999",
)

NUMERIC_DATA_TYPES: list[NumberDataType] = [
    int2_type,
    int4_type,
    int8_type,
    uint2_type,
    uint4_type,
    uint8_type,
    decimal39_0_type,
    decimal39_4_type,
    real_type,
    double_type,
]
DATA_TYPES: list[DataType] = []
DATA_TYPES.extend(NUMERIC_DATA_TYPES)

for num_data_type in NUMERIC_DATA_TYPES:
    num_data_type.add_raw_value("NULL", "NULL", {ValueCharacteristics.NULL})
    num_data_type.add_raw_value("0", "ZERO", {ValueCharacteristics.ZERO})
    num_data_type.add_raw_value(
        "1", "ONE", {ValueCharacteristics.ONE, ValueCharacteristics.NON_EMPTY}
    )
    num_data_type.add_raw_value(
        num_data_type.max_value,
        "MAX",
        {ValueCharacteristics.MAX_VALUE, ValueCharacteristics.NON_EMPTY},
    )

    if num_data_type.is_signed:
        num_data_type.add_raw_value(
            f"-{num_data_type.max_value}",
            "NEG_MAX",
            {
                ValueCharacteristics.NEGATIVE,
                ValueCharacteristics.MAX_VALUE,
                ValueCharacteristics.NON_EMPTY,
            },
        )

    if num_data_type.is_decimal:
        num_data_type.add_raw_value(
            num_data_type.tiny_value,
            "DECIMAL",
            {ValueCharacteristics.DECIMAL, ValueCharacteristics.NON_EMPTY},
        )

for num_data_type in NUMERIC_DATA_TYPES:
    if num_data_type.is_decimal:
        for raw_value in num_data_type.raw_values:
            raw_value.characteristics.add(ValueCharacteristics.DECIMAL_TYPED)

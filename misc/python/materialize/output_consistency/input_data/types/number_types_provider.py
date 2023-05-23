# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List, Optional

from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory


class NumberDataType(DataType):
    def __init__(
        self,
        identifier: str,
        type_name: str,
        is_signed: bool,
        is_decimal: bool,
        tiny_value: str,
        max_value: str,
        max_negative_value: Optional[str],
    ):
        super().__init__(identifier, type_name, DataTypeCategory.NUMERIC)
        self.is_signed = is_signed
        self.is_decimal = is_decimal
        self.tiny_value = tiny_value
        self.max_value = max_value
        self.max_negative_value = max_negative_value


INT2_TYPE = NumberDataType(
    "INT2",
    "INT2",
    is_signed=True,
    is_decimal=False,
    tiny_value="1",
    max_value="32767",
    max_negative_value="-32768",
)
INT4_TYPE = NumberDataType(
    "INT4",
    "INT4",
    is_signed=True,
    is_decimal=False,
    tiny_value="1",
    max_value="2147483647",
    max_negative_value="-2147483648",
)
INT8_TYPE = NumberDataType(
    "INT8",
    "INT8",
    is_signed=True,
    is_decimal=False,
    tiny_value="1",
    max_value="9223372036854775807",
    max_negative_value="-9223372036854775808",
)

UINT2_TYPE = NumberDataType(
    "UINT2",
    "UINT2",
    is_signed=False,
    is_decimal=False,
    tiny_value="1",
    max_value="65535",
    max_negative_value=None,
)
UINT4_TYPE = NumberDataType(
    "UINT4",
    "UINT4",
    is_signed=False,
    is_decimal=False,
    tiny_value="1",
    max_value="4294967295",
    max_negative_value=None,
)
UINT8_TYPE = NumberDataType(
    "UINT8",
    "UINT8",
    is_signed=False,
    is_decimal=False,
    tiny_value="1",
    max_value="18446744073709551615",
    max_negative_value=None,
)

# configurable decimal digits
DECIMAL39_0_TYPE = NumberDataType(
    "DECIMAL_39_0",
    "DECIMAL(39)",
    is_signed=True,
    is_decimal=True,
    tiny_value="1",
    max_value="999999999999999999999999999999999999999",
    max_negative_value="-999999999999999999999999999999999999999",
)
DECIMAL39_8_TYPE = NumberDataType(
    "DECIMAL_39_8",
    "DECIMAL(39,8)",
    is_signed=True,
    is_decimal=True,
    tiny_value="0.00000001",
    max_value="9999999999999999999999999999999.99999999",
    max_negative_value="-9999999999999999999999999999999.99999999",
)

REAL_TYPE = NumberDataType(
    "REAL",
    "REAL",
    is_signed=True,
    is_decimal=True,
    tiny_value="0.000000000000000000000000000000000000001",
    max_value="99999999999999999999999999999999999999",
    max_negative_value="99999999999999999999999999999999999999",
)
DOUBLE_TYPE = NumberDataType(
    "DOUBLE",
    "DOUBLE",
    is_signed=True,
    is_decimal=True,
    tiny_value="0.000000000000000000000000000000000000001",
    max_value="999999999999999999999999999999999999999",
    max_negative_value="-999999999999999999999999999999999999999",
)

SIGNED_INT_TYPES: List[NumberDataType] = [
    INT2_TYPE,
    INT4_TYPE,
    INT8_TYPE,
]

UNSIGNED_INT_TYPES: List[NumberDataType] = [
    UINT2_TYPE,
    UINT4_TYPE,
    UINT8_TYPE,
]

FLOAT_OR_DECIMAL_DATA_TYPES: List[NumberDataType] = [
    DECIMAL39_0_TYPE,
    DECIMAL39_8_TYPE,
    REAL_TYPE,
    DOUBLE_TYPE,
]

NUMERIC_DATA_TYPES: List[NumberDataType] = []
NUMERIC_DATA_TYPES.extend(SIGNED_INT_TYPES)
NUMERIC_DATA_TYPES.extend(UNSIGNED_INT_TYPES)
NUMERIC_DATA_TYPES.extend(FLOAT_OR_DECIMAL_DATA_TYPES)

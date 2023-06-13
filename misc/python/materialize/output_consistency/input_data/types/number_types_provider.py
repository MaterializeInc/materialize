# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List, Optional, Set

from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.input_data.return_specs.number_return_spec import (
    NumericReturnTypeSpec,
)
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec


class NumberDataType(DataType):
    def __init__(
        self,
        identifier: str,
        type_name: str,
        is_signed: bool,
        is_decimal: bool,
        smallest_value: str,
        max_value: str,
        max_negative_value: Optional[str],
        further_tiny_dec_values: Optional[Set[str]] = None,
        is_floating_point_type: bool = False,
    ):
        super().__init__(identifier, type_name, DataTypeCategory.NUMERIC)
        self.is_signed = is_signed
        self.is_decimal = is_decimal
        self.smallest_value = smallest_value
        self.max_value = max_value
        self.max_negative_value = max_negative_value
        self.further_tiny_dec_values = (
            further_tiny_dec_values if further_tiny_dec_values is not None else set()
        )
        self.is_floating_point_type = is_floating_point_type

    def resolve_return_type_spec(
        self, characteristics: Set[ExpressionCharacteristics]
    ) -> ReturnTypeSpec:
        return NumericReturnTypeSpec(
            only_integer=not self.is_decimal
            or ExpressionCharacteristics.DECIMAL not in characteristics,
        )


INT2_TYPE_IDENTIFIER = "INT2"
INT4_TYPE_IDENTIFIER = "INT4"
INT8_TYPE_IDENTIFIER = "INT8"
UINT2_TYPE_IDENTIFIER = "UINT2"
UINT4_TYPE_IDENTIFIER = "UINT4"
UINT8_TYPE_IDENTIFIER = "UINT8"
DECIMAL_39_0_TYPE_IDENTIFIER = "DECIMAL_39_0"
DECIMAL_39_8_TYPE_IDENTIFIER = "DECIMAL_39_8"
REAL_TYPE_IDENTIFIER = "REAL"
DOUBLE_TYPE_IDENTIFIER = "DOUBLE"

INT2_TYPE = NumberDataType(
    INT2_TYPE_IDENTIFIER,
    "INT2",
    is_signed=True,
    is_decimal=False,
    smallest_value="1",
    max_value="32767",
    max_negative_value="-32768",
)
INT4_TYPE = NumberDataType(
    INT4_TYPE_IDENTIFIER,
    "INT4",
    is_signed=True,
    is_decimal=False,
    smallest_value="1",
    max_value="2147483647",
    max_negative_value="-2147483648",
)
INT8_TYPE = NumberDataType(
    INT8_TYPE_IDENTIFIER,
    "INT8",
    is_signed=True,
    is_decimal=False,
    smallest_value="1",
    max_value="9223372036854775807",
    max_negative_value="-9223372036854775808",
)

UINT2_TYPE = NumberDataType(
    UINT2_TYPE_IDENTIFIER,
    "UINT2",
    is_signed=False,
    is_decimal=False,
    smallest_value="1",
    max_value="65535",
    max_negative_value=None,
)
UINT4_TYPE = NumberDataType(
    UINT4_TYPE_IDENTIFIER,
    "UINT4",
    is_signed=False,
    is_decimal=False,
    smallest_value="1",
    max_value="4294967295",
    max_negative_value=None,
)
UINT8_TYPE = NumberDataType(
    UINT8_TYPE_IDENTIFIER,
    "UINT8",
    is_signed=False,
    is_decimal=False,
    smallest_value="1",
    max_value="18446744073709551615",
    max_negative_value=None,
)

# configurable decimal digits
DECIMAL39_0_TYPE = NumberDataType(
    DECIMAL_39_0_TYPE_IDENTIFIER,
    "DECIMAL(39)",
    is_signed=True,
    is_decimal=True,
    smallest_value="1",
    max_value="999999999999999999999999999999999999999",
    max_negative_value="-999999999999999999999999999999999999999",
)
DECIMAL39_8_TYPE = NumberDataType(
    DECIMAL_39_8_TYPE_IDENTIFIER,
    "DECIMAL(39,8)",
    is_signed=True,
    is_decimal=True,
    smallest_value="0.00000001",
    max_value="9999999999999999999999999999999.99999999",
    max_negative_value="-9999999999999999999999999999999.99999999",
    further_tiny_dec_values={"0.49999999", "0.50000001", "0.99999999", "1.00000001"},
)

REAL_TYPE = NumberDataType(
    REAL_TYPE_IDENTIFIER,
    "REAL",
    is_signed=True,
    is_decimal=True,
    smallest_value="0.000000000000000000000000000000000000001",
    max_value="99999999999999999999999999999999999999",
    max_negative_value="99999999999999999999999999999999999999",
    further_tiny_dec_values={
        "0.499999999999999999999999999999999999999",
        "0.500000000000000000000000000000000000001",
        "0.999999999999999999999999999999999999999",
        "1.000000000000000000000000000000000000001",
    },
    is_floating_point_type=True,
)
DOUBLE_TYPE = NumberDataType(
    DOUBLE_TYPE_IDENTIFIER,
    "DOUBLE",
    is_signed=True,
    is_decimal=True,
    smallest_value="0.000000000000000000000000000000000000001",
    max_value="999999999999999999999999999999999999999",
    max_negative_value="-999999999999999999999999999999999999999",
    further_tiny_dec_values={
        "0.499999999999999999999999999999999999999",
        "0.500000000000000000000000000000000000001",
        "0.999999999999999999999999999999999999999",
        "1.000000000000000000000000000000000000001",
    },
    is_floating_point_type=True,
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

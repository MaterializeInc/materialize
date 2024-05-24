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
from materialize.output_consistency.input_data.types.string_type_provider import (
    STRING_DATA_TYPES,
    StringDataType,
)

VALUES_PER_STRING_DATA_TYPE: dict[StringDataType, DataTypeWithValues] = dict()

for string_data_type in STRING_DATA_TYPES:
    values_of_type = DataTypeWithValues(string_data_type)
    VALUES_PER_STRING_DATA_TYPE[string_data_type] = values_of_type

    values_of_type.add_raw_value(
        "''",
        "EMPTY",
        {ExpressionCharacteristics.STRING_EMPTY},
    )
    values_of_type.add_raw_value("'a'", "VAL_1", set())

    if not string_data_type.only_few_values:
        values_of_type.add_raw_value("'abc'", "VAL_2", set())
        values_of_type.add_raw_value("'xAAx'", "VAL_3", set())
        values_of_type.add_raw_value("'0*a*B_c-dE.$=#(%)?!'", "VAL_4", set())
        values_of_type.add_raw_value("'ff00aa'", "HEX_VAL", set())

    values_of_type.add_raw_value(
        "'äÖüß'", "VAL_5", {ExpressionCharacteristics.STRING_WITH_ESZETT}
    )
    values_of_type.add_raw_value(
        "' mAA m\n\t '",
        "VAL_W_SPACES",
        {ExpressionCharacteristics.STRING_WITH_SPECIAL_SPACE_CHARS},
    )

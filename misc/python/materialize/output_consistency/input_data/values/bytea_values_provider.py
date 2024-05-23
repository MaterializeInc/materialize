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
from materialize.output_consistency.input_data.types.bytea_type_provider import (
    BYTEA_DATA_TYPE,
)

BYTEA_DATA_TYPE_WITH_VALUES = DataTypeWithValues(BYTEA_DATA_TYPE)

BYTEA_DATA_TYPE_WITH_VALUES.add_raw_value(
    "'\\x'",
    "EMPTY",
    {ExpressionCharacteristics.STRING_EMPTY},
)
# space character
BYTEA_DATA_TYPE_WITH_VALUES.add_raw_value("'\\x20'", "VAL_SPACE", set())
# string 'hello'
BYTEA_DATA_TYPE_WITH_VALUES.add_raw_value("'\\x68656c6c6f'", "VAL_1", set())
# string 'hello' with space and emoticon
BYTEA_DATA_TYPE_WITH_VALUES.add_raw_value(
    "'\\x68656c6c6f20f09f918b'",
    "VAL_2",
    {ExpressionCharacteristics.STRING_WITH_SPECIAL_NON_SPACE_CHARS},
)
# (R) symbol as string
BYTEA_DATA_TYPE_WITH_VALUES.add_raw_value(
    "'\\xc2ae'",
    "VAL_3",
    {ExpressionCharacteristics.STRING_WITH_SPECIAL_NON_SPACE_CHARS},
)
# two Japanese characters
BYTEA_DATA_TYPE_WITH_VALUES.add_raw_value(
    "'\\xe381a1e381af'",
    "VAL_4",
    {ExpressionCharacteristics.STRING_WITH_SPECIAL_NON_SPACE_CHARS},
)

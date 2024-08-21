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
from materialize.output_consistency.input_data.types.jsonb_type_provider import (
    JSONB_DATA_TYPE,
)

JSONB_DATA_TYPE_WITH_VALUES = DataTypeWithValues(JSONB_DATA_TYPE)

JSONB_DATA_TYPE_WITH_VALUES.add_raw_value(
    "'{}'",
    "EMPTY",
    {ExpressionCharacteristics.JSON_EMPTY},
)
JSONB_DATA_TYPE_WITH_VALUES.add_raw_value(
    "'[]'",
    "EMPTY_ARR",
    {ExpressionCharacteristics.JSON_ARRAY},
)
JSONB_DATA_TYPE_WITH_VALUES.add_raw_value(
    """'["a", "b"]'""",
    "VAL_ARR",
    {ExpressionCharacteristics.JSON_ARRAY},
)
JSONB_DATA_TYPE_WITH_VALUES.add_raw_value("""'{"a": 1}'""", "VAL_1", set())
JSONB_DATA_TYPE_WITH_VALUES.add_raw_value("""'{"a": 1,"b": 2}'""", "VAL_1B", set())
JSONB_DATA_TYPE_WITH_VALUES.add_raw_value("""'{"a": 1,"b": null}'""", "VAL_1C", set())
JSONB_DATA_TYPE_WITH_VALUES.add_raw_value("""'{"a": 1,"c": 3}'""", "VAL_2", set())
JSONB_DATA_TYPE_WITH_VALUES.add_raw_value(
    """'{"a": 1,"b": 4, "m": {}}'""", "VAL_3", set()
)
JSONB_DATA_TYPE_WITH_VALUES.add_raw_value(
    """'{"a": "2", "b": ["1", "2"]}'""", "VAL_4", set()
)
JSONB_DATA_TYPE_WITH_VALUES.add_raw_value(
    """'{"a": "2", "b": { "b1": "1", "b2": { "b20": "2", "b21": "2" }}}'""",
    "VAL_5",
    set(),
)

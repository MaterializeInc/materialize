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
from materialize.output_consistency.input_data.types.range_type_provider import (
    RANGE_DATA_TYPES,
    RangeDataType,
    as_range,
)

VALUES_PER_RANGE_DATA_TYPE: dict[RangeDataType, DataTypeWithValues] = dict()

for range_data_type in RANGE_DATA_TYPES:
    values_of_type = DataTypeWithValues(range_data_type)
    VALUES_PER_RANGE_DATA_TYPE[range_data_type] = values_of_type

    values_of_type.add_raw_value(
        as_range(None, None),
        "EMPTY",
        {ExpressionCharacteristics.COLLECTION_EMPTY},
    )

    values_of_type.add_raw_value(
        as_range(range_data_type.entry_value_1, None),
        "R_OPEN",
        set(),
    )

    values_of_type.add_raw_value(
        as_range(None, range_data_type.entry_value_2),
        "L_OPEN",
        set(),
    )

    values_of_type.add_raw_value(
        as_range(range_data_type.entry_value_1, range_data_type.entry_value_2),
        "CLOSED",
        set(),
    )

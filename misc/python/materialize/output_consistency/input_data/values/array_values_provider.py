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
from materialize.output_consistency.input_data.types.array_type_provider import (
    ARRAY_DATA_TYPES,
    ArrayDataType,
    as_array_sql,
)

VALUES_PER_ARRAY_DATA_TYPE: dict[ArrayDataType, DataTypeWithValues] = dict()

for array_data_type in ARRAY_DATA_TYPES:
    values_of_type = DataTypeWithValues(array_data_type)
    VALUES_PER_ARRAY_DATA_TYPE[array_data_type] = values_of_type

    values_of_type.add_raw_value(
        as_array_sql([]),
        "EMPTY",
        {ExpressionCharacteristics.COLLECTION_EMPTY},
    )

    values_of_type.add_raw_value(
        as_array_sql([array_data_type.entry_value_1]),
        "ONE_ELEM",
        set(),
    )

    values_of_type.add_raw_value(
        as_array_sql([array_data_type.entry_value_1, array_data_type.entry_value_2]),
        "TWO_ELEM",
        set(),
    )

    values_of_type.add_raw_value(
        as_array_sql(
            [
                array_data_type.entry_value_1,
                array_data_type.entry_value_1,
                array_data_type.entry_value_2,
            ]
        ),
        "DUP_ELEM",
        {ExpressionCharacteristics.ARRAY_WITH_DUP_ENTRIES},
    )

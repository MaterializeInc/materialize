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
from materialize.output_consistency.input_data.types.list_type_provider import (
    LIST_DATA_TYPES,
    ListDataType,
    as_list_sql,
)

VALUES_PER_LIST_DATA_TYPE: dict[ListDataType, DataTypeWithValues] = dict()

for list_data_type in LIST_DATA_TYPES:
    values_of_type = DataTypeWithValues(list_data_type)
    VALUES_PER_LIST_DATA_TYPE[list_data_type] = values_of_type

    values_of_type.add_raw_value(
        as_list_sql([]),
        "EMPTY",
        {ExpressionCharacteristics.COLLECTION_EMPTY},
    )

    values_of_type.add_raw_value(
        as_list_sql([list_data_type.entry_value_1]),
        "ONE_ELEM",
        set(),
    )

    values_of_type.add_raw_value(
        as_list_sql([list_data_type.entry_value_1, list_data_type.entry_value_2]),
        "TWO_ELEM",
        set(),
    )

    values_of_type.add_raw_value(
        as_list_sql(
            [
                list_data_type.entry_value_1,
                list_data_type.entry_value_1,
                list_data_type.entry_value_2,
            ]
        ),
        "DUP_ELEM",
        {ExpressionCharacteristics.LIST_WITH_DUP_ENTRIES},
    )

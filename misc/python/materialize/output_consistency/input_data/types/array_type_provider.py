# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.input_data.return_specs.array_return_spec import (
    ArrayReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.collection_return_spec import (
    CollectionReturnTypeSpec,
)
from materialize.output_consistency.input_data.types.collection_type_provider import (
    CollectionDataType,
)

DOUBLE_ARRAY_TYPE_IDENTIFIER = "DOUBLE_ARRAY"
TIMESTAMPTZ_ARRAY_TYPE_IDENTIFIER = "TIMESTAMPTZ_ARRAY"
MULTI_DIM_TEXT_ARRAY_IDENTIFIER = "MULTI_DIM_TEXT_ARRAY"


class ArrayDataType(CollectionDataType):
    def __init__(
        self,
        internal_identifier: str,
        type_name: str,
        array_entry_value_1: str,
        array_entry_value_2: str,
        value_type_category: DataTypeCategory,
    ):
        super().__init__(
            internal_identifier,
            type_name,
            DataTypeCategory.ARRAY,
            entry_value_1=array_entry_value_1,
            entry_value_2=array_entry_value_2,
            value_type_category=value_type_category,
            is_pg_compatible=True,
        )

    def _create_collection_return_type_spec(self) -> CollectionReturnTypeSpec:
        return ArrayReturnTypeSpec(array_value_type_category=self.value_type_category)


def as_array_sql(values: list[str], as_castable_string: bool = True) -> str:
    entries_str = ", ".join(values)
    sql = f"{{{entries_str}}}"

    if as_castable_string:
        sql = f"'{sql}'"

    return sql


DOUBLE_ARRAY_TYPE = ArrayDataType(
    DOUBLE_ARRAY_TYPE_IDENTIFIER,
    type_name="DOUBLE[]",
    array_entry_value_1="0.0",
    array_entry_value_2="4.7",
    value_type_category=DataTypeCategory.NUMERIC,
)
TIMESTAMPTZ_ARRAY_TYPE = ArrayDataType(
    TIMESTAMPTZ_ARRAY_TYPE_IDENTIFIER,
    type_name="TIMESTAMPTZ[]",
    array_entry_value_1="2024-02-29 11:50:00 EST",
    array_entry_value_2="2024-03-31 01:20:00 Europe/Vienna",
    value_type_category=DataTypeCategory.DATE_TIME,
)
MULTI_DIM_TEXT_ARRAY_TYPE = ArrayDataType(
    MULTI_DIM_TEXT_ARRAY_IDENTIFIER,
    type_name="TEXT[][]",
    array_entry_value_1=as_array_sql(["a", "b"], as_castable_string=False),
    array_entry_value_2=as_array_sql(["a", "A"], as_castable_string=False),
    value_type_category=DataTypeCategory.ARRAY,
)
ARRAY_DATA_TYPES = [
    DOUBLE_ARRAY_TYPE,
    TIMESTAMPTZ_ARRAY_TYPE,
    MULTI_DIM_TEXT_ARRAY_TYPE,
]

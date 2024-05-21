# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.input_data.return_specs.collection_return_spec import (
    CollectionReturnTypeSpec,
)
from materialize.output_consistency.input_data.return_specs.list_return_spec import (
    ListReturnTypeSpec,
)
from materialize.output_consistency.input_data.types.collection_type_provider import (
    CollectionDataType,
)

DOUBLE_LIST_TYPE_IDENTIFIER = "DOUBLE_LIST"
TIMESTAMPTZ_LIST_TYPE_IDENTIFIER = "TIMESTAMPTZ_LIST"
NESTED_TEXT_LIST_IDENTIFIER = "NESTED_TEXT_LIST"


class ListDataType(CollectionDataType):
    def __init__(
        self,
        internal_identifier: str,
        type_name: str,
        list_entry_value_1: str,
        list_entry_value_2: str,
        value_type_category: DataTypeCategory,
    ):
        super().__init__(
            internal_identifier,
            type_name,
            DataTypeCategory.LIST,
            entry_value_1=list_entry_value_1,
            entry_value_2=list_entry_value_2,
            value_type_category=value_type_category,
            is_pg_compatible=False,
        )

    def _create_collection_return_type_spec(self) -> CollectionReturnTypeSpec:
        return ListReturnTypeSpec(list_value_type_category=self.value_type_category)


def as_list_sql(values: list[str], as_castable_string: bool = True) -> str:
    entries_str = ", ".join(values)
    sql = f"{{{entries_str}}}"

    if as_castable_string:
        sql = f"'{sql}'"

    return sql


DOUBLE_LIST_TYPE = ListDataType(
    DOUBLE_LIST_TYPE_IDENTIFIER,
    type_name="DOUBLE LIST",
    list_entry_value_1="0.0",
    list_entry_value_2="4.7",
    value_type_category=DataTypeCategory.NUMERIC,
)
TIMESTAMPTZ_LIST_TYPE = ListDataType(
    TIMESTAMPTZ_LIST_TYPE_IDENTIFIER,
    type_name="TIMESTAMPTZ LIST",
    list_entry_value_1="2024-02-29 11:50:00 EST",
    list_entry_value_2="2024-03-31 01:20:00 Europe/Vienna",
    value_type_category=DataTypeCategory.DATE_TIME,
)
NESTED_TEXT_LIST_TYPE = ListDataType(
    NESTED_TEXT_LIST_IDENTIFIER,
    type_name="TEXT LIST LIST",
    list_entry_value_1=as_list_sql(["a", "b"], as_castable_string=False),
    list_entry_value_2=as_list_sql(["a", "A"], as_castable_string=False),
    value_type_category=DataTypeCategory.LIST,
)
LIST_DATA_TYPES = [
    DOUBLE_LIST_TYPE,
    TIMESTAMPTZ_LIST_TYPE,
    NESTED_TEXT_LIST_TYPE,
]

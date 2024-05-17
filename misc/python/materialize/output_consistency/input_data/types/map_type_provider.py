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
from materialize.output_consistency.input_data.return_specs.map_return_spec import (
    MapReturnTypeSpec,
)
from materialize.output_consistency.input_data.types.collection_type_provider import (
    CollectionDataType,
)

MAP_TEXT_TO_DOUBLE_TYPE_IDENTIFIER = "MAP_TEXT_TO_DOUBLE"
MAP_TEXT_TO_TIMESTAMPTZ_TYPE_IDENTIFIER = "MAP_TEXT_TO_TIMESTAMPTZ"
MAP_TEXT_TO_TEXT_MAP_TYPE_IDENTIFIER = "MAP_TEXT_TO_TEXT_MAP"


class MapDataType(CollectionDataType):
    def __init__(
        self,
        internal_identifier: str,
        type_name: str,
        map_entry_value_1: str,
        map_entry_value_2: str,
        value_type_category: DataTypeCategory,
    ):
        super().__init__(
            internal_identifier,
            type_name,
            DataTypeCategory.MAP,
            entry_value_1=map_entry_value_1,
            entry_value_2=map_entry_value_2,
            value_type_category=value_type_category,
            is_pg_compatible=False,
        )

    def _create_collection_return_type_spec(self) -> CollectionReturnTypeSpec:
        return MapReturnTypeSpec(map_value_type_category=self.value_type_category)


def _map_entry(
    key: str,
    sql_value: str,
) -> str:
    return f"{key}=>{sql_value}"


def as_sql_map_string(
    sql_value_by_key: dict[str, str], omit_quotes: bool = False
) -> str:
    entries = []
    for key, sql_value in sql_value_by_key.items():
        entries.append(_map_entry(key, sql_value))
    entries_str = ", ".join(entries)
    entries_str = f"{{{entries_str}}}"

    if not omit_quotes:
        entries_str = f"'{entries_str}'"

    return entries_str


MAP_TEXT_TO_DOUBLE_TYPE = MapDataType(
    MAP_TEXT_TO_DOUBLE_TYPE_IDENTIFIER,
    "MAP[TEXT=>DOUBLE]",
    map_entry_value_1="0.0",
    map_entry_value_2="4.7",
    value_type_category=DataTypeCategory.NUMERIC,
)
MAP_TEXT_TO_TIMESTAMPTZ_TYPE = MapDataType(
    MAP_TEXT_TO_TIMESTAMPTZ_TYPE_IDENTIFIER,
    "MAP[TEXT=>TIMESTAMPTZ]",
    map_entry_value_1="''2024-02-29 11:50:00 EST''",
    map_entry_value_2="''2024-03-31 01:20:00 Europe/Vienna''",
    value_type_category=DataTypeCategory.DATE_TIME,
)
NESTED_MAP_TEXT_TO_TEXT_TEXT_TYPE = MapDataType(
    MAP_TEXT_TO_TEXT_MAP_TYPE_IDENTIFIER,
    "MAP[TEXT=>MAP[TEXT=>TEXT]]",
    map_entry_value_1=as_sql_map_string({"a": "10", "b": "20"}, omit_quotes=True),
    map_entry_value_2=as_sql_map_string({"a": "11", "b": "22"}, omit_quotes=True),
    value_type_category=DataTypeCategory.MAP,
)
MAP_DATA_TYPES = [
    MAP_TEXT_TO_DOUBLE_TYPE,
    MAP_TEXT_TO_TIMESTAMPTZ_TYPE,
    NESTED_MAP_TEXT_TO_TEXT_TEXT_TYPE,
]

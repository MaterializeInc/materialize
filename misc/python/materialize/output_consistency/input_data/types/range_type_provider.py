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
from materialize.output_consistency.input_data.return_specs.range_return_spec import (
    RangeReturnTypeSpec,
)
from materialize.output_consistency.input_data.types.collection_type_provider import (
    CollectionDataType,
)

INT4_RANGE_TYPE_IDENTIFIER = "INT4RANGE"
TIMESTAMPTZ_RANGE_TYPE_IDENTIFIER = "TSTZRANGE"


class RangeDataType(CollectionDataType):
    def __init__(
        self,
        internal_identifier: str,
        type_name: str,
        lower_range_value: str,
        upper_range_value: str,
        value_type_category: DataTypeCategory,
    ):
        super().__init__(
            internal_identifier,
            type_name,
            DataTypeCategory.RANGE,
            entry_value_1=lower_range_value,
            entry_value_2=upper_range_value,
            value_type_category=value_type_category,
            is_pg_compatible=True,
        )

    def _create_collection_return_type_spec(self) -> CollectionReturnTypeSpec:
        return RangeReturnTypeSpec(range_value_type_category=self.value_type_category)


def as_range(
    lower_boundary: str | None,
    upper_boundary: str | None,
    lower_bound_inclusive: bool = True,
    upper_bound_inclusive: bool = True,
) -> str:
    lower_boundary = lower_boundary or ""
    upper_boundary = upper_boundary or ""

    sql = f"{'[' if lower_bound_inclusive else '('}{lower_boundary},{upper_boundary}{']' if upper_bound_inclusive else ')'}"
    return f"'{sql}'"


INT4_RANGE_DATA_TYPE = RangeDataType(
    INT4_RANGE_TYPE_IDENTIFIER, "INT4RANGE", "3", "8", DataTypeCategory.NUMERIC
)
TIMESTAMPTZ_RANGE_DATA_TYPE = RangeDataType(
    TIMESTAMPTZ_RANGE_TYPE_IDENTIFIER,
    "TSTZRANGE",
    "2024-02-29 11:50:00 EST",
    "2024-03-31 01:20:00 Europe/Vienna",
    DataTypeCategory.DATE_TIME,
)


RANGE_DATA_TYPES: list[RangeDataType] = [
    INT4_RANGE_DATA_TYPE,
    TIMESTAMPTZ_RANGE_DATA_TYPE,
]

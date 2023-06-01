# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List

from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory


class DateTimeDataType(DataType):
    def __init__(
        self,
        identifier: str,
        type_name: str,
        min_value: str,
        max_value: str,
        further_values: List[str],
        has_time_zone: bool = False,
    ):
        super().__init__(identifier, type_name, DataTypeCategory.DATE_TIME)
        self.min_value = min_value
        self.max_value = max_value
        self.further_values = further_values
        self.has_time_zone = has_time_zone


DATE_TYPE_IDENTIFIER = "DATE"
TIME_TYPE_IDENTIFIER = "TIME"
TIMESTAMP_TYPE_IDENTIFIER = "TIMESTAMP"
TIMESTAMPTZ_TYPE_IDENTIFIER = "TIMESTAMPTZ"
INTERVAL_TYPE_IDENTIFIER = "INTERVAL"

DATE_TYPE = DateTimeDataType(
    DATE_TYPE_IDENTIFIER,
    "DATE",
    # BC, AD not working, see: https://github.com/MaterializeInc/materialize/issues/19637
    # "4714-11-24 BC",
    # "262143-12-31 AD",
    # "2023-06-01 AD",
    "0001-01-01",
    "99999-12-31",
    ["2023-06-01"],
)
TIME_TYPE = DateTimeDataType(
    TIME_TYPE_IDENTIFIER, "TIME", "00:00:00", "23:59:59.999999", ["01:02:03.000001"]
)
TIMESTAMP_TYPE = DateTimeDataType(
    TIMESTAMP_TYPE_IDENTIFIER,
    "TIMESTAMP",
    # BC, AD not working, see: https://github.com/MaterializeInc/materialize/issues/19637
    # "4713-01-01 BC 00:00:00",
    # "294276-12-31 23:59:59 AD",
    # "2023-06-01 11:22:33 AD",
    # TODO verify
    "0001-01-01 00:00:00",
    "99999-12-31 23:59:59",
    ["2023-06-01 11:22:33"],
)
TIMESTAMPTZ_TYPE = DateTimeDataType(
    TIMESTAMPTZ_TYPE_IDENTIFIER,
    "TIMESTAMPTZ",
    # BC, AD not working, see: https://github.com/MaterializeInc/materialize/issues/19637
    # "4713-01-01 BC 00:00:00",
    # "294276-12-31 23:59:59 AD",
    # "2023-06-01 11:22:33 AD",
    # TODO verify
    "0001-01-01 00:00:00",
    "99999-12-31 23:59:59",
    ["2023-06-01 11:22:33"],
    has_time_zone=True,
)
INTERVAL_TYPE = DateTimeDataType(
    INTERVAL_TYPE_IDENTIFIER,
    "INTERVAL",
    "-178956970 years -8 months -2147483648 days -2562047788:00:54.775808",
    "178956970 years 7 months 2147483647 days 2562047788:00:54.775807",
    ["2 years 3 months 4 days 11:22:33.456789", "100 months 100 days", "44:45:45"],
)

DATE_TIME_DATA_TYPES: List[DateTimeDataType] = [
    DATE_TYPE,
    TIME_TYPE,
    TIMESTAMP_TYPE,
    TIMESTAMPTZ_TYPE,
    INTERVAL_TYPE,
]

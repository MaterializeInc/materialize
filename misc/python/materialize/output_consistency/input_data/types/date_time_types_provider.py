# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.input_data.return_specs.date_time_return_spec import (
    DateTimeReturnTypeSpec,
)
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec


class DateTimeDataType(DataType):
    def __init__(
        self,
        internal_identifier: str,
        type_name: str,
        min_value: str,
        max_value: str,
        further_values: list[tuple[str, set[ExpressionCharacteristics]]],
        further_values_with_fixed_timezone: list[str] = [],
        has_time_zone: bool = False,
        is_pg_compatible: bool = True,
        is_max_value_pg_compatible: bool = True,
    ):
        super().__init__(
            internal_identifier,
            type_name,
            DataTypeCategory.DATE_TIME,
            is_pg_compatible=is_pg_compatible,
        )
        self.min_value = min_value
        self.max_value = max_value
        self.further_values = further_values
        self.further_values_with_fixed_timezone = further_values_with_fixed_timezone
        self.has_time_zone = has_time_zone
        self.is_max_value_pg_compatible = is_max_value_pg_compatible

    def resolve_return_type_spec(
        self, characteristics: set[ExpressionCharacteristics]
    ) -> ReturnTypeSpec:
        return DateTimeReturnTypeSpec(
            type_identifier=self.internal_identifier,
        )


DATE_TYPE_IDENTIFIER = "DATE"
TIME_TYPE_IDENTIFIER = "TIME"
TIMESTAMP_TYPE_IDENTIFIER = "TIMESTAMP"
TIMESTAMPTZ_TYPE_IDENTIFIER = "TIMESTAMPTZ"
INTERVAL_TYPE_IDENTIFIER = "INTERVAL"

DATE_TYPE = DateTimeDataType(
    DATE_TYPE_IDENTIFIER,
    "DATE",
    # BC, AD not working, see: https://github.com/MaterializeInc/database-issues/issues/5843
    "0001-01-01",
    "99999-12-31",
    [
        ("2023-06-01", set()),
        ("2024-02-29", set()),
        # TODO: Reenable when database-issues#8642 is fixed
        # ("01-02-03", {ExpressionCharacteristics.DATE_WITH_SHORT_YEAR}),
    ],
    is_max_value_pg_compatible=False,
)
TIME_TYPE = DateTimeDataType(
    TIME_TYPE_IDENTIFIER,
    "TIME",
    "00:00:00",
    "23:59:59.999999",
    [
        ("01:02:03.000001", set()),
        ("11:", {ExpressionCharacteristics.INCOMPLETE_TIME_VALUE}),
    ],
)
TIMESTAMP_TYPE = DateTimeDataType(
    TIMESTAMP_TYPE_IDENTIFIER,
    "TIMESTAMP",
    # BC, AD not working, see: https://github.com/MaterializeInc/database-issues/issues/5843
    "0001-01-01 00:00:00",
    "99999-12-31 23:59:59",
    [
        ("2023-02-28 11:22:33.44444", set()),
        ("2024-02-29 23:50:00", set()),
        # TODO: Reenable when database-issues#8642 is fixed
        # (
        #    "01-02-03 11:",
        #    {
        #        ExpressionCharacteristics.DATE_WITH_SHORT_YEAR,
        #        ExpressionCharacteristics.INCOMPLETE_TIME_VALUE,
        #    },
        # ),
    ],
    is_max_value_pg_compatible=False,
)
TIMESTAMPTZ_TYPE = DateTimeDataType(
    TIMESTAMPTZ_TYPE_IDENTIFIER,
    "TIMESTAMPTZ",
    # BC, AD not working, see: https://github.com/MaterializeInc/database-issues/issues/5843
    "0001-01-01 00:00:00",
    "99999-12-31 23:59:59",
    further_values=[("2023-06-01 11:22:33.44444", set())],
    further_values_with_fixed_timezone=[
        # leap year
        "2024-02-29 11:50:00 EST",
        "2024-02-28 19:50:00 America/Los_Angeles",
        # change to DST
        "2024-03-31 01:20:00 Europe/Vienna",
        "2024-03-31 02:20:00 Europe/Vienna",
        "2024-03-31 03:20:00 Europe/Vienna",
    ],
    has_time_zone=True,
    is_max_value_pg_compatible=False,
)
INTERVAL_TYPE = DateTimeDataType(
    INTERVAL_TYPE_IDENTIFIER,
    "INTERVAL",
    "-178956970 years -8 months -2147483648 days -2562047788:00:54.775808",
    "178956970 years 7 months 2147483647 days 2562047788:00:54.775807",
    [
        ("2 years 3 months 4 days 11:22:33.456789", set()),
        ("100 months 100 days", set()),
        ("44:45:45", set()),
        ("45 minutes", set()),
        ("70 minutes", set()),
    ],
    # type is compatible but causes too many issues for now
    is_pg_compatible=False,
    is_max_value_pg_compatible=True,
)

DATE_TIME_DATA_TYPES: list[DateTimeDataType] = [
    DATE_TYPE,
    TIME_TYPE,
    TIMESTAMP_TYPE,
    TIMESTAMPTZ_TYPE,
    INTERVAL_TYPE,
]

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import re

from materialize.output_consistency.data_type.data_type_with_values import (
    DataTypeWithValues,
)
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.input_data.params.enum_constant_operation_params import (
    TIME_ZONE_PARAM,
)
from materialize.output_consistency.input_data.types.date_time_types_provider import (
    DATE_TIME_DATA_TYPES,
    DateTimeDataType,
)

DATE_TIME_DATA_TYPES_WITH_VALUES: list[DataTypeWithValues] = []


def __create_values(
    _values_of_type: DataTypeWithValues,
    _date_time_data_type: DateTimeDataType,
    _timezone: str | None,
) -> None:
    timezone_value_suffix = f" {_timezone}" if _timezone else ""
    timezone_column_suffix = f"_{_timezone}" if _timezone else ""
    timezone_column_suffix = re.sub("\\+", "PLUS", timezone_column_suffix)
    timezone_column_suffix = re.sub("/", "_", timezone_column_suffix)

    if not _date_time_data_type.has_time_zone or _timezone is not None:
        # do not create these values for data types with time zones but without selected time zone

        _values_of_type.add_raw_value(
            f"'{_date_time_data_type.min_value}{timezone_value_suffix}'",
            f"MIN_VAL{timezone_column_suffix}",
            {ExpressionCharacteristics.MAX_VALUE, ExpressionCharacteristics.NEGATIVE},
        )
        _values_of_type.add_raw_value(
            f"'{_date_time_data_type.max_value}{timezone_value_suffix}'",
            f"MAX_VAL{timezone_column_suffix}",
            {ExpressionCharacteristics.MAX_VALUE},
            is_pg_compatible=_date_time_data_type.is_max_value_pg_compatible,
        )

        for index, value_and_characteristics in enumerate(
            _date_time_data_type.further_values
        ):
            value = value_and_characteristics[0]
            characteristics = value_and_characteristics[1]
            _values_of_type.add_raw_value(
                f"'{value}{timezone_value_suffix}'",
                f"VAL_{index + 1}{timezone_column_suffix}",
                characteristics,
            )

    if _timezone is None:
        for index, value in enumerate(
            _date_time_data_type.further_values_with_fixed_timezone
        ):
            assert _date_time_data_type.has_time_zone
            # time zone is already part of the value, do not add it
            _values_of_type.add_raw_value(
                f"'{value}'",
                f"VAL2_{index + 1}",
                set(),
            )


for date_time_data_type in DATE_TIME_DATA_TYPES:
    values_of_type = DataTypeWithValues(date_time_data_type)
    DATE_TIME_DATA_TYPES_WITH_VALUES.append(values_of_type)

    if date_time_data_type.has_time_zone:
        for timezone in TIME_ZONE_PARAM.get_valid_values():
            __create_values(values_of_type, date_time_data_type, timezone)
        # create values with fixed time zone
        __create_values(values_of_type, date_time_data_type, None)
    else:
        __create_values(values_of_type, date_time_data_type, None)

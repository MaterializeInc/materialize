# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import re

from materialize.output_consistency.execution.sql_dialect_adjuster import (
    SqlDialectAdjuster,
)
from materialize.output_consistency.input_data.types.date_time_types_provider import (
    INTERVAL_TYPE_IDENTIFIER,
)


class PgSqlDialectAdjuster(SqlDialectAdjuster):
    def adjust_type(self, type_name: str) -> str:
        if type_name == "DOUBLE":
            return "DOUBLE PRECISION"
        if type_name == "DOUBLE[]":
            return "DOUBLE PRECISION[]"

        return type_name

    def adjust_enum_value(self, string_value: str) -> str:
        if string_value == "DOUBLE":
            return "DOUBLE PRECISION"
        if string_value == "DOUBLE[]":
            return "DOUBLE PRECISION[]"

        return string_value

    def adjust_value(
        self, string_value: str, internal_type_identifier: str, type_name: str
    ) -> str:
        if internal_type_identifier == INTERVAL_TYPE_IDENTIFIER:
            # cut milliseconds away
            string_value = re.sub("\\.\\d+'$", "'", string_value)

        if type_name.startswith("INT") and string_value.startswith("-"):
            # wrap negative numbers in parentheses
            # see: https://github.com/MaterializeInc/database-issues/issues/6611
            string_value = f"({string_value})"

        return string_value

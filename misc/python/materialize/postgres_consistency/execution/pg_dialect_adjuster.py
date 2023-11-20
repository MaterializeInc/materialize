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


class PgSqlDialectAdjuster(SqlDialectAdjuster):
    def adjust_type(self, type_name: str) -> str:
        if type_name == "DOUBLE":
            return "DOUBLE PRECISION"

        return type_name

    def adjust_value(self, string_value: str, type_name: str) -> str:
        if type_name == "INTERVAL":
            # cut milliseconds away
            string_value = re.sub("\\.\\d+'$", "'", string_value)

        if type_name.startswith("INT") and string_value.startswith("-"):
            # wrap negative numbers in parentheses
            # see: https://github.com/MaterializeInc/materialize/issues/21993
            string_value = f"({string_value})"

        return string_value

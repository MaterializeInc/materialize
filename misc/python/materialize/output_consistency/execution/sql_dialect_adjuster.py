# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


class SqlDialectAdjuster:
    def adjust_type(self, type_name: str) -> str:
        return type_name

    def adjust_value(
        self, string_value: str, internal_type_identifier: str, type_name: str
    ) -> str:
        return string_value

    def adjust_enum_value(self, string_value: str) -> str:
        return string_value


class MzSqlDialectAdjuster(SqlDialectAdjuster):
    pass

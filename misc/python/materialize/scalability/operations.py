# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.scalability.operation import Operation


class InsertDefaultValues(Operation):
    def sql_statement(self) -> str:
        return "INSERT INTO t1 DEFAULT VALUES;"


class SelectOne(Operation):
    def sql_statement(self) -> str:
        return "SELECT 1;"


class SelectStar(Operation):
    def sql_statement(self) -> str:
        return "SELECT * FROM t1;"


class SelectCount(Operation):
    def sql_statement(self) -> str:
        return "SELECT COUNT(*) FROM t1;"


class SelectUnionAll(Operation):
    def sql_statement(self) -> str:
        return "SELECT * FROM t1 UNION ALL SELECT * FROM t1;"

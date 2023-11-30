# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import uuid

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


class SelectLimit(Operation):
    def sql_statement(self) -> str:
        return "SELECT * FROM t1 LIMIT 1;"


class SelectCount(Operation):
    def sql_statement(self) -> str:
        return "SELECT COUNT(*) FROM t1;"


class SelectCountInMv(Operation):
    def sql_statement(self) -> str:
        return "SELECT count FROM mv1;"


class SelectUnionAll(Operation):
    def sql_statement(self) -> str:
        return "SELECT * FROM t1 UNION ALL SELECT * FROM t1;"


class Update(Operation):
    def sql_statement(self) -> str:
        return "UPDATE t1 SET f1 = f1 + 1;"


class CreateDropTable(Operation):
    def sql_statement(self) -> str:
        seed = str(uuid.uuid4())
        return (
            "BEGIN;"
            f'CREATE TABLE "ddl_t{seed}" (a INT);'
            "COMMIT;"
            "BEGIN;"
            f'DROP TABLE "ddl_t{seed}";'
            "COMMIT;"
        )


class CreateDropView(Operation):
    def sql_statement(self) -> str:
        seed = str(uuid.uuid4())
        return (
            "BEGIN;"
            f'CREATE VIEW "ddl_v{seed}" AS SELECT 42;'
            "COMMIT;"
            "BEGIN;"
            f'DROP VIEW "ddl_v{seed}";'
            "COMMIT;"
        )


class CreateDropMaterializedView(Operation):
    def sql_statement(self) -> str:
        seed = str(uuid.uuid4())
        return (
            "BEGIN;"
            f'CREATE MATERIALIZED VIEW "ddl_mv{seed}" AS SELECT 42;'
            "COMMIT;"
            "BEGIN;"
            f'DROP MATERIALIZED VIEW "ddl_mv{seed}";'
            "COMMIT;"
        )

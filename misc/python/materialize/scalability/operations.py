# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from psycopg import Connection

from materialize.scalability.endpoint import Endpoint
from materialize.scalability.operation import (
    Operation,
    SimpleSqlOperation,
    SqlOperationWithSeed,
    SqlOperationWithTwoSeeds,
)
from materialize.scalability.operation_data import OperationData
from materialize.scalability.schema import Schema


class InsertDefaultValues(SimpleSqlOperation):
    def sql_statement(self) -> str:
        return "INSERT INTO t1 DEFAULT VALUES;"


class SelectOne(SimpleSqlOperation):
    def sql_statement(self) -> str:
        return "SELECT 1;"


class SelectStar(SimpleSqlOperation):
    def sql_statement(self) -> str:
        return "SELECT * FROM t1;"


class SelectLimit(SimpleSqlOperation):
    def sql_statement(self) -> str:
        return "SELECT * FROM t1 LIMIT 1;"


class SelectCount(SimpleSqlOperation):
    def sql_statement(self) -> str:
        return "SELECT COUNT(*) FROM t1;"


class SelectCountInMv(SimpleSqlOperation):
    def sql_statement(self) -> str:
        return "SELECT count FROM mv1;"


class SelectUnionAll(SimpleSqlOperation):
    def sql_statement(self) -> str:
        return "SELECT * FROM t1 UNION ALL SELECT * FROM t1;"


class Update(SimpleSqlOperation):
    def sql_statement(self) -> str:
        return "UPDATE t1 SET f1 = f1 + 1;"


class CreateTableX(SqlOperationWithSeed):
    def __init__(self) -> None:
        super().__init__("table_seed")

    def sql_statement(self, table_seed: str) -> str:
        return f"CREATE TABLE x_{table_seed} (f1 INT, f2 INT, f3 INT, f4 INT, f5 INT);"


class CreateIndexOnTableX(SqlOperationWithSeed):
    def __init__(self) -> None:
        super().__init__("table_seed")

    def sql_statement(self, table_seed: str) -> str:
        return f"CREATE INDEX i_x_{table_seed} ON x_{table_seed} (f1);"


class CreateMvOnTableX(SqlOperationWithSeed):
    def __init__(self) -> None:
        super().__init__("table_seed")

    def sql_statement(self, table_seed: str) -> str:
        return f"CREATE MATERIALIZED VIEW mv_x_{table_seed} AS SELECT * FROM x_{table_seed};"


class PopulateTableX(SqlOperationWithSeed):
    def __init__(self) -> None:
        super().__init__("table_seed")

    def sql_statement(self, table_seed: str) -> str:
        return f"INSERT INTO x_{table_seed} SELECT generate_series(1, 100), 200, 300, 400, 500;"


class FillColumnInTableX(SqlOperationWithTwoSeeds):
    def __init__(self, column_seed_key: str = "column_seed") -> None:
        super().__init__("table_seed", column_seed_key)

    def sql_statement(self, table_seed: str, column_seed: str) -> str:
        return f"UPDATE x_{table_seed} SET c_{column_seed} = f1;"


class DropMvOfTableX(SqlOperationWithSeed):
    def __init__(self) -> None:
        super().__init__("table_seed")

    def sql_statement(self, table_seed: str) -> str:
        return f"DROP MATERIALIZED VIEW mv_x_{table_seed} CASCADE;"


class DropTableX(SqlOperationWithSeed):
    def __init__(self) -> None:
        super().__init__("table_seed")

    def sql_statement(self, table_seed: str) -> str:
        return f"DROP TABLE x_{table_seed} CASCADE;"


class SelectStarFromTableX(SqlOperationWithSeed):
    def __init__(self) -> None:
        super().__init__("table_seed")

    def sql_statement(self, table_seed: str) -> str:
        return f"SELECT * FROM x_{table_seed};"


class SelectStarFromMvOnTableX(SqlOperationWithSeed):
    def __init__(self) -> None:
        super().__init__("table_seed")

    def sql_statement(self, table_seed: str) -> str:
        return f"SELECT * FROM mv_x_{table_seed};"


class Connect(Operation):
    def required_keys(self) -> set[str]:
        return {"endpoint", "schema"}

    def produced_keys(self) -> set[str]:
        return {"connection", "cursor"}

    def _execute(self, data: OperationData) -> OperationData:
        endpoint: Endpoint = data.get("endpoint")
        schema: Schema = data.get("schema")

        connection = endpoint.sql_connection(quiet=True)
        connection.autocommit = True
        cursor = connection.cursor()

        # this sets the database schema
        for connect_sql in schema.connect_sqls():
            cursor.execute(connect_sql.encode("utf8"))

        data.push("connection", connection)
        data.push("cursor", cursor)
        return data


class Disconnect(Operation):
    def required_keys(self) -> set[str]:
        return {"connection"}

    def _execute(self, data: OperationData) -> OperationData:
        connection: Connection = data.get("connection")
        connection.close()
        return data

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import Any, Sequence

from pg8000 import Cursor
from pg8000.dbapi import ProgrammingError
from pg8000.exceptions import DatabaseError

from materialize.output_consistency.configuration.configuration import (
    ConsistencyTestConfiguration,
)


class SqlExecutionError(Exception):
    def __init__(self, original_exception: Exception):
        super().__init__(str(original_exception))


class SqlExecutor:
    def __str__(self) -> str:
        return self.__class__.__name__

    def ddl(self, sql: str) -> None:
        raise RuntimeError("Not implemented")

    def begin_tx(self, isolation_level: str) -> None:
        raise RuntimeError("Not implemented")

    def commit(self) -> None:
        raise RuntimeError("Not implemented")

    def rollback(self) -> None:
        raise RuntimeError("Not implemented")

    def query(self, sql: str) -> Sequence[Sequence[Any]]:
        raise RuntimeError("Not implemented")


class PgWireDatabaseSqlExecutor(SqlExecutor):
    def __init__(self, cursor: Cursor):
        self.cursor = cursor

    def ddl(self, sql: str) -> None:
        try:
            self.cursor.execute(sql)
        except (ProgrammingError, DatabaseError) as err:
            raise SqlExecutionError(err)

    def begin_tx(self, isolation_level: str) -> None:
        self._execute_with_cursor(f"BEGIN ISOLATION LEVEL {isolation_level};")

    def commit(self) -> None:
        self._execute_with_cursor("COMMIT;")

    def rollback(self) -> None:
        self._execute_with_cursor("ROLLBACK;")

    def query(self, sql: str) -> Sequence[Sequence[Any]]:
        try:
            self._execute_with_cursor(sql)
            return self.cursor.fetchall()
        except (ProgrammingError, DatabaseError) as err:
            raise SqlExecutionError(err)

    def _execute_with_cursor(self, sql: str) -> None:
        try:
            self.cursor.execute(sql)
        except (ProgrammingError, DatabaseError) as err:
            raise SqlExecutionError(err)


class DryRunSqlExecutor(SqlExecutor):
    def consume_sql(self, sql: str) -> None:
        print(f"> {sql}")

    def ddl(self, sql: str) -> None:
        self.consume_sql(sql)

    def begin_tx(self, isolation_level: str) -> None:
        self.consume_sql(f"BEGIN ISOLATION LEVEL {isolation_level};")

    def commit(self) -> None:
        self.consume_sql("COMMIT;")

    def rollback(self) -> None:
        self.consume_sql("ROLLBACK;")

    def query(self, sql: str) -> Sequence[Sequence[Any]]:
        self.consume_sql(sql)
        return []


def create_sql_executor(
    config: ConsistencyTestConfiguration, cursor: Cursor
) -> SqlExecutor:
    if config.dry_run:
        return DryRunSqlExecutor()
    else:
        return PgWireDatabaseSqlExecutor(cursor)

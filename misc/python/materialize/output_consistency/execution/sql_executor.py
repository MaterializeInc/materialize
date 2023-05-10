# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import Any, Optional, Sequence

from pg8000 import Cursor
from pg8000.dbapi import ProgrammingError
from pg8000.exceptions import DatabaseError

from materialize.mzcompose import Composition
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

    def acquire_cursor(self) -> None:
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
    def __init__(self, composition: Composition):
        self.composition = composition
        self._cursor: Optional[Cursor] = None

    def get_cursor(self) -> Cursor:
        if self._cursor is None:
            raise RuntimeError("Cursor not initialized")
        else:
            return self._cursor

    def ddl(self, sql: str) -> None:
        try:
            self.composition.sql(sql)
        except (ProgrammingError, DatabaseError) as err:
            raise SqlExecutionError(err)

    def acquire_cursor(self) -> None:
        self._cursor = self.composition.sql_cursor()

    def begin_tx(self, isolation_level: str) -> None:
        self._execute_with_cursor(f"BEGIN ISOLATION LEVEL {isolation_level};")

    def commit(self) -> None:
        self._execute_with_cursor("COMMIT;")

    def rollback(self) -> None:
        self._execute_with_cursor("ROLLBACK;")

    def query(self, sql: str) -> Sequence[Sequence[Any]]:
        try:
            cursor = self.get_cursor()
            self._execute_with_cursor(sql)
            return cursor.fetchall()
        except (ProgrammingError, DatabaseError) as err:
            raise SqlExecutionError(err)

    def _execute_with_cursor(self, sql: str) -> None:
        try:
            self.get_cursor().execute(sql)
        except (ProgrammingError, DatabaseError) as err:
            raise SqlExecutionError(err)


class DryRunSqlExecutor(SqlExecutor):
    def consume_sql(self, sql: str) -> None:
        print(f"> {sql}")

    def ddl(self, sql: str) -> None:
        self.consume_sql(sql)

    def acquire_cursor(self) -> None:
        pass

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
    config: ConsistencyTestConfiguration, composition: Composition
) -> SqlExecutor:
    if config.dry_run:
        return DryRunSqlExecutor()
    else:
        return PgWireDatabaseSqlExecutor(composition)

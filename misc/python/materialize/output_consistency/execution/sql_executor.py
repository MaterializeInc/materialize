# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import Any, Sequence, Union

from pg8000 import Connection
from pg8000.dbapi import ProgrammingError
from pg8000.exceptions import DatabaseError

from materialize.output_consistency.common.configuration import (
    ConsistencyTestConfiguration,
)
from materialize.output_consistency.output.output_printer import OutputPrinter


class SqlExecutionError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        # storing it here as well makes it easier to access the message
        self.message = message


class SqlExecutor:
    """Base class of `PgWireDatabaseSqlExecutor` and `DryRunSqlExecutor`"""

    def __str__(self) -> str:
        return self.__class__.__name__

    def ddl(self, sql: str) -> None:
        raise NotImplementedError

    def begin_tx(self, isolation_level: str) -> None:
        raise NotImplementedError

    def commit(self) -> None:
        raise NotImplementedError

    def rollback(self) -> None:
        raise NotImplementedError

    def query(self, sql: str) -> Sequence[Sequence[Any]]:
        raise NotImplementedError


class PgWireDatabaseSqlExecutor(SqlExecutor):
    def __init__(
        self,
        connection: Connection,
        use_autocommit: bool,
        output_printer: OutputPrinter,
    ):
        connection.autocommit = use_autocommit
        self.cursor = connection.cursor()
        self.output_printer = output_printer

    def ddl(self, sql: str) -> None:
        try:
            self.cursor.execute(sql)
        except (ProgrammingError, DatabaseError) as err:
            raise SqlExecutionError(self._extract_message_from_error(err))

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
            raise SqlExecutionError(self._extract_message_from_error(err))

    def _execute_with_cursor(self, sql: str) -> None:
        try:
            self.cursor.execute(sql)
        except (ProgrammingError, DatabaseError) as err:
            raise SqlExecutionError(self._extract_message_from_error(err))
        except ValueError as err:
            self.output_printer.print_error(f"Query with value error is: {sql}")
            raise err
        except Exception as err:
            self.output_printer.print_error(f"Query with unexpected error is: {sql}")
            raise err

    def _extract_message_from_error(
        self, error: Union[ProgrammingError, DatabaseError]
    ) -> str:
        error_args = error.args[0]
        message = error_args.get("M")
        details = error_args.get("H")

        if details is None:
            return f"{message}"
        else:
            return f"{message} ({details})"


class DryRunSqlExecutor(SqlExecutor):
    def __init__(self, output_printer: OutputPrinter):
        self.output_printer = output_printer

    def consume_sql(self, sql: str) -> None:
        self.output_printer.print_sql(sql)

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
    config: ConsistencyTestConfiguration,
    connection: Connection,
    output_printer: OutputPrinter,
) -> SqlExecutor:
    if config.dry_run:
        return DryRunSqlExecutor(output_printer)
    else:
        return PgWireDatabaseSqlExecutor(
            connection, config.use_autocommit, output_printer
        )

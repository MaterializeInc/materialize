# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import re
import ssl
from textwrap import dedent

import pg8000
from pg8000 import Connection, Cursor

from materialize.test_analytics.config.mz_db_config import MzDbConfig


class TestAnalyticsUploadError(Exception):
    def __init__(self, message: str, sql: str):
        super().__init__(message)
        # storing it here as well makes it easier to access the message
        self.message = message
        self.sql = sql

    def __str__(self) -> str:
        return f"{self.message} (last executed sql: {self.sql})"


class DatabaseConnector:
    def __init__(self, config: MzDbConfig, current_data_version: int, log_sql: bool):
        self.config = config
        self.current_data_version = current_data_version
        self.update_statements = []
        self._log_sql = log_sql
        self._read_only = False
        # Note that transactions in mz do not allow to mix read and write statements (INSERT INTO ... SELECT FROM ...)
        self._use_transaction = False
        self.open_connection: Connection | None = None

    def get_or_create_connection(self, autocommit: bool = False) -> Connection:
        if self.open_connection is None:
            self.open_connection = self.create_connection(autocommit=autocommit)
        else:
            self.open_connection.autocommit = autocommit

        return self.open_connection

    def create_connection(
        self, autocommit: bool = False, timeout_in_seconds: int = 5
    ) -> Connection:
        connection = pg8000.connect(
            host=self.config.hostname,
            user=self.config.username,
            password=self.config.app_password,
            port=self.config.port,
            ssl_context=ssl.SSLContext(),
            timeout=timeout_in_seconds,
        )

        connection.autocommit = autocommit

        return connection

    def create_cursor(
        self,
        connection: Connection | None = None,
        autocommit: bool = False,
        allow_reusing_connection: bool = False,
        statement_timeout: str = "1s",
    ) -> Cursor:
        if connection is None:
            if allow_reusing_connection:
                connection = self.get_or_create_connection(autocommit=autocommit)
            else:
                connection = self.create_connection(autocommit=autocommit)

        cursor = connection.cursor()
        cursor.execute(f"SET database = {self.config.database}")
        cursor.execute(f"SET search_path = {self.config.search_path}")
        cursor.execute(f"SET statement_timeout = '{statement_timeout}'")
        return cursor

    def set_read_only(self) -> None:
        self._read_only = True

    def query_min_required_data_version(self, cursor: Cursor) -> int:
        cursor.execute("SELECT min_required_data_version_for_uploads FROM config;")
        return int(cursor.fetchall()[0][0])

    def add_update_statements(self, sql_statements: list[str]) -> None:
        self.update_statements.extend(sql_statements)

    def submit_update_statements(self) -> None:
        if len(self.update_statements) == 0:
            return

        cursor = self.create_cursor(autocommit=not self._use_transaction)

        self._disable_if_on_unsupported_version(cursor)

        if self._read_only:
            print(
                "Read-only mode: Not writing any data to the test analytics database!"
            )
            return

        last_executed_sql = self.update_statements[0]

        print("~~~ Updates to test analytics database")

        try:
            if self._use_transaction:
                self._execute_sql(cursor, "BEGIN;")

            for sql in self.update_statements:
                sql = dedent(sql)
                last_executed_sql = sql
                self._execute_sql(cursor, sql, print_status=True)

            if self._use_transaction:
                self._execute_sql(cursor, "COMMIT;")
            print("Upload completed.")
        except Exception as e:
            print("Upload failed, triggering rollback.")
            try:
                if self._use_transaction:
                    self._execute_sql(cursor, "ROLLBACK;")
            except:
                pass

            error_msg = f"Failed to write to test analytics database! Cause: {e}"
            raise TestAnalyticsUploadError(error_msg, sql=last_executed_sql)
        finally:
            self.update_statements = []

    def _execute_sql(
        self, cursor: Cursor, sql: str, print_status: bool = False
    ) -> None:
        if self._log_sql:
            printable_sql = self.to_short_printable_sql(sql)
            print(f"> {printable_sql}")

        try:
            cursor.execute(sql)

            if print_status:
                affected_rows = cursor.rowcount
                print(
                    f"-- OK ({affected_rows} row{'s' if affected_rows != 1 else ''} affected)"
                )
        except:
            if print_status:
                print("-- FAILED!")
            raise

    def _disable_if_on_unsupported_version(
        self,
        cursor: Cursor,
    ) -> None:
        min_required_data_version = self.query_min_required_data_version(cursor)
        print(
            f"Current data version is {self.current_data_version}, min required version is {min_required_data_version}"
        )

        if self.current_data_version < min_required_data_version:
            print(
                f"Uploading test_analytics data is not supported from this data version ({self.current_data_version})"
            )
            self.set_read_only()

    def to_short_printable_sql(self, sql: str) -> str:
        return re.sub(r"^\s+", "", sql, flags=re.MULTILINE).replace("\n", " ").strip()

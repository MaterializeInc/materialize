# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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
        self.current_data_version = current_data_version
        self._connection = self._create_connection(config)
        self._cursor = self._create_cursor(config, self._connection)
        self.update_statements = []
        self._log_sql = log_sql
        self._read_only = False

    def _create_connection(self, config: MzDbConfig) -> Connection:
        connection = pg8000.connect(
            host=config.hostname,
            user=config.username,
            password=config.app_password,
            port=config.port,
            ssl_context=ssl.SSLContext(),
        )

        connection.autocommit = False

        return connection

    def _create_cursor(
        self, config: MzDbConfig, connection: Connection | None = None
    ) -> Cursor:
        if connection is None:
            connection = self._create_connection(config)

        cursor = connection.cursor()
        cursor.execute(f"SET database = {config.database}")
        cursor.execute(f"SET search_path = {config.search_path}")
        return cursor

    def set_read_only(self) -> None:
        self._read_only = True

    def query_min_required_data_version(self) -> int:
        self._cursor.execute(
            "SELECT min_required_data_version_for_uploads FROM config;"
        )
        return int(self._cursor.fetchall()[0][0])

    def add_update_statements(self, sql_statements: list[str]) -> None:
        self.update_statements.extend(sql_statements)

    def submit_update_statements(self) -> None:
        if len(self.update_statements) == 0:
            return

        self._disable_if_on_unsupported_version()

        if self._read_only:
            print("Skipping updates to test_analytics due to read-only mode!")
            return

        last_executed_sql = self.update_statements[0]

        try:
            self._execute_sql("BEGIN;")
            for sql in self.update_statements:
                sql = dedent(sql)
                last_executed_sql = sql
                self._execute_sql(sql)
            self._execute_sql("COMMIT;")
        except Exception as e:
            try:
                self._execute_sql("ROLLBACK;")
            except:
                pass

            error_msg = f"Failed to write to test analytics database! Cause: {e}"
            raise TestAnalyticsUploadError(error_msg, sql=last_executed_sql)
        finally:
            self.update_statements = []

    def _execute_sql(self, sql: str) -> None:
        if self._log_sql:
            print(f"> {sql.strip()}")

        self._cursor.execute(sql)

    def _disable_if_on_unsupported_version(
        self,
    ) -> None:
        min_required_data_version = self.query_min_required_data_version()
        print(
            f"Current data version is {self.current_data_version}, min required version is {min_required_data_version}"
        )

        if self.current_data_version < min_required_data_version:
            print(
                f"Uploading test_analytics data is not supported from this data version ({self.current_data_version})"
            )
            self.set_read_only()

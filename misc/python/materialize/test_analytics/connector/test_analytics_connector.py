# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import re
import time
from dataclasses import dataclass
from textwrap import dedent

import psycopg
from psycopg import Connection, Cursor

from materialize.test_analytics.config.mz_db_config import MzDbConfig
from materialize.test_analytics.util.mz_sql_util import as_sanitized_literal


def _is_usable(connection: Connection) -> bool:
    return not connection.closed and not connection.broken


class TestAnalyticsUploadError(Exception):
    __test__ = False

    def __init__(self, message: str, sql: str):
        super().__init__(message)
        # storing it here as well makes it easier to access the message
        self.message = message
        self.sql = sql

    def __str__(self) -> str:
        return f"{self.message}. Last executed SQL:\n```\n{self.sql.strip()}\n```"


@dataclass
class TestAnalyticsSettings:
    __test__ = False

    uploads_enabled: bool
    min_required_data_version_for_uploads: int
    only_notify_about_communication_failures_on_main: bool


class DatabaseConnector:

    def __init__(self, config: MzDbConfig):
        self.config = config

    def get_or_create_connection(self, autocommit: bool = False) -> Connection:
        raise NotImplementedError

    def create_connection(
        self,
        autocommit: bool = False,
        timeout_in_seconds: int = 60,
        remaining_retries: int = 1,
    ) -> Connection:
        raise NotImplementedError

    def create_cursor(
        self,
        connection: Connection | None = None,
        autocommit: bool = False,
        allow_reusing_connection: bool = False,
        statement_timeout: str | None = None,
    ) -> Cursor:
        raise NotImplementedError

    def set_read_only(self) -> None:
        raise NotImplementedError

    def try_get_or_query_settings(self) -> TestAnalyticsSettings | None:
        raise NotImplementedError

    def add_update_statements(
        self, sql_statements: list[str], idempotent: bool = False
    ) -> None:
        """Queue statements for the next `submit_update_statements`.

        Set `idempotent` when executing a statement a second time cannot
        duplicate its write, which holds for an UPDATE assigning fixed values
        and for an INSERT guarded by a `WHERE NOT EXISTS` uniqueness check.
        Only such statements are replayed after the connection drops while they
        are in flight. Everything else is abandoned with an unknown outcome, so
        leaving this at the default risks losing a row, never duplicating one.
        """
        raise NotImplementedError

    def submit_update_statements(self) -> None:
        raise NotImplementedError

    def to_short_printable_sql(self, sql: str) -> str:
        return re.sub(r"^\s+", "", sql, flags=re.MULTILINE).replace("\n", " ").strip()


class DummyDatabaseConnector(DatabaseConnector):

    def __init__(self, config: MzDbConfig):
        super().__init__(config)

    def get_or_create_connection(self, autocommit: bool = False) -> Connection:
        raise RuntimeError("Forbidden")

    def create_connection(
        self,
        autocommit: bool = False,
        timeout_in_seconds: int = 60,
        remaining_retries: int = 1,
    ) -> Connection:
        raise RuntimeError("Forbidden")

    def create_cursor(
        self,
        connection: Connection | None = None,
        autocommit: bool = False,
        allow_reusing_connection: bool = False,
        statement_timeout: str | None = None,
    ) -> Cursor:
        raise RuntimeError("Forbidden")

    def set_read_only(self) -> None:
        pass

    def try_get_or_query_settings(self) -> TestAnalyticsSettings | None:
        return None

    def add_update_statements(
        self, sql_statements: list[str], idempotent: bool = False
    ) -> None:
        pass

    def submit_update_statements(self) -> None:
        print("Not submitting update statements in dummy mode")


class DatabaseConnectorImpl(DatabaseConnector):
    def __init__(self, config: MzDbConfig, current_data_version: int, log_sql: bool):
        super().__init__(config)
        self.current_data_version = current_data_version
        self.update_statements: list[tuple[str, bool]] = []
        self._log_sql = log_sql
        self._read_only = False
        self.open_connection: Connection | None = None
        self.cached_settings: TestAnalyticsSettings | None = None

    def get_or_create_connection(self, autocommit: bool = False) -> Connection:
        if self.open_connection is None:
            self.open_connection = self.create_connection(autocommit=autocommit)
        else:
            self.open_connection.autocommit = autocommit

        return self.open_connection

    def create_connection(
        self,
        autocommit: bool = False,
        timeout_in_seconds: int = 60,
        remaining_retries: int = 1,
    ) -> Connection:
        try:
            connection = psycopg.connect(
                host=self.config.hostname,
                user=self.config.username,
                password=self.config.app_password,
                port=self.config.port,
                sslmode="require",
                connect_timeout=timeout_in_seconds,
                application_name=self.config.application_name,
                options="-c tcp_keepalives_idle=30 -c tcp_keepalives_interval=10 -c tcp_keepalives_count=5",
            )
        except Exception:
            print(
                f"Failed to connect to host '{self.config.hostname}' on port {self.config.port}"
            )
            if remaining_retries > 0:
                print("Retrying in 5 seconds...")
                time.sleep(5)
                return self.create_connection(
                    autocommit=autocommit,
                    timeout_in_seconds=timeout_in_seconds,
                    remaining_retries=remaining_retries - 1,
                )
            else:
                raise

        connection.autocommit = autocommit
        connection.execute(f"SET database = {self.config.database}".encode())
        connection.execute(f"SET search_path = {self.config.search_path}".encode())
        connection.execute(
            f"SET cluster = {as_sanitized_literal(self.config.cluster)}".encode()
        )
        connection.execute("SET transaction_isolation = serializable")

        return connection

    def create_cursor(
        self,
        connection: Connection | None = None,
        autocommit: bool = False,
        allow_reusing_connection: bool = False,
        statement_timeout: str | None = None,
    ) -> Cursor:
        statement_timeout = statement_timeout or self.config.default_statement_timeout

        if connection is None:
            if allow_reusing_connection:
                connection = self.get_or_create_connection(autocommit=autocommit)
            else:
                connection = self.create_connection(autocommit=autocommit)

        cursor = connection.cursor()
        cursor.execute(
            f"SET statement_timeout = {as_sanitized_literal(statement_timeout)}".encode()
        )
        return cursor

    def set_read_only(self) -> None:
        self._read_only = True

    def _get_or_query_settings(self, cursor: Cursor) -> TestAnalyticsSettings:
        if self.cached_settings is None:
            self._query_settings(cursor)

        assert self.cached_settings is not None
        return self.cached_settings

    def _query_settings(self, cursor: Cursor) -> TestAnalyticsSettings:
        cursor.execute("""
            SELECT
                uploads_enabled,
                min_required_data_version_for_uploads,
                only_notify_about_communication_failures_on_main
            FROM config;
            """)

        rows = cursor.fetchall()
        assert len(rows) == 1, f"Expected exactly one row, got {len(rows)}"

        column_values = rows[0]

        settings = TestAnalyticsSettings(
            uploads_enabled=column_values[0],
            min_required_data_version_for_uploads=column_values[1],
            only_notify_about_communication_failures_on_main=column_values[2],
        )

        self.cached_settings = settings
        return settings

    def try_get_or_query_settings(self) -> TestAnalyticsSettings | None:
        try:
            return self._get_or_query_settings(self.create_cursor())
        except:
            return None

    def add_update_statements(
        self, sql_statements: list[str], idempotent: bool = False
    ) -> None:
        self.update_statements.extend((sql, idempotent) for sql in sql_statements)

    def submit_update_statements(self) -> None:
        # Do not use transactions because they do not allow to mix read and write statements (INSERT INTO ... SELECT FROM ...) in mz.

        if not self.config.enabled:
            print("Disabled: Not writing any data to the test analytics database!")
            return

        if len(self.update_statements) == 0:
            return

        cursor = self.create_cursor(autocommit=True)

        self._disable_if_uploads_not_allowed(cursor)

        if self._read_only:
            print(
                "Read-only mode: Not writing any data to the test analytics database!"
            )
            return

        last_executed_sql = dedent(self.update_statements[0][0])
        abandoned_sql: list[str] = []

        print("--- Updates to test analytics database")

        try:
            for sql, idempotent in self.update_statements:
                sql = dedent(sql)
                last_executed_sql = sql
                cursor, outcome_known = self._execute_sql(
                    cursor, sql, idempotent=idempotent, print_status=True
                )
                if not outcome_known:
                    abandoned_sql.append(sql)
        except Exception as e:
            print("Upload failed.")

            error_msg = f"Failed to write to test analytics database! Cause: {e}"
            raise TestAnalyticsUploadError(error_msg, sql=last_executed_sql)
        finally:
            self.update_statements = []

        if abandoned_sql:
            print("Upload incomplete.")

            error_msg = (
                "Lost the connection to the test analytics database while "
                f"{len(abandoned_sql)} statement(s) were in flight! They were not "
                "replayed because re-executing them could have duplicated an "
                "already committed write."
            )
            raise TestAnalyticsUploadError(error_msg, sql=abandoned_sql[0])

        print("Upload completed.")

    def _execute_sql(
        self,
        cursor: Cursor,
        sql: str,
        idempotent: bool,
        print_status: bool = False,
        remaining_retries: int = 1,
    ) -> tuple[Cursor, bool]:
        """Execute sql, retrying once when a retry cannot duplicate its write.

        Returns the cursor to use for subsequent statements, which is a new one
        if the connection had to be re-established, and whether the server-side
        outcome of the statement is known.

        Statements run in autocommit mode, so losing the connection while one is
        in flight leaves its outcome unknowable: the server may have committed
        before the result reached us. Such a statement is only replayed when
        `idempotent` promises a second execution cannot duplicate the write.
        Otherwise it is abandoned with an unknown outcome, because a duplicated
        row corrupts the data silently whereas a missing one is reported. The
        two other failure shapes leave the server untouched and are always safe
        to retry: an error that arrives over a still-usable connection, and a
        statement that was never sent because the connection was already dead.
        """
        if self._log_sql:
            printable_sql = self.to_short_printable_sql(sql)
            print(f"> {printable_sql}")

        was_usable = _is_usable(cursor.connection)

        try:
            start_time = time.time()
            cursor.execute(sql.encode())
            end_time = time.time()

            if print_status:
                affected_rows = cursor.rowcount
                duration_in_sec = round(end_time - start_time, 2)
                print(
                    f"-- OK ({affected_rows} row{'s' if affected_rows != 1 else ''} affected, {duration_in_sec}s)"
                )
        except:
            lost_in_flight = was_usable and not _is_usable(cursor.connection)

            if lost_in_flight and not idempotent:
                print(
                    "-- UNKNOWN OUTCOME! Connection lost while the statement was in "
                    "flight, not replaying it. Reconnecting..."
                )
                return self._reconnect(cursor), False

            if remaining_retries > 0:
                print("-- Retrying in 5 seconds...")
                time.sleep(5)
                if not _is_usable(cursor.connection):
                    print("-- Connection lost, reconnecting...")
                    cursor = self._reconnect(cursor)
                return self._execute_sql(
                    cursor=cursor,
                    sql=sql,
                    idempotent=idempotent,
                    print_status=print_status,
                    remaining_retries=remaining_retries - 1,
                )

            if print_status:
                print("-- FAILED!")
            raise

        return cursor, True

    def _reconnect(self, cursor: Cursor) -> Cursor:
        return self.create_cursor(autocommit=cursor.connection.autocommit)

    def _disable_if_uploads_not_allowed(
        self,
        cursor: Cursor,
    ) -> None:
        print("Fetching test_analytics settings")
        test_analytics_settings = self._query_settings(cursor)

        if not test_analytics_settings.uploads_enabled:
            print("Uploading test_analytics data is disabled!")
            self.set_read_only()
            return

        print(
            f"Current data version is {self.current_data_version}, min required version is {test_analytics_settings.min_required_data_version_for_uploads}"
        )

        if (
            self.current_data_version
            < test_analytics_settings.min_required_data_version_for_uploads
        ):
            print(
                f"Uploading test_analytics data is not supported from this data version ({self.current_data_version})"
            )
            self.set_read_only()

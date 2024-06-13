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


def create_connection(config: MzDbConfig) -> Connection:
    connection = pg8000.connect(
        host=config.hostname,
        user=config.username,
        password=config.app_password,
        port=config.port,
        ssl_context=ssl.SSLContext(),
    )

    if config.auto_commit:
        connection.autocommit = True

    return connection


def create_cursor(config: MzDbConfig, connection: Connection | None = None) -> Cursor:
    if connection is None:
        connection = create_connection(config)

    cursor = connection.cursor()
    cursor.execute(f"SET database = {config.database}")
    cursor.execute(f"SET search_path = {config.search_path}")
    return cursor


def execute_updates(
    sql_statements: list[str], cursor: Cursor, log_sql: bool = True
) -> None:
    if len(sql_statements) == 0:
        return

    last_executed_sql = sql_statements[0]

    try:
        for sql in sql_statements:
            sql = dedent(sql)
            last_executed_sql = sql
            if log_sql:
                print(f"> {sql}")

            cursor.execute(sql)
    except Exception as e:
        error_msg = f"Failed to write to test analytics database! Cause: {e}"
        raise TestAnalyticsUploadError(error_msg, sql=last_executed_sql)

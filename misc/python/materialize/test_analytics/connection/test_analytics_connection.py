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

from materialize.test_analytics.config.test_analytics_db_config import (
    DATABASE,
    MATERIALIZE_PROD_SANDBOX_APP_PASSWORD,
    MATERIALIZE_PROD_SANDBOX_HOSTNAME,
    MATERIALIZE_PROD_SANDBOX_USERNAME,
    SEARCH_PATH,
)


def create_connection(auto_commit: bool = True) -> Connection:
    connection = pg8000.connect(
        host=MATERIALIZE_PROD_SANDBOX_HOSTNAME,
        user=MATERIALIZE_PROD_SANDBOX_USERNAME,
        password=MATERIALIZE_PROD_SANDBOX_APP_PASSWORD,
        port=6875,
        ssl_context=ssl.SSLContext(),
    )

    if auto_commit:
        connection.autocommit = True

    return connection


def create_cursor(connection: Connection) -> Cursor:
    cursor = connection.cursor()
    connection.autocommit = True
    cursor.execute(f"SET database = {DATABASE}")
    cursor.execute(f"SET search_path = {SEARCH_PATH}")
    return cursor


def execute_updates(
    sql_statements: list[str], cursor: Cursor | None = None, verbose: bool = False
) -> None:
    if cursor is None:
        connection = create_connection()
        cursor = create_cursor(connection)

    for sql in sql_statements:
        sql = dedent(sql)
        if verbose:
            print(f"> {sql}")

        cursor.execute(sql)

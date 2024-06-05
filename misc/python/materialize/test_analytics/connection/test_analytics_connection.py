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
    sql_statements: list[str], cursor: Cursor, verbose: bool = False
) -> None:
    for sql in sql_statements:
        sql = dedent(sql)
        if verbose:
            print(f"> {sql}")

        cursor.execute(sql)

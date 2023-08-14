# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Any, List, Optional, Sequence

import psycopg2
from psycopg2.extensions import connection

from materialize.cloudtest.config.environment_config import EnvironmentConfig
from materialize.cloudtest.util.environment import wait_for_environmentd
from materialize.cloudtest.util.http import post


def sql_query(
    conn: connection,
    query: str,
    vars: Optional[Sequence[Any]] = None,
) -> List[List[Any]]:
    cur = conn.cursor()
    cur.execute(query, vars)  # type: ignore
    return [list(row) for row in cur]


def sql_execute(
    conn: connection,
    query: str,
    vars: Optional[Sequence[Any]] = None,
) -> None:
    cur = conn.cursor()
    cur.execute(query, vars)  # type: ignore[no-untyped-call]


def sql_query_pgwire(config: EnvironmentConfig, query: str) -> List[List[Any]]:
    conn = pgwire_sql_conn(config)
    return sql_query(conn, query)


def pgwire_sql_conn(config: EnvironmentConfig) -> connection:
    environment = wait_for_environmentd(config)
    pgwire_url: str = environment["regionInfo"]["sqlAddress"]
    (pgwire_host, pgwire_port) = pgwire_url.split(":")
    conn = psycopg2.connect(
        dbname="materialize",
        user=config.e2e_test_user_email,
        password=config.auth.app_password,
        host=pgwire_host,
        port=pgwire_port,
        sslmode="require",
    )
    conn.autocommit = True
    return conn


def sql_query_http(config: EnvironmentConfig, query: str) -> List[List[Any]]:
    environment = wait_for_environmentd(config)
    environmentd_url: str = environment["regionInfo"]["httpAddress"]
    schema = "http" if "127.0.0.1" in environmentd_url else "https"
    response = post(
        config,
        f"{schema}://{environmentd_url}",
        "/api/sql",
        {"query": query},
    )
    rows: List[List[Any]] = response.json()["results"][0]["rows"]
    return rows

from typing import Any, List, Optional, Sequence

import psycopg2
from psycopg2.extensions import connection

from ..config.environment_config import EnvironmentConfig
from .environment import wait_for_environmentd
from .http import post


def sql_query(
    conn: connection,
    query: str,
    vars: Optional[Sequence[Any]] = None,
) -> List[List[Any]]:
    cur = conn.cursor()
    cur.execute(query, vars)
    return [list(row) for row in cur]


def sql_execute(
    conn: connection,
    query: str,
    vars: Optional[Sequence[Any]] = None,
) -> None:
    cur = conn.cursor()
    cur.execute(query, vars)


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


def sql_query_http(config: EnvironmentConfig, query: str) -> str:
    environment = wait_for_environmentd(config)
    environmentd_url: str = environment["regionInfo"]["httpAddress"]
    schema = "http" if "127.0.0.1" in environmentd_url else "https"
    response = post(
        config,
        f"{schema}://{environmentd_url}",
        "/api/sql",
        {"query": query},
    )
    return response.json()["results"][0]["rows"]

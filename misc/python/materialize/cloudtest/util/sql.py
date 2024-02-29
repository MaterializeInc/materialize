# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import logging
from ipaddress import IPv4Address
from typing import Any

import psycopg
from psycopg.abc import Params, Query
from psycopg.connection import Connection

from materialize.cloudtest.util.authentication import AuthConfig
from materialize.cloudtest.util.environment import Environment
from materialize.cloudtest.util.web_request import WebRequests

LOGGER = logging.getLogger(__name__)


def sql_query(
    conn: Connection[Any],
    query: Query,
    vars: Params | None = None,
) -> list[list[Any]]:
    cur = conn.cursor()
    cur.execute(query, vars)
    return [list(row) for row in cur]


def sql_execute(
    conn: Connection[Any],
    query: Query,
    vars: Params | None = None,
) -> None:
    cur = conn.cursor()
    cur.execute(query, vars)


def sql_execute_ddl(
    conn: Connection[Any],
    query: Query,
    vars: Params | None = None,
) -> None:
    cur = psycopg.ClientCursor(conn)
    cur.execute(query, vars)


def pgwire_sql_conn(auth: AuthConfig, environment: Environment) -> Connection[Any]:
    environment_params = environment.wait_for_environmentd()
    pgwire_url: str = environment_params["regionInfo"]["sqlAddress"]
    (pgwire_host, pgwire_port) = pgwire_url.split(":")
    conn = psycopg.connect(
        dbname="materialize",
        user=auth.app_user,
        password=auth.app_password,
        host=pgwire_host,
        port=pgwire_port,
        sslmode=auth.pgwire_ssl_mode,
        sslrootcert=auth.tls_ca_cert_path,
    )
    conn.autocommit = True
    return conn


def sql_query_pgwire(
    auth: AuthConfig,
    environment: Environment,
    query: Query,
    vars: Params | None = None,
) -> list[list[Any]]:
    with pgwire_sql_conn(auth, environment) as conn:
        LOGGER.info(f"QUERY: {query}")
        return sql_query(conn, query, vars)


def sql_execute_pgwire(
    auth: AuthConfig,
    environment: Environment,
    query: Query,
    vars: Params | None = None,
) -> None:
    with pgwire_sql_conn(auth, environment) as conn:
        LOGGER.info(f"QUERY: {query}")
        return sql_execute(conn, query, vars)


def sql_query_http(
    auth: AuthConfig, environment: Environment, query: str
) -> list[list[Any]]:
    environment_params = environment.wait_for_environmentd()
    environmentd_url: str = environment_params["regionInfo"]["httpAddress"]
    override_ip = (
        IPv4Address("127.0.0.1")
        if environment.env_kubectl.context == "kind-mzcloud"
        else None
    )
    schema = "http" if "127.0.0.1" in environmentd_url else "https"
    verify = (
        "misc/kind/balancer/tls/ca-cert.pem"
        if schema == "https" and override_ip is not None
        else None
    )
    envd_web_requests = WebRequests(
        auth,
        f"{schema}://{environmentd_url}",
        override_ip=override_ip,
        verify=verify,
    )
    response = envd_web_requests.post(
        "/api/sql",
        {"query": query},
    )
    rows: list[list[Any]] = response.json()["results"][0]["rows"]
    return rows

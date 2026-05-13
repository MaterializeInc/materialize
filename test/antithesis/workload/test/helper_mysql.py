# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""MySQL connection helpers for Antithesis drivers.

Connects to the MySQL primary and replica via PyMySQL. All calls retry
transient network and operational errors up to a fixed budget so the
workload keeps progressing under fault injection.
"""

from __future__ import annotations

import logging
import os
import time

import pymysql
import pymysql.cursors

LOG = logging.getLogger("antithesis.helper_mysql")

MYSQL_HOST = os.environ.get("MYSQL_HOST", "mysql")
MYSQL_REPLICA_HOST = os.environ.get("MYSQL_REPLICA_HOST", "mysql-replica")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "p@ssw0rd")

_RETRY_BUDGET_S = 120
_RETRY_INITIAL_S = 0.5
_RETRY_MAX_S = 4.0


def _retryable(exc: BaseException) -> bool:
    return isinstance(exc, pymysql.OperationalError | pymysql.InterfaceError)


def _open(host: str, database: str) -> pymysql.connections.Connection:
    """Open a single MySQL connection with retries on transient errors."""
    deadline = time.monotonic() + _RETRY_BUDGET_S
    backoff = _RETRY_INITIAL_S
    while True:
        try:
            return pymysql.connect(
                host=host,
                port=MYSQL_PORT,
                user="root",
                password=MYSQL_PASSWORD,
                database=database,
                connect_timeout=15,
                autocommit=True,
            )
        except Exception as exc:  # noqa: BLE001
            if not _retryable(exc) or time.monotonic() > deadline:
                raise
            LOG.info(
                "mysql connect to %s retrying after %s; backoff=%.2fs",
                host,
                exc,
                backoff,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, _RETRY_MAX_S)


def _execute(host: str, sql: str, params: tuple = (), database: str = "mysql") -> None:
    deadline = time.monotonic() + _RETRY_BUDGET_S
    backoff = _RETRY_INITIAL_S
    while True:
        try:
            conn = _open(host, database)
            with conn.cursor() as cur:
                cur.execute(sql, params)
            conn.close()
            return
        except Exception as exc:  # noqa: BLE001
            if not _retryable(exc) or time.monotonic() > deadline:
                raise
            LOG.info("mysql execute on %s retrying after %s", host, exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, _RETRY_MAX_S)


def _query(
    host: str, sql: str, params: tuple = (), database: str = "mysql"
) -> list[tuple]:
    deadline = time.monotonic() + _RETRY_BUDGET_S
    backoff = _RETRY_INITIAL_S
    while True:
        try:
            conn = _open(host, database)
            with conn.cursor() as cur:
                cur.execute(sql, params)
                result = list(cur.fetchall())
            conn.close()
            return result
        except Exception as exc:  # noqa: BLE001
            if not _retryable(exc) or time.monotonic() > deadline:
                raise
            LOG.info("mysql query on %s retrying after %s", host, exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, _RETRY_MAX_S)


def execute_primary(sql: str, params: tuple = (), database: str = "mysql") -> None:
    """Execute a statement on the MySQL primary."""
    _execute(MYSQL_HOST, sql, params, database)


def execute_replica(sql: str, params: tuple = (), database: str = "mysql") -> None:
    """Execute a statement on the MySQL replica."""
    _execute(MYSQL_REPLICA_HOST, sql, params, database)


def query_primary(sql: str, params: tuple = (), database: str = "mysql") -> list[tuple]:
    """Run a query on the MySQL primary and return all rows."""
    return _query(MYSQL_HOST, sql, params, database)


def query_replica(sql: str, params: tuple = (), database: str = "mysql") -> list[tuple]:
    """Run a query on the MySQL replica and return all rows."""
    return _query(MYSQL_REPLICA_HOST, sql, params, database)


def wait_for_host(host: str, timeout_s: float = 180.0) -> None:
    """Block until MySQL on `host` accepts connections."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            conn = pymysql.connect(
                host=host,
                port=MYSQL_PORT,
                user="root",
                password=MYSQL_PASSWORD,
                connect_timeout=5,
            )
            conn.close()
            LOG.info("mysql %s is ready", host)
            return
        except Exception as exc:  # noqa: BLE001
            LOG.info("waiting for mysql %s: %s", host, exc)
            time.sleep(2)
    raise TimeoutError(f"MySQL at {host} not ready after {timeout_s}s")


def wait_for_primary(timeout_s: float = 180.0) -> None:
    wait_for_host(MYSQL_HOST, timeout_s)


def wait_for_replica(timeout_s: float = 180.0) -> None:
    wait_for_host(MYSQL_REPLICA_HOST, timeout_s)

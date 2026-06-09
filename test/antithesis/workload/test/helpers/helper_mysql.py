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

# See helper_pg for the rationale on these values. The global fault-
# orchestrator's MAX_ON/MAX_OFF defaults (40s each) mean a per-attempt
# connect_timeout shorter than ~MAX_ON will fast-fail entirely inside a
# faults-ON window, and a retry budget shorter than ~one full ON+OFF cycle
# won't give an attempt a chance to land in the next quiet window. MySQL
# also adds the primary→replica replication path, so the budget is sized
# the same as helper_pg's.
_CONNECT_TIMEOUT_S = 30
_RETRY_BUDGET_S = 180
_RETRY_INITIAL_S = 0.5
_RETRY_MAX_S = 4.0


def _retryable(exc: BaseException) -> bool:
    return isinstance(exc, pymysql.OperationalError | pymysql.InterfaceError)


def _open(host: str, database: str) -> pymysql.connections.Connection:
    """Open a single MySQL connection with retries on transient errors."""
    start = time.monotonic()
    deadline = start + _RETRY_BUDGET_S
    backoff = _RETRY_INITIAL_S
    attempt = 0
    LOG.debug(
        "mysql connect: starting (host=%s db=%s timeout=%ds budget=%ds)",
        host,
        database,
        _CONNECT_TIMEOUT_S,
        _RETRY_BUDGET_S,
    )
    while True:
        attempt += 1
        attempt_start = time.monotonic()
        try:
            conn = pymysql.connect(
                host=host,
                port=MYSQL_PORT,
                user="root",
                password=MYSQL_PASSWORD,
                database=database,
                connect_timeout=_CONNECT_TIMEOUT_S,
                autocommit=True,
            )
            LOG.info(
                "mysql connect: %s established on attempt %d in %.2fs (total %.2fs)",
                host,
                attempt,
                time.monotonic() - attempt_start,
                time.monotonic() - start,
            )
            return conn
        except Exception as exc:  # noqa: BLE001
            elapsed_attempt = time.monotonic() - attempt_start
            elapsed_total = time.monotonic() - start
            if not _retryable(exc) or time.monotonic() > deadline:
                LOG.warning(
                    "mysql connect: %s giving up after attempt %d (%.2fs attempt, %.2fs total): %s",
                    host,
                    attempt,
                    elapsed_attempt,
                    elapsed_total,
                    exc,
                )
                raise
            LOG.info(
                "mysql connect: %s attempt %d failed in %.2fs (%.2fs of %ds used): %s; "
                "sleeping %.2fs",
                host,
                attempt,
                elapsed_attempt,
                elapsed_total,
                _RETRY_BUDGET_S,
                exc,
                backoff,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, _RETRY_MAX_S)


def _execute(host: str, sql: str, params: tuple = (), database: str = "mysql") -> None:
    deadline = time.monotonic() + _RETRY_BUDGET_S
    backoff = _RETRY_INITIAL_S
    while True:
        conn = None
        try:
            conn = _open(host, database)
            with conn.cursor() as cur:
                cur.execute(sql, params)
            return
        except Exception as exc:  # noqa: BLE001
            if not _retryable(exc) or time.monotonic() > deadline:
                raise
            LOG.info("mysql execute on %s retrying after %s", host, exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, _RETRY_MAX_S)
        finally:
            # `_open` + `cur.execute` + return path each open a fresh
            # connection per retry; without explicit close, a sustained
            # fault leaks one socket per retry attempt and eventually
            # exhausts MySQL's max_connections.  Close in `finally` so
            # the success path, the retry path, and the deadline-raise
            # path all release the connection.  pymysql's `close()` is
            # idempotent on already-closed connections.
            if conn is not None:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass


def _query(
    host: str, sql: str, params: tuple = (), database: str = "mysql"
) -> list[tuple]:
    deadline = time.monotonic() + _RETRY_BUDGET_S
    backoff = _RETRY_INITIAL_S
    while True:
        conn = None
        try:
            conn = _open(host, database)
            with conn.cursor() as cur:
                cur.execute(sql, params)
                result = list(cur.fetchall())
            return result
        except Exception as exc:  # noqa: BLE001
            if not _retryable(exc) or time.monotonic() > deadline:
                raise
            LOG.info("mysql query on %s retrying after %s", host, exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, _RETRY_MAX_S)
        finally:
            # See _execute above — close in finally so every exit path
            # releases the connection.
            if conn is not None:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass


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

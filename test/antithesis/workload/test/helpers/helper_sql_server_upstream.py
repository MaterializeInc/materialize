# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""SQL Server upstream connection helpers for Antithesis drivers.

Connects to the SQL Server CDC upstream via pymssql. All calls retry
transient network and operational errors up to a fixed budget so the
workload keeps progressing under fault injection.

The `master` database is the safe default for the initial connect — the
`test` database is created at first_sql_server_cdc_setup.py time and
doesn't exist yet on the very first attempt. Subsequent calls override
the `database=` arg explicitly.
"""

from __future__ import annotations

import logging
import os
import time

import pymssql

LOG = logging.getLogger("antithesis.helper_sql_server_upstream")

SQL_SERVER_HOST = os.environ.get("SQL_SERVER_HOST", "sql-server")
SQL_SERVER_PORT = int(os.environ.get("SQL_SERVER_PORT", "1433"))
SQL_SERVER_USER = os.environ.get("SQL_SERVER_USER", "SA")
SQL_SERVER_PASSWORD = os.environ.get("SQL_SERVER_PASSWORD", "RPSsql12345")
SQL_SERVER_DATABASE = os.environ.get("SQL_SERVER_DATABASE", "antithesis_test")

# Budgets sized to span at least one full fault-orchestrator cycle
# (MAX_ON+MAX_OFF, default 40+40=80s) so a connect attempt has a chance
# to land in the next quiet window even if it started inside a faults-ON
# window.
_CONNECT_TIMEOUT_S = 30
_RETRY_BUDGET_S = 180
_RETRY_INITIAL_S = 0.5
_RETRY_MAX_S = 4.0


def _retryable(exc: BaseException) -> bool:
    # pymssql raises OperationalError for connect failures + transient
    # server-side conditions (e.g. "is in transition" during recovery).
    # InterfaceError fires on torn-down connections / DNS hiccups.
    return isinstance(exc, pymssql.OperationalError | pymssql.InterfaceError)


def _open(database: str) -> pymssql.Connection:
    """Open a single SQL Server connection with retries on transient errors."""
    start = time.monotonic()
    deadline = start + _RETRY_BUDGET_S
    backoff = _RETRY_INITIAL_S
    attempt = 0
    LOG.debug(
        "sql-server connect: starting (host=%s db=%s timeout=%ds budget=%ds)",
        SQL_SERVER_HOST,
        database,
        _CONNECT_TIMEOUT_S,
        _RETRY_BUDGET_S,
    )
    while True:
        attempt += 1
        attempt_start = time.monotonic()
        try:
            conn = pymssql.connect(
                server=SQL_SERVER_HOST,
                port=SQL_SERVER_PORT,
                user=SQL_SERVER_USER,
                password=SQL_SERVER_PASSWORD,
                database=database,
                login_timeout=_CONNECT_TIMEOUT_S,
                timeout=_CONNECT_TIMEOUT_S,
                autocommit=True,
            )
            LOG.info(
                "sql-server connect: established on attempt %d in %.2fs (total %.2fs)",
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
                    "sql-server connect: giving up after attempt %d "
                    "(%.2fs attempt, %.2fs total): %s",
                    attempt,
                    elapsed_attempt,
                    elapsed_total,
                    exc,
                )
                raise
            LOG.info(
                "sql-server connect: attempt %d failed in %.2fs "
                "(%.2fs of %ds used): %s; sleeping %.2fs",
                attempt,
                elapsed_attempt,
                elapsed_total,
                _RETRY_BUDGET_S,
                exc,
                backoff,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, _RETRY_MAX_S)


def execute(sql: str, params: tuple = (), database: str | None = None) -> None:
    """Execute a statement against the SQL Server upstream."""
    db = database if database is not None else SQL_SERVER_DATABASE
    deadline = time.monotonic() + _RETRY_BUDGET_S
    backoff = _RETRY_INITIAL_S
    while True:
        try:
            conn = _open(db)
            with conn.cursor() as cur:
                cur.execute(sql, params)
            conn.close()
            return
        except Exception as exc:  # noqa: BLE001
            if not _retryable(exc) or time.monotonic() > deadline:
                raise
            LOG.info("sql-server execute retrying after %s", exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, _RETRY_MAX_S)


def query(sql: str, params: tuple = (), database: str | None = None) -> list[tuple]:
    """Run a query against the SQL Server upstream and return all rows."""
    db = database if database is not None else SQL_SERVER_DATABASE
    deadline = time.monotonic() + _RETRY_BUDGET_S
    backoff = _RETRY_INITIAL_S
    while True:
        try:
            conn = _open(db)
            with conn.cursor() as cur:
                cur.execute(sql, params)
                result = list(cur.fetchall())
            conn.close()
            return result
        except Exception as exc:  # noqa: BLE001
            if not _retryable(exc) or time.monotonic() > deadline:
                raise
            LOG.info("sql-server query retrying after %s", exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, _RETRY_MAX_S)


def wait_until_ready(timeout_s: float = 300.0) -> None:
    """Block until SQL Server accepts connections against `master`.

    Bootstrapping (msdb recovery, agent start) can take a while in the
    sa_password-fresh container — generous timeout matches the SqlServer
    service's healthcheck start_period of 300s.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            conn = pymssql.connect(
                server=SQL_SERVER_HOST,
                port=SQL_SERVER_PORT,
                user=SQL_SERVER_USER,
                password=SQL_SERVER_PASSWORD,
                database="master",
                login_timeout=5,
                timeout=5,
            )
            conn.close()
            LOG.info("sql-server %s is ready", SQL_SERVER_HOST)
            return
        except Exception as exc:  # noqa: BLE001
            LOG.info("waiting for sql-server %s: %s", SQL_SERVER_HOST, exc)
            time.sleep(2)
    raise TimeoutError(f"SQL Server at {SQL_SERVER_HOST} not ready after {timeout_s}s")

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Upstream-Postgres connection helpers for Antithesis drivers.

Talks to the `postgres-source` container — the PG instance that
Materialize subscribes to via a logical replication slot. Separate from
`helper_pg`, which talks to materialized itself over pgwire.

All calls retry transient network and operational errors up to a fixed
budget so the workload keeps progressing through fault-injection windows.
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Sequence
from typing import Any

import psycopg

LOG = logging.getLogger("antithesis.helper_pg_upstream")

PG_HOST = os.environ.get("PG_SOURCE_HOST", "postgres-source")
PG_PORT = int(os.environ.get("PG_SOURCE_PORT", "5432"))
PG_USER = os.environ.get("PG_SOURCE_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_SOURCE_PASSWORD", "postgres")
PG_DATABASE = os.environ.get("PG_SOURCE_DATABASE", "postgres")

# Mirrors helper_mysql / helper_pg budgets so per-attempt timeouts and
# overall retry budget span at least one full faults-ON + faults-OFF
# orchestrator cycle plus margin for the upstream to actually respond.
_CONNECT_TIMEOUT_S = 30
_RETRY_BUDGET_S = 180
_RETRY_INITIAL_S = 0.5
_RETRY_MAX_S = 4.0


def _retryable(exc: BaseException) -> bool:
    if isinstance(exc, psycopg.OperationalError):
        return True
    if isinstance(exc, psycopg.InterfaceError):
        return True
    return False


def _open() -> psycopg.Connection:
    """Open a single connection to the upstream PG, retrying transients."""
    start = time.monotonic()
    deadline = start + _RETRY_BUDGET_S
    backoff = _RETRY_INITIAL_S
    attempt = 0
    LOG.debug(
        "pg upstream connect: starting (host=%s port=%d db=%s)",
        PG_HOST,
        PG_PORT,
        PG_DATABASE,
    )
    while True:
        attempt += 1
        attempt_start = time.monotonic()
        try:
            conn = psycopg.connect(
                host=PG_HOST,
                port=PG_PORT,
                user=PG_USER,
                password=PG_PASSWORD,
                dbname=PG_DATABASE,
                connect_timeout=_CONNECT_TIMEOUT_S,
                autocommit=True,
            )
            LOG.info(
                "pg upstream connect: established on attempt %d in %.2fs",
                attempt,
                time.monotonic() - attempt_start,
            )
            return conn
        except Exception as exc:  # noqa: BLE001
            if not _retryable(exc) or time.monotonic() > deadline:
                LOG.warning(
                    "pg upstream connect: giving up after %d attempts (%.2fs total): %s",
                    attempt,
                    time.monotonic() - start,
                    exc,
                )
                raise
            LOG.info(
                "pg upstream connect: attempt %d failed: %s; sleeping %.2fs",
                attempt,
                exc,
                backoff,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, _RETRY_MAX_S)


def execute(sql: str, params: Sequence[Any] | None = None) -> None:
    """Execute a statement, retrying transient errors. No result returned."""
    deadline = time.monotonic() + _RETRY_BUDGET_S
    backoff = _RETRY_INITIAL_S
    while True:
        try:
            conn = _open()
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, params or ())
            finally:
                conn.close()
            return
        except Exception as exc:  # noqa: BLE001
            if not _retryable(exc) or time.monotonic() > deadline:
                raise
            LOG.info("pg upstream execute retrying after %s", exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, _RETRY_MAX_S)


def query(sql: str, params: Sequence[Any] | None = None) -> list[tuple[Any, ...]]:
    """Run a query and return all rows, retrying transient errors."""
    deadline = time.monotonic() + _RETRY_BUDGET_S
    backoff = _RETRY_INITIAL_S
    while True:
        try:
            conn = _open()
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, params or ())
                    rows = list(cur.fetchall())
            finally:
                conn.close()
            return rows
        except Exception as exc:  # noqa: BLE001
            if not _retryable(exc) or time.monotonic() > deadline:
                raise
            LOG.info("pg upstream query retrying after %s", exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, _RETRY_MAX_S)


def wait_until_ready(timeout_s: float = 180.0) -> None:
    """Block until the upstream PG accepts connections."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            conn = psycopg.connect(
                host=PG_HOST,
                port=PG_PORT,
                user=PG_USER,
                password=PG_PASSWORD,
                dbname=PG_DATABASE,
                connect_timeout=5,
            )
            conn.close()
            LOG.info("pg upstream %s is ready", PG_HOST)
            return
        except Exception as exc:  # noqa: BLE001
            LOG.info("waiting for pg upstream %s: %s", PG_HOST, exc)
            time.sleep(2)
    raise TimeoutError(f"upstream Postgres at {PG_HOST} not ready after {timeout_s}s")

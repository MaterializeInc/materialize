# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Resilient Materialize/pgwire connection helpers for Antithesis drivers.

The workload runs under active fault injection. Every call retries network and
admission errors transparently; everything else propagates.
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

import psycopg

LOG = logging.getLogger("antithesis.helper_pg")

PGHOST = os.environ.get("PGHOST", "materialized")
PGPORT = int(os.environ.get("PGPORT", "6875"))
PGUSER = os.environ.get("PGUSER", "materialize")
PGDATABASE = os.environ.get("PGDATABASE", "materialize")

# Internal pgwire endpoint for system-privileged operations (ALTER SYSTEM SET).
PGPORT_INTERNAL = int(os.environ.get("PGPORT_INTERNAL", "6877"))
PGUSER_INTERNAL = os.environ.get("PGUSER_INTERNAL", "mz_system")

# Retry tuning. Antithesis injects partitions and node hangs; conservative bounds
# keep drivers progressing without masking real correctness signals.
_CONNECT_TIMEOUT_S = 5
_RETRY_BUDGET_S = 60
_RETRY_INITIAL_S = 0.1
_RETRY_MAX_S = 2.0


def _retryable(exc: BaseException) -> bool:
    if isinstance(exc, psycopg.OperationalError):
        return True
    # psycopg wraps server-side admin shutdowns as InterfaceError on next op.
    if isinstance(exc, psycopg.InterfaceError):
        return True
    return False


@contextmanager
def connect(autocommit: bool = True) -> Iterator[psycopg.Connection]:
    """Yield a connection, retrying transient failures up to RETRY_BUDGET_S."""
    deadline = time.monotonic() + _RETRY_BUDGET_S
    backoff = _RETRY_INITIAL_S
    while True:
        try:
            conn = psycopg.connect(
                host=PGHOST,
                port=PGPORT,
                user=PGUSER,
                dbname=PGDATABASE,
                connect_timeout=_CONNECT_TIMEOUT_S,
                autocommit=autocommit,
            )
            break
        except Exception as exc:  # noqa: BLE001
            if not _retryable(exc) or time.monotonic() > deadline:
                raise
            LOG.info("pg connect retrying after %s; backoff=%.2fs", exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, _RETRY_MAX_S)
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


def execute_retry(sql: str, params: Sequence[Any] | None = None) -> None:
    """Execute a statement, retrying transient errors. No result returned."""
    deadline = time.monotonic() + _RETRY_BUDGET_S
    backoff = _RETRY_INITIAL_S
    while True:
        try:
            with connect() as conn, conn.cursor() as cur:
                cur.execute(sql, params or ())
            return
        except Exception as exc:  # noqa: BLE001
            if not _retryable(exc) or time.monotonic() > deadline:
                raise
            LOG.info("pg execute retrying after %s", exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, _RETRY_MAX_S)


def query_retry(sql: str, params: Sequence[Any] | None = None) -> list[tuple[Any, ...]]:
    """Run a query and return all rows, retrying transient errors."""
    deadline = time.monotonic() + _RETRY_BUDGET_S
    backoff = _RETRY_INITIAL_S
    while True:
        try:
            with connect() as conn, conn.cursor() as cur:
                cur.execute(sql, params or ())
                return list(cur.fetchall())
        except Exception as exc:  # noqa: BLE001
            if not _retryable(exc) or time.monotonic() > deadline:
                raise
            LOG.info("pg query retrying after %s", exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, _RETRY_MAX_S)


def query_one_retry(
    sql: str, params: Sequence[Any] | None = None
) -> tuple[Any, ...] | None:
    rows = query_retry(sql, params)
    return rows[0] if rows else None


def execute_internal_retry(sql: str, params: Sequence[Any] | None = None) -> None:
    """Execute a system-privileged statement on the internal port (mz_system).

    Used for ALTER SYSTEM SET and other operations the regular `materialize`
    role cannot perform. Retries the same transient errors as `execute_retry`.
    """
    deadline = time.monotonic() + _RETRY_BUDGET_S
    backoff = _RETRY_INITIAL_S
    while True:
        try:
            with psycopg.connect(
                host=PGHOST,
                port=PGPORT_INTERNAL,
                user=PGUSER_INTERNAL,
                dbname=PGDATABASE,
                connect_timeout=_CONNECT_TIMEOUT_S,
                autocommit=True,
            ) as conn, conn.cursor() as cur:
                cur.execute(sql, params or ())
            return
        except Exception as exc:  # noqa: BLE001
            if not _retryable(exc) or time.monotonic() > deadline:
                raise
            LOG.info("pg internal execute retrying after %s", exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, _RETRY_MAX_S)


def create_source_idempotent(create_sql: str, source_name: str) -> None:
    """Run a CREATE SOURCE statement, tolerating IF-NOT-EXISTS race gaps.

    `CREATE SOURCE IF NOT EXISTS` only short-circuits on the primary source
    name. When two driver invocations race past the existence check, or when
    a fault-injected crash mid-DDL leaves an orphan `<name>_progress`
    subsource in the catalog, the primary create errors with "catalog item
    ... already exists" despite `IF NOT EXISTS`. Re-check `mz_sources` after
    such an error; if the source landed concurrently, treat as success.
    Otherwise re-raise so a true orphan still surfaces.
    """
    try:
        execute_retry(create_sql)
        return
    except psycopg.errors.InternalError as exc:
        if "already exists" not in str(exc):
            raise
        rows = query_retry(
            "SELECT 1 FROM mz_sources WHERE name = %s",
            (source_name,),
        )
        if rows:
            LOG.info("source %s landed concurrently; tolerating collision", source_name)
            return
        raise

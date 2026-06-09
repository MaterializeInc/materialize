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
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from typing import Any, TypeVar

import psycopg

# Canonical fault-shape classifier shared with every other driver.  See
# `helper_fault_tolerance` for what qualifies as fault-shaped.  Used
# below in `_retryable` so server-initiated drops / admission-control
# / restart-window strings are absorbed without each driver maintaining
# its own copy of the pattern list.
from helper_fault_tolerance import looks_like_fault

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
#
# The global fault-orchestrator alternates faults-ON/OFF windows of up to
# MAX_ON / MAX_OFF seconds each (defaults 40s, defined in
# test/antithesis/mzcompose.py FaultOrchestrator). One full
# fault-ON+fault-OFF cycle is up to MAX_ON+MAX_OFF ~= 80s.
#
# Per-attempt connect_timeout must be long enough that an attempt starting
# late in a faults-ON window has a real chance of completing across the
# transition into the next faults-OFF window. A 15s timeout entirely inside
# a 40s faults-ON window fast-fails before the orchestrator opens a quiet
# period, burning retry budget on TCP timeouts rather than waiting for
# materialized to be reachable.
#
# Retry budget must comfortably span at least one full ON+OFF cycle plus
# margin for the system to actually respond once faults pause.
CONNECT_TIMEOUT_S = 30
_RETRY_BUDGET_S = 180
_RETRY_INITIAL_S = 0.1
_RETRY_MAX_S = 2.0


def _truncate_sql(sql: str, max_len: int = 120) -> str:
    """Single-line truncation for logging."""
    flat = " ".join(sql.split())
    return flat if len(flat) <= max_len else flat[: max_len - 3] + "..."


# Substring matches for server-side `InternalError` whose root cause is a
# transient external dependency rather than a workload bug. Materialize
# surfaces broker/upstream validation failures during DDL (notably
# `CREATE SOURCE FOR KAFKA`, which does a broker metadata fetch as part
# of validation) as plain `InternalError`, *not* `OperationalError`, so
# the default psycopg classification treats them as non-retryable. Under
# the global fault-orchestrator a broker pause is expected to clear in
# the next quiet window; we should keep trying.
#
# Keep the patterns specific enough that we don't accidentally swallow
# real schema errors. Anything not matched here still propagates after
# one attempt.
# Server-side `InternalError` patterns specific to pgwire callers that
# `helper_fault_tolerance.FAULT_PATTERNS` doesn't cover.  These surface
# during DDL validation when an upstream component (broker / schema
# registry / source PG) is faulted.  Anything in `FAULT_PATTERNS` is
# matched first (via `looks_like_fault`) so this list is the additive
# pgwire-only delta — pgwire-helper-specific patterns that don't fit
# the cross-driver "looks like a kill/pause" categorisation.
_TRANSIENT_PGWIRE_PATTERNS = (
    # librdkafka error surfaces (CREATE SOURCE / CREATE CONNECTION validation)
    "Meta data fetch error",
    "Local: All broker connections are down",
    "Local: Timed out",
    # schema-registry HTTP failures during CREATE SOURCE FORMAT AVRO ...
    "schema registry",
    # postgres / mysql source validation reach-the-upstream failures
    "could not translate host name",
)


def _retryable(exc: BaseException) -> bool:
    if isinstance(exc, psycopg.OperationalError):
        return True
    # psycopg wraps server-side admin shutdowns as InterfaceError on next op.
    if isinstance(exc, psycopg.InterfaceError):
        return True
    # Server-side InternalError caused by a transient external dependency.
    # First check the canonical cross-driver `FAULT_PATTERNS` (kept in
    # sync centrally — see `helper_fault_tolerance`), then the pgwire-
    # specific additive list.  Without the FAULT_PATTERNS branch, a
    # server-initiated drop wording like
    # `terminating connection due to administrator command` or
    # `is (re)initializing` would surface as a hard failure even though
    # every *other* driver (testdrive, parallel-workload) already knows
    # those are fault-shaped.
    if isinstance(exc, psycopg.errors.InternalError):
        msg = str(exc)
        if looks_like_fault(msg):
            return True
        return any(pat in msg for pat in _TRANSIENT_PGWIRE_PATTERNS)
    return False


@contextmanager
def connect(autocommit: bool = True) -> Iterator[psycopg.Connection]:
    """Yield a connection, retrying transient failures up to RETRY_BUDGET_S."""
    start = time.monotonic()
    deadline = start + _RETRY_BUDGET_S
    backoff = _RETRY_INITIAL_S
    attempt = 0
    LOG.debug(
        "pg connect: starting (host=%s port=%d timeout=%ds budget=%ds)",
        PGHOST,
        PGPORT,
        CONNECT_TIMEOUT_S,
        _RETRY_BUDGET_S,
    )
    while True:
        attempt += 1
        attempt_start = time.monotonic()
        try:
            conn = psycopg.connect(
                host=PGHOST,
                port=PGPORT,
                user=PGUSER,
                dbname=PGDATABASE,
                connect_timeout=CONNECT_TIMEOUT_S,
                autocommit=autocommit,
            )
            LOG.info(
                "pg connect: established on attempt %d in %.2fs (total %.2fs)",
                attempt,
                time.monotonic() - attempt_start,
                time.monotonic() - start,
            )
            break
        except Exception as exc:  # noqa: BLE001
            elapsed_attempt = time.monotonic() - attempt_start
            elapsed_total = time.monotonic() - start
            if not _retryable(exc) or time.monotonic() > deadline:
                LOG.warning(
                    "pg connect: giving up after attempt %d (%.2fs attempt, %.2fs total): %s",
                    attempt,
                    elapsed_attempt,
                    elapsed_total,
                    exc,
                )
                raise
            LOG.info(
                "pg connect: attempt %d failed in %.2fs (%.2fs of %ds budget used): %s; "
                "sleeping %.2fs",
                attempt,
                elapsed_attempt,
                elapsed_total,
                _RETRY_BUDGET_S,
                exc,
                backoff,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, _RETRY_MAX_S)
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


T = TypeVar("T")


def _retry_loop(label: str, sql_summary: str, op: Callable[[], T]) -> T:
    """Shared retry/backoff/deadline loop used by every pgwire helper.

    `op` is a no-arg callable that opens a fresh connection, runs the
    intended SQL, closes the connection, and either returns a value or
    raises.  The loop classifies exceptions via `_retryable` (which
    consults the canonical `helper_fault_tolerance.FAULT_PATTERNS`
    plus a small pgwire-only additive list) and retries with
    exponential backoff up to `_RETRY_BUDGET_S`.  Non-retryable
    exceptions bubble immediately.

    Centralising the loop here means a behaviour change to the retry
    semantics (e.g. expanding the fault-pattern list, adjusting the
    budget) lands in one place instead of three.  The previous
    structure had three near-identical 30-line copies that had
    already drifted: `execute_internal_retry` opened its own
    `psycopg.connect` rather than going through `connect()`, so any
    improvement to the connect helper had to be applied twice.
    """
    start = time.monotonic()
    deadline = start + _RETRY_BUDGET_S
    backoff = _RETRY_INITIAL_S
    attempt = 0
    while True:
        attempt += 1
        try:
            result = op()
            LOG.debug(
                "pg %s: ok on attempt %d in %.2fs (%s)",
                label,
                attempt,
                time.monotonic() - start,
                sql_summary,
            )
            return result
        except Exception as exc:  # noqa: BLE001
            if not _retryable(exc) or time.monotonic() > deadline:
                LOG.warning(
                    "pg %s: giving up after %d attempts (%.2fs total) on %s: %s",
                    label,
                    attempt,
                    time.monotonic() - start,
                    sql_summary,
                    exc,
                )
                raise
            LOG.info(
                "pg %s: attempt %d failed (%.2fs of %ds used) on %s: %s",
                label,
                attempt,
                time.monotonic() - start,
                _RETRY_BUDGET_S,
                sql_summary,
                exc,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, _RETRY_MAX_S)


def execute_retry(sql: str, params: Sequence[Any] | None = None) -> None:
    """Execute a statement, retrying transient errors. No result returned."""
    sql_summary = _truncate_sql(sql)
    LOG.debug("pg execute: %s", sql_summary)

    def _op() -> None:
        with connect() as conn, conn.cursor() as cur:
            cur.execute(sql, params or ())

    _retry_loop("execute", sql_summary, _op)


def query_retry(
    sql: str,
    params: Sequence[Any] | None = None,
    real_time_recency: bool = False,
) -> list[tuple[Any, ...]]:
    """Run a query and return all rows, retrying transient errors.

    Set `real_time_recency=True` when the query is a queryability gate after a
    just-produced upstream write. With strict-serializable (the workload
    default) plus real-time recency, the coordinator pushes the SELECT
    timestamp's lower bound to the source's real-time frontier — i.e. the
    SELECT waits for ingestion to reach the broker/upstream's current
    high-water mark before responding. Without this, `wait_for_catchup` on
    `mz_source_statistics.offset_committed` can clear before the just-ingested
    rows are visible at the timestamp the SELECT chooses (`offset_committed`
    tracks the data-shard upper, which can advance past `oracle_read_ts` while
    the rows live at an mz_ts further forward — assigned by the reclock's
    next-probe binding).
    """
    sql_summary = _truncate_sql(sql)
    LOG.debug("pg query: %s (rtr=%s)", sql_summary, real_time_recency)

    def _op() -> list[tuple[Any, ...]]:
        with connect() as conn, conn.cursor() as cur:
            if real_time_recency:
                cur.execute("SET real_time_recency = TRUE")
            cur.execute(sql, params or ())
            return list(cur.fetchall())

    return _retry_loop("query", sql_summary, _op)


def query_one_retry(
    sql: str,
    params: Sequence[Any] | None = None,
    real_time_recency: bool = False,
) -> tuple[Any, ...] | None:
    rows = query_retry(sql, params, real_time_recency=real_time_recency)
    return rows[0] if rows else None


def execute_internal_retry(sql: str, params: Sequence[Any] | None = None) -> None:
    """Execute a system-privileged statement on the internal port (mz_system).

    Used for ALTER SYSTEM SET and other operations the regular `materialize`
    role cannot perform. Retries the same transient errors as `execute_retry`.

    Doesn't go through `connect()` because that helper targets the
    user pgwire port (PGPORT) — internal-port connections need the
    `mz_system` superuser on PGPORT_INTERNAL.  Connection setup uses
    a single `psycopg.connect` call; the outer `_retry_loop` handles
    connect-level fault windows by reattempting from scratch, so
    losing the connect-level retry that `connect()` has internally
    isn't a problem (each retry-loop attempt is a fresh
    connect+exec+close cycle).
    """
    sql_summary = _truncate_sql(sql)
    LOG.debug("pg internal execute: %s", sql_summary)

    def _op() -> None:
        with (
            psycopg.connect(
                host=PGHOST,
                port=PGPORT_INTERNAL,
                user=PGUSER_INTERNAL,
                dbname=PGDATABASE,
                connect_timeout=CONNECT_TIMEOUT_S,
                autocommit=True,
            ) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(sql, params or ())

    _retry_loop("internal execute", sql_summary, _op)


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

#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver: replay a captured Materialize workload's queries.

Loads the workload YAML bundled at image-build time
(`captured/console_cluster_tabs.yml` by default) and spawns a small
pool of worker threads that pull random queries off the captured
`queries` list and execute them against the live `materialized` over
pgwire.  Container-level fault injection runs alongside.

The captured workload is a 1-hour slice of Materialize web-console
introspection traffic — every query targets the built-in
`mz_catalog_server` cluster and the `materialize` database.  There are
no user clusters / databases / sources / sinks to recreate, which is
why the workload-replay group's topology is the bare universal set.

Why this matters for fault injection:
  * The pgwire serving path, the adapter's catalog reads, and the
    transient-result caching in `mz_catalog_server` collectively
    process every console query — bugs here surface as panics, internal
    errors, or stalls that Antithesis flags.
  * SUBSCRIBE queries (6 of the 212 distinct shapes in the bundled
    capture) exercise the long-poll streaming path, which under fault
    injection has historically tripped on session lifecycle bugs.
  * Concurrent driver invocations + concurrent worker threads per
    invocation mean dozens of catalog readers race against any
    fault-injected restart of clusterd / environmentd / postgres-
    metadata / minio.

Property shape (same as parallel-workload):
  * `always(no_unexpected_error)` — replay only saw fault-shaped errors
    or known captured-vs-current-build mismatches.
  * `sometimes(queries_succeeded > 0)` — at least one query ran
    cleanly per timeline, otherwise the safety check is vacuous.
  * `sometimes(fault_absorbed)` — at some point a fault was caught and
    demoted, proving the absorption path is actually exercised.

Per-invocation budget is bounded so Antithesis re-launches the driver
freely (the standard parallel-driver pattern).  Defaults: 4 worker
threads × 60 s per invocation × random pick of up to 60 queries per
invocation.
"""

from __future__ import annotations

import queue
import sys
import threading
import time
from typing import Any

import helper_logging
import helper_random
import helper_workload_replay
import psycopg
from antithesis.assertions import always, sometimes
from helper_fault_tolerance import looks_like_fault
from helper_pg import (
    PGDATABASE,
    PGHOST,
    PGPORT_INTERNAL,
    PGUSER_INTERNAL,
)
from helper_workload_replay import (
    env_float,
    env_int,
    is_expected_replay_error,
)
from psycopg.sql import SQL, Literal

LOG = helper_logging.setup_logging("driver.workload_replay")

# Per-invocation runtime envelope.  Sized so each invocation can do a
# few dozen queries (most catalog queries finish in <100 ms, but some
# of the heavier CTEs cross 1 s) while leaving Antithesis room to
# launch several invocations per timeline.  Tunable via env in case
# triage wants to lengthen or shrink the per-invocation window.
RUNTIME_S = env_float("WR_RUNTIME_S", 60.0)
NUM_THREADS = env_int("WR_THREADS", 4)
# Random sample size per invocation.  Cap rather than full replay so
# concurrent invocations explore different subsets of the captured
# space — important because Antithesis's coverage signal comes from
# fresh draws per timeline, not from one driver hitting every query.
MAX_QUERIES = env_int("WR_MAX_QUERIES", 60)
# Per-statement timeout in milliseconds.  SUBSCRIBE replay would
# otherwise stream forever; SELECT replay can hit catastrophic plans
# on the heavier CTE shapes when introspection state is unusual under
# fault injection.  3 s is short enough to keep the worker pool
# churning yet long enough that fast queries don't trip on it.
STATEMENT_TIMEOUT_MS = env_int("WR_STATEMENT_TIMEOUT_MS", 3000)
# Per-attempt psql connect timeout.  Helper-default is 30 s (sized for
# long-running drivers); for the workload-replay short 60 s invocation
# that's half the budget eaten on a single failed connect.  A 10 s
# timeout fail-fasts inside a fault window, lets the worker loop
# observe the fault and retry on the next cycle, and the helper's
# overall retry budget still bounds total wall-clock.
WORKER_CONNECT_TIMEOUT_S = env_int("WR_CONNECT_TIMEOUT_S", 10)
# Cadence for the in-run progress log line.  A 60s invocation with no
# output between start and end is hard to triage when faults stall the
# worker pool; emitting a merged-stats snapshot every PROGRESS_INTERVAL_S
# means the workload-container logs show forward motion (or its absence)
# without parsing per-thread state.  Short enough to land at least 3-4
# lines inside a default 60s window; long enough not to drown the log
# in idle ticks.
PROGRESS_INTERVAL_S = env_float("WR_PROGRESS_INTERVAL_S", 15.0)


def _set_session(
    cur: psycopg.Cursor[Any],
    q: dict[str, Any],
    last: dict[str, Any],
) -> None:
    """Apply the captured per-query session settings, skipping no-op SETs.

    Each query in the workload YAML records the cluster, database,
    search_path, and isolation level it was executed against.  Mirror
    them here so the catalog snapshot the SUT sees matches the capture.

    The bundled capture has 212 distinct queries that share three of
    the four session-shape fields (cluster, database, transaction
    isolation are identical for every query — only search_path varies).
    Re-issuing every SET on every query meant 6 round-trips per query
    even on the common path; under fault windows where each round-trip
    can stall the per-query budget eats up before any query runs.
    `last` caches the values already applied on this connection so
    we only emit a SET when the value changes (or on first use after
    connect, when every key is missing).  `statement_timeout` is
    applied once per connection rather than per query for the same
    reason: it's a stable constant for the whole driver lifetime.

    Materialize's `SET` does not accept extended-protocol parameters,
    so every value is inlined via psycopg's `SQL.format(Literal(...))`
    rather than passed as a bind parameter (mirrors
    `materialize.workload_replay.replay.run_query`).
    """
    iso = q.get("transaction_isolation")
    if iso and last.get("transaction_isolation") != iso:
        cur.execute(SQL("SET transaction_isolation = {}").format(Literal(iso)))
        last["transaction_isolation"] = iso
    cluster = q.get("cluster")
    if cluster and last.get("cluster") != cluster:
        cur.execute(SQL("SET cluster = {}").format(Literal(cluster)))
        last["cluster"] = cluster
    database = q.get("database")
    if database and last.get("database") != database:
        cur.execute(SQL("SET database = {}").format(Literal(database)))
        last["database"] = database
    search_path = q.get("search_path") or []
    if search_path:
        # `search_path` is a comma-separated list of bare identifiers
        # in PG syntax; we trust the capture's values rather than
        # quoting because the captured paths are all bare identifiers.
        joined = ",".join(search_path)
        if last.get("search_path") != joined:
            cur.execute(f"SET search_path = {joined}".encode())
            last["search_path"] = joined
    if not last.get("statement_timeout"):
        # statement_timeout is a stable constant for this driver — set
        # it once on first use and trust the connection lifetime.
        cur.execute(
            SQL("SET statement_timeout = {}").format(Literal(STATEMENT_TIMEOUT_MS))
        )
        last["statement_timeout"] = STATEMENT_TIMEOUT_MS


_PARAM_PLACEHOLDER = "$"


def _convert_pg_params(sql: str, params: list[Any]) -> tuple[bytes, list[Any]]:
    """Convert `$1`-style placeholders to psycopg's `%s` style.

    The capture records SQL with PG-native `$N` placeholders (which is
    what the wire protocol carries when an extended-protocol Prepare
    happens client-side).  psycopg's `cur.execute` expects `%s`.  Also
    double `%` to `%%` first so any literal `%` in the SQL isn't
    interpreted as a placeholder marker.
    """
    if not params:
        return sql.replace("%", "%%").encode(), []
    out_params: list[Any] = []
    parts: list[str] = []
    sql_escaped = sql.replace("%", "%%")
    i = 0
    while i < len(sql_escaped):
        ch = sql_escaped[i]
        if (
            ch == _PARAM_PLACEHOLDER
            and i + 1 < len(sql_escaped)
            and sql_escaped[i + 1].isdigit()
        ):
            j = i + 1
            while j < len(sql_escaped) and sql_escaped[j].isdigit():
                j += 1
            idx = int(sql_escaped[i + 1 : j]) - 1
            if 0 <= idx < len(params):
                out_params.append(params[idx])
                parts.append("%s")
            else:
                # Bad capture — the param list doesn't cover the
                # placeholder.  Leave the literal `$N` in place; the
                # SUT will error and we'll demote via
                # `is_expected_replay_error`.
                parts.append(sql_escaped[i:j])
            i = j
        else:
            parts.append(ch)
            i += 1
    return "".join(parts).encode(), out_params


def _run_one(
    conn: psycopg.Connection,
    q: dict[str, Any],
    stats: dict[str, int],
    session_cache: dict[str, Any],
) -> None:
    """Execute one captured query.  Updates `stats` in place.

    `session_cache` is per-connection state used by `_set_session` to
    skip no-op SETs (see that function's docstring); cleared by the
    worker on reconnect.

    Errors are classified into three buckets:
      * Looks-like-fault (network-level disconnect, broker timeout, …)
        → counted under `fault_absorbed`.
      * Expected replay error (catalog drift since capture, bad cast on
        param) → counted under `expected_skipped`.
      * Anything else → counted under `unexpected_error` and the first
        such message is sent up via `stats['first_unexpected']`.
    """
    with conn.cursor() as cur:
        try:
            _set_session(cur, q, session_cache)
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            if looks_like_fault(msg):
                stats["fault_absorbed"] += 1
            else:
                stats["session_setup_error"] += 1
                stats.setdefault("first_unexpected", f"SET failed: {msg}")
            return

        sql_bytes, params = _convert_pg_params(q["sql"], q.get("params") or [])
        statement_type = q.get("statement_type", "select")
        try:
            if statement_type == "subscribe":
                # statement_timeout (set in _set_session) bounds the
                # streaming read; consume in a tight loop until either
                # the per-query timeout fires (caught below) or the
                # iterator drains, which for a typical SUBSCRIBE only
                # happens on partition close.
                cur.execute(sql_bytes, params)
                rows = 0
                for _ in cur:
                    rows += 1
                    if rows > 1000:
                        # Stop pulling once we have enough evidence the
                        # SUBSCRIBE plan is producing rows; the goal is
                        # exercise + safety, not exhaustive read.
                        break
                stats["subscribe_rows"] += rows
            else:
                cur.execute(sql_bytes, params)
                if cur.description is not None:
                    # Drain so the round trip completes; we don't care
                    # about the actual rows.
                    cur.fetchall()
            stats["succeeded"] += 1
        except psycopg.errors.QueryCanceled:
            # statement_timeout fired — common for the heavier CTEs
            # during fault windows.  Not a property violation.
            stats["timed_out"] += 1
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            if looks_like_fault(msg):
                stats["fault_absorbed"] += 1
            elif is_expected_replay_error(msg):
                stats["expected_skipped"] += 1
            else:
                stats["unexpected_error"] += 1
                stats.setdefault(
                    "first_unexpected",
                    f"{statement_type}: {msg[:500]}",
                )


def _worker(
    q_queue: queue.Queue[dict[str, Any]],
    stop_event: threading.Event,
    stats: dict[str, int],
    end_time: float,
) -> None:
    """Worker thread loop.

    Opens its own pgwire connection (reconnects on close), then pulls
    queries off the shared queue and replays them until either the
    queue empties, the per-invocation deadline passes, or `stop_event`
    fires (set by `main` if a worker is going to abort).
    """
    while time.time() < end_time and not stop_event.is_set():
        try:
            conn = psycopg.connect(
                host=PGHOST,
                port=PGPORT_INTERNAL,
                user=PGUSER_INTERNAL,
                dbname=PGDATABASE,
                autocommit=True,
                connect_timeout=WORKER_CONNECT_TIMEOUT_S,
            )
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            if looks_like_fault(msg):
                stats["fault_absorbed"] += 1
                time.sleep(1.0)
                continue
            stats["connect_error"] += 1
            stats.setdefault("first_unexpected", f"connect: {msg}")
            return

        # Per-connection session-state cache used by `_set_session` to
        # skip no-op SETs.  Reset on each new connection since the
        # SUT-side session state is fresh.
        session_cache: dict[str, Any] = {}
        try:
            while time.time() < end_time and not stop_event.is_set():
                try:
                    q = q_queue.get_nowait()
                except queue.Empty:
                    return
                _run_one(conn, q, stats, session_cache)
                # `_run_one` may have left the connection in an aborted
                # state if a fault landed mid-statement; psycopg's
                # `closed` flag tells us when to reconnect.
                if conn.closed:
                    break
        finally:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass


def _merge_stats(per_thread: list[dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "succeeded": 0,
        "timed_out": 0,
        "fault_absorbed": 0,
        "expected_skipped": 0,
        "unexpected_error": 0,
        "session_setup_error": 0,
        "connect_error": 0,
        "subscribe_rows": 0,
        "first_unexpected": None,
    }
    for s in per_thread:
        for k, v in s.items():
            if k == "first_unexpected":
                if out["first_unexpected"] is None and v is not None:
                    out["first_unexpected"] = v
            else:
                out[k] = out.get(k, 0) + v
    return out


def main() -> int:
    workload = helper_workload_replay.load_workload()
    all_queries = helper_workload_replay.replayable_queries(workload)
    if not all_queries:
        LOG.warning("bundled workload has no replayable queries; exiting cleanly")
        return 0

    rng = helper_random.AntithesisRandom()
    # Sample without replacement so this invocation explores a spread
    # of query shapes rather than picking the same heavy query
    # repeatedly.  Across many invocations the union of samples gives
    # broad coverage.
    sample = rng.sample(all_queries, min(MAX_QUERIES, len(all_queries)))

    LOG.info(
        "workload-replay starting: sample=%d (of %d) threads=%d runtime=%ss",
        len(sample),
        len(all_queries),
        NUM_THREADS,
        RUNTIME_S,
    )

    q_queue: queue.Queue[dict[str, Any]] = queue.Queue()
    for q in sample:
        q_queue.put(q)

    end_time = time.time() + RUNTIME_S
    stop_event = threading.Event()
    per_thread: list[dict[str, Any]] = []
    threads: list[threading.Thread] = []
    for i in range(NUM_THREADS):
        stats: dict[str, Any] = {
            "succeeded": 0,
            "timed_out": 0,
            "fault_absorbed": 0,
            "expected_skipped": 0,
            "unexpected_error": 0,
            "session_setup_error": 0,
            "connect_error": 0,
            "subscribe_rows": 0,
        }
        per_thread.append(stats)
        t = threading.Thread(
            name=f"wr-worker-{i}",
            target=_worker,
            args=(q_queue, stop_event, stats, end_time),
        )
        t.start()
        threads.append(t)

    # Periodic mid-run progress log.  A 60s invocation with no output
    # in the middle is hard to triage when a fault stalls every worker
    # — print live merged-stats every PROGRESS_INTERVAL_S so logs show
    # forward motion (or its absence) without parsing per-thread state.
    next_progress = time.time() + PROGRESS_INTERVAL_S
    while any(t.is_alive() for t in threads):
        now = time.time()
        if now >= next_progress:
            snap = _merge_stats(per_thread)
            LOG.info(
                "[PROGRESS] workload-replay live: succeeded=%d timed_out=%d "
                "fault_absorbed=%d unexpected_error=%d (t+%ds)",
                snap["succeeded"],
                snap["timed_out"],
                snap["fault_absorbed"],
                snap["unexpected_error"],
                int(now - (end_time - RUNTIME_S)),
            )
            next_progress = now + PROGRESS_INTERVAL_S
        # Cap remaining-time so we don't overshoot end_time + margin.
        remaining = max(0.0, (end_time + 30.0) - now)
        for t in threads:
            if not t.is_alive():
                continue
            t.join(timeout=min(PROGRESS_INTERVAL_S, remaining))
            break  # re-check elapsed before joining the next thread
        if time.time() > end_time + 30.0:
            break
    # Catch up: any thread still alive past the window gets a stop +
    # short final join.  Matches the original 5s grace period.
    for t in threads:
        if t.is_alive():
            stop_event.set()
            t.join(timeout=5.0)

    merged = _merge_stats(per_thread)

    LOG.info(
        "[SUMMARY] workload-replay done: succeeded=%d timed_out=%d fault_absorbed=%d "
        "expected_skipped=%d unexpected_error=%d session_setup_error=%d "
        "connect_error=%d subscribe_rows=%d",
        merged["succeeded"],
        merged["timed_out"],
        merged["fault_absorbed"],
        merged["expected_skipped"],
        merged["unexpected_error"],
        merged["session_setup_error"],
        merged["connect_error"],
        merged["subscribe_rows"],
    )

    # Tiered progress milestones.  Each `sometimes(True, ...)` fires when
    # ANY timeline observes at least that throughput; the triage report
    # ends up with green entries at 1, 10, 100, 1000 — telling us at-a-
    # glance how far we typically get per invocation.  Names parameterised
    # by threshold (not by per-thread/per-query) so the report breadth
    # stays bounded.
    for threshold in (1, 10, 100, 1000):
        sometimes(
            merged["succeeded"] >= threshold,
            f"workload-replay: succeeded >= {threshold} queries in at least one timeline",
            {
                "threshold": threshold,
                "succeeded": merged["succeeded"],
                "runtime_s": RUNTIME_S,
            },
        )

    # Liveness: at least sometimes, replay actually completes queries.
    # If this never fires, every other assertion below is vacuous (the
    # worker pool might just be sitting on connect-time failures).
    sometimes(
        merged["succeeded"] > 0,
        "workload-replay: captured queries execute against materialized",
        {"succeeded": merged["succeeded"], "sample": len(sample)},
    )
    # Liveness: meaningful throughput per invocation when faults aren't
    # active.  Demoted to `sometimes` because a single 60 s invocation
    # entirely inside a faults-ON window may finish with 0 succeeded.
    sometimes(
        merged["succeeded"] >= NUM_THREADS,
        "workload-replay: each worker thread completed at least one query",
        {"succeeded": merged["succeeded"], "threads": NUM_THREADS},
    )
    # Liveness: the fault-absorption path is exercised at some point.
    # If this never fires across many timelines, either the fault
    # orchestrator isn't injecting faults during replay (regression
    # in cadence) or the demotion patterns are stale (a real fault
    # shape escaped tolerance and looked like an unexpected error).
    sometimes(
        merged["fault_absorbed"] > 0,
        "workload-replay: at least one fault-injected error was absorbed",
        {"fault_absorbed": merged["fault_absorbed"]},
    )

    # Safety: every error we saw was either fault-shape or a known
    # capture-vs-current-build mismatch.  An unexpected error here means
    # the SUT surfaced a SQL error that doesn't fit either bucket — the
    # exact thing Antithesis is looking for.
    always(
        merged["unexpected_error"] == 0
        and merged["session_setup_error"] == 0
        and merged["connect_error"] == 0,
        "workload-replay: no unexpected SQL errors escaped captured-query replay",
        {
            "succeeded": merged["succeeded"],
            "timed_out": merged["timed_out"],
            "fault_absorbed": merged["fault_absorbed"],
            "expected_skipped": merged["expected_skipped"],
            "unexpected_error": merged["unexpected_error"],
            "session_setup_error": merged["session_setup_error"],
            "connect_error": merged["connect_error"],
            "first_unexpected": merged["first_unexpected"],
            "sample": len(sample),
            "threads": NUM_THREADS,
        },
    )

    return (
        1
        if (
            merged["unexpected_error"]
            + merged["session_setup_error"]
            + merged["connect_error"]
        )
        > 0
        else 0
    )


if __name__ == "__main__":
    sys.exit(main())

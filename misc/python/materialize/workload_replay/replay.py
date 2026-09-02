# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Query replay functions for workload replay.
"""

from __future__ import annotations

import concurrent.futures
import datetime
import random
import re
import threading
import time
from typing import Any

import psycopg
from psycopg.sql import SQL, Identifier, Literal

from materialize.mzcompose.composition import Composition

PG_PARAM_RE = re.compile(r"\$(\d+)")

# Per-worker-thread connection reuse. Opening a fresh connection for every query
# (with up to `max_concurrent_queries` threads) floods environmentd with connection
# establishment and causes `ConnectionTimeout`s; reusing one connection per thread
# keeps the count bounded to the pool size.
_thread_local = threading.local()


def _get_connection(c: Composition):
    conn = getattr(_thread_local, "conn", None)
    if conn is None or conn.closed:
        conn = c.sql_connection(user="mz_system", port=6877)
        _thread_local.conn = conn
    return conn


def _reset_connection() -> None:
    """Drop the thread's connection so the next query reconnects (self-heal)."""
    conn = getattr(_thread_local, "conn", None)
    _thread_local.conn = None
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass


def pg_params_to_psycopg(sql: str, params: list[Any]) -> tuple[str, list[Any]]:
    """Convert PostgreSQL $N parameters to psycopg %s format."""
    out_params = []

    def replace(m):
        i = int(m.group(1)) - 1
        # Anonymized parameters carry the '<REDACTED>' placeholder; bind them as
        # NULL (we no longer know the value or its type), mirroring how redacted
        # '<REDACTED>' SQL literals are mapped to NULL below.
        param = params[i]
        out_params.append(None if param == "<REDACTED>" else param)
        return "%s"

    return PG_PARAM_RE.sub(replace, sql.replace("%", "%%")), out_params


def run_query(
    c: Composition,
    query: dict[str, Any],
    stats: dict[str, Any],
    verbose: bool,
    stop_event: threading.Event,
) -> None:
    """Execute a single query against Materialize."""
    sql = query["sql"]
    try:
        conn = _get_connection(c)
        with conn.cursor() as cur:
            cur.execute(
                SQL("SET transaction_isolation = {}").format(
                    Literal(query["transaction_isolation"])
                )
            )
            cur.execute(SQL("SET cluster = {}").format(Literal(query["cluster"])))
            cur.execute(SQL("SET database = {}").format(Literal(query["database"])))
            # Quote each schema as an identifier so names with special characters
            # survive, and skip empty entries (an empty captured search_path would
            # otherwise render as "SET search_path =", a syntax error).
            search_path = [s for s in query["search_path"] if s]
            if search_path:
                cur.execute(
                    SQL("SET search_path = {}").format(
                        SQL(", ").join(Identifier(s) for s in search_path)
                    )
                )

            stats["total"] += 1

            try:
                sql, params = pg_params_to_psycopg(query["sql"], query["params"])
                # TODO: Better replacements for <REDACTED>, but requires parsing the SQL, figuring out the column name, object name, looking up the data type, etc.
                sql = sql.replace("'<REDACTED>'", "NULL")

                start_time = time.time()

                if query["statement_type"] == "subscribe":
                    # cur.stream() blocks on the socket waiting for the next
                    # SUBSCRIBE row, so over quiet data the per-row deadline and
                    # stop_event checks below never run and the worker thread would
                    # hang past shutdown. A watchdog cancels the query to unblock the
                    # read. statement_timeout can't do this: SUBSCRIBE is exempt from
                    # it, and SET LOCAL would evaporate anyway on the autocommit
                    # connection sql_connection() returns.
                    duration = query.get("duration")
                    deadline = start_time + duration if duration else None
                    subscribe_done = threading.Event()

                    def cancel_subscribe() -> None:
                        while not subscribe_done.is_set():
                            if stop_event.is_set():
                                break
                            if deadline is not None and time.time() >= deadline:
                                break
                            subscribe_done.wait(timeout=0.5)
                        if not subscribe_done.is_set():
                            conn.cancel()

                    watchdog = threading.Thread(
                        target=cancel_subscribe, name="subscribe-watchdog"
                    )
                    watchdog.start()
                    row_count = 0
                    try:
                        for row in cur.stream(sql.encode(), params):
                            row_count += 1
                            if stop_event.is_set():
                                break
                            if deadline is not None and time.time() >= deadline:
                                break
                    except psycopg.errors.QueryCanceled:
                        pass
                    finally:
                        subscribe_done.set()
                        watchdog.join()

                    end_time = time.time()
                    stats["timings"].append((sql, end_time - start_time))
                    stats.setdefault("subscribe_rows", 0)

                    if verbose:
                        print(
                            f"Success: {sql} ({end_time - start_time:.2f}s), rows: {row_count}"
                        )

                else:
                    cur.execute(sql.encode(), params)
                    end_time = time.time()
                    stats["timings"].append((sql, end_time - start_time))

                    if verbose:
                        print(f"Success: {sql} (params: {params})")

            except psycopg.Error as e:
                stats["failed"] += 1
                stats["errors"][f"{e.sqlstate}: {e}"].append(sql)

                if query["finished_status"] == "success":
                    if "unknown catalog item" not in str(e):
                        print(f"Failed: {sql} (params: {params})")
                        print(f"{e.sqlstate}: {e}")
                elif verbose:
                    print(f"Failed expectedly: {sql} (params: {params})")
                    print(f"{e.sqlstate}: {e}")
    except Exception as e:
        # Connection-level failure (e.g. connect timeout, or the server dropped
        # the connection). Record it, drop the (possibly broken) thread connection
        # so the next query reconnects, and stay non-fatal: a transient blip must
        # not abort the whole continuous-query run.
        stats["failed"] += 1
        sqlstate = getattr(e, "sqlstate", None)
        stats["errors"][f"{sqlstate}: {e}"].append("<connection>")
        _reset_connection()
        if verbose:
            print(f"Connection failure ({sqlstate}): {e}")


def continuous_queries(
    c: Composition,
    workload: dict[str, Any],
    stop_event: threading.Event,
    factor_queries: float,
    verbose: bool,
    stats: dict[str, int],
    rng: random.Random,
    max_concurrent_queries: bool,
) -> None:
    """Run continuous query replay in a loop."""
    if not workload["queries"]:
        return

    start = workload["queries"][0]["began_at"]
    futures: set[concurrent.futures.Future[None]] = set()

    def submit_query(query: dict[str, Any]) -> None:
        fut = executor.submit(run_query, c, query, stats, verbose, stop_event)
        futures.add(fut)

    try:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_concurrent_queries
        ) as executor:
            i = 0
            while True:
                i += 1
                replay_start = datetime.datetime.now(datetime.timezone.utc)

                for query in workload["queries"]:
                    if stop_event.is_set():
                        return

                    if query["statement_type"] in (
                        # TODO: Requires recreating transactions in which these have to be run
                        "start_transaction",
                        "set_transaction",
                        "commit",
                        "rollback",
                        "fetch",
                        # They will already exist statically, don't create
                        "create_connection",
                        "create_webhook",
                        "create_source",
                        "create_subsource",
                        "create_sink",
                        "create_table_from_source",
                    ):
                        continue

                    offset = (query["began_at"] - start) / factor_queries
                    scheduled = replay_start + offset
                    sleep_seconds = (
                        scheduled - datetime.datetime.now(datetime.timezone.utc)
                    ).total_seconds()

                    if sleep_seconds > 0:
                        stop_event.wait(timeout=sleep_seconds)
                        if stop_event.is_set():
                            return
                    elif sleep_seconds < 0:
                        stats["slow"] += 1
                        if verbose:
                            print(f"Can't keep up: {query}")

                    done = {f for f in futures if f.done()}
                    for f in done:
                        futures.remove(f)
                        f.result()

                    submit_query(query)

    except Exception as e:
        print(f"Failed: {query['sql']}")
        print(e)
        stop_event.set()
        raise

    finally:
        for f in futures:
            f.cancel()
        concurrent.futures.wait(futures)

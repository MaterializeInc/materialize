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
from psycopg.sql import SQL, Literal

from materialize.mzcompose.composition import Composition

PG_PARAM_RE = re.compile(r"\$(\d+)")


def pg_params_to_psycopg(sql: str, params: list[Any]) -> tuple[str, list[Any]]:
    """Convert PostgreSQL $N parameters to psycopg %s format."""
    out_params = []

    def replace(m):
        i = int(m.group(1)) - 1
        out_params.append(params[i])
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
    conn = c.sql_connection(user="mz_system", port=6877)
    with conn.cursor() as cur:
        cur.execute(
            SQL("SET transaction_isolation = {}").format(
                Literal(query["transaction_isolation"])
            )
        )
        cur.execute(SQL("SET cluster = {}").format(Literal(query["cluster"])))
        cur.execute(SQL("SET database = {}").format(Literal(query["database"])))
        cur.execute(f"SET search_path = {','.join(query['search_path'])}".encode())

        stats["total"] += 1

        try:
            sql, params = pg_params_to_psycopg(query["sql"], query["params"])
            # TODO: Better replacements for <REDACTED>, but requires parsing the SQL, figuring out the column name, object name, looking up the data type, etc.
            sql = sql.replace("'<REDACTED'>", "NULL")

            start_time = time.time()

            if query["statement_type"] == "subscribe":
                if query.get("duration"):
                    duration = query["duration"]
                    cur.execute(
                        SQL("SET LOCAL statement_timeout = {}").format(
                            Literal(int(duration * 1000))
                        )
                    )
                    end_deadline = start_time + duration
                row_count = 0
                try:
                    for row in cur.stream(sql.encode(), params):
                        row_count += 1
                        if query.get("duration"):
                            if time.time() >= end_deadline:
                                break
                        if stop_event.is_set():
                            return
                except psycopg.errors.QueryCanceled:
                    pass

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

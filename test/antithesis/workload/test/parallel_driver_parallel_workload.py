#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis-native randomized parallel SQL workload.

This ports the *intent* of `test/parallel-workload/mzcompose.py` into the
existing Antithesis workload model without trying to ship the whole
`materialize.parallel_workload` Python stack inside the workload image.

The driver deliberately shares a small fixed pool of objects across all
invocations and worker threads:
  - one schema
  - four tables
  - four materialized views over those tables

Workers race CREATE/DROP/INSERT/UPDATE/DELETE/SELECT against that pool. The
property is not result correctness; it is that concurrent randomized SQL under
fault injection should not surface *unexpected* query errors. Expected catalog
race/drop errors are counted and ignored, mirroring the philosophy of the
original parallel workload.
"""

from __future__ import annotations

import logging
import os
import random
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

import helper_random
import psycopg
from helper_pg import PGDATABASE, PGHOST, PGPORT, PGUSER, execute_retry

from antithesis.assertions import always, sometimes

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
LOG = logging.getLogger("driver.parallel_workload")

CLUSTER = os.environ.get("MZ_ANTITHESIS_CLUSTER", "antithesis_cluster")
SCHEMA = "antithesis_parallel_workload"

TABLE_COUNT = 4
WORKER_THREADS = 4
RUNTIME_S = 25.0
CONNECT_TIMEOUT_S = 5
MAX_KEY = 31
MAX_VALUE = 1000

EXPECTED_ERROR_SUBSTRINGS = [
    "already exists",
    "does not exist",
    "unknown catalog item",
    "unknown schema",
    "was dropped while executing a statement",
    "another session modified the catalog while this DDL transaction was open",
    "object state changed while transaction was in progress",
    "query could not complete",
    "cached plan must not change result type",
    "the transaction's active cluster has been dropped",
    "concurrent transaction",
]


@dataclass
class WorkerStats:
    successes: int = 0
    reconnects: int = 0
    ignored_errors: int = 0
    actions: Counter[str] = field(default_factory=Counter)
    ignored_by_reason: Counter[str] = field(default_factory=Counter)
    unexpected: dict[str, Any] | None = None


def table_name(idx: int) -> str:
    return f"{SCHEMA}.t{idx}"


def mv_name(idx: int) -> str:
    return f"{SCHEMA}.mv{idx}"


def ensure_shared_objects() -> None:
    execute_retry(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    for idx in range(2):
        execute_retry(
            f"CREATE TABLE IF NOT EXISTS {table_name(idx)} ("
            "worker TEXT NOT NULL, "
            "k BIGINT NOT NULL, "
            "v BIGINT NOT NULL"
            ")"
        )


def connect() -> psycopg.Connection[Any]:
    return psycopg.connect(
        host=PGHOST,
        port=PGPORT,
        user=PGUSER,
        dbname=PGDATABASE,
        connect_timeout=CONNECT_TIMEOUT_S,
        autocommit=True,
    )


def choose_action(rng: random.Random) -> str:
    return rng.choices(
        [
            "create_table",
            "drop_table",
            "insert",
            "update",
            "delete",
            "select_table",
            "create_mv",
            "drop_mv",
            "select_mv",
        ],
        weights=[6, 2, 25, 12, 10, 20, 6, 2, 17],
        k=1,
    )[0]


def execute_action(
    conn: psycopg.Connection[Any], rng: random.Random, worker_name: str, action: str
) -> None:
    idx = rng.randrange(TABLE_COUNT)
    table = table_name(idx)
    mv = mv_name(idx)

    with conn.cursor() as cur:
        if action == "create_table":
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {table} ("
                "worker TEXT NOT NULL, "
                "k BIGINT NOT NULL, "
                "v BIGINT NOT NULL"
                ")"
            )
        elif action == "drop_table":
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        elif action == "insert":
            cur.execute(
                f"INSERT INTO {table} (worker, k, v) VALUES (%s, %s, %s)",
                (
                    worker_name,
                    rng.randint(0, MAX_KEY),
                    rng.randint(0, MAX_VALUE),
                ),
            )
        elif action == "update":
            cur.execute(
                f"UPDATE {table} SET v = v + 1 WHERE k = %s",
                (rng.randint(0, MAX_KEY),),
            )
        elif action == "delete":
            cur.execute(
                f"DELETE FROM {table} WHERE k = %s",
                (rng.randint(0, MAX_KEY),),
            )
        elif action == "select_table":
            cur.execute(
                f"SELECT count(*)::bigint, min(v)::bigint, max(v)::bigint FROM {table}"
            )
            cur.fetchall()
        elif action == "create_mv":
            cur.execute(
                f"CREATE MATERIALIZED VIEW IF NOT EXISTS {mv} "
                f"IN CLUSTER {CLUSTER} AS "
                f"SELECT worker, count(*)::bigint AS c, sum(v)::bigint AS s "
                f"FROM {table} GROUP BY worker"
            )
        elif action == "drop_mv":
            cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {mv}")
        elif action == "select_mv":
            cur.execute(
                f"SELECT count(*)::bigint, sum(c)::bigint, sum(s)::bigint FROM {mv}"
            )
            cur.fetchall()
        else:
            raise ValueError(f"unknown action {action}")


def expected_error_reason(exc: BaseException) -> str | None:
    msg = str(exc)
    for candidate in EXPECTED_ERROR_SUBSTRINGS:
        if candidate in msg:
            return candidate
    return None


def is_connection_error(exc: BaseException) -> bool:
    return isinstance(exc, (psycopg.OperationalError, psycopg.InterfaceError))


def run_worker(
    worker_id: int,
    seed: int,
    deadline: float,
    stop: threading.Event,
    stats: WorkerStats,
) -> None:
    rng = random.Random(seed)
    worker_name = f"pw{worker_id}"
    conn: psycopg.Connection[Any] | None = None

    try:
        while time.monotonic() < deadline and not stop.is_set():
            if conn is None or conn.closed:
                try:
                    conn = connect()
                except Exception as exc:  # noqa: BLE001
                    if not is_connection_error(exc):
                        stats.unexpected = {
                            "worker": worker_name,
                            "action": "connect",
                            "error": str(exc),
                        }
                        stop.set()
                        return
                    stats.reconnects += 1
                    time.sleep(rng.uniform(0.05, 0.2))
                    continue

            action = choose_action(rng)
            try:
                execute_action(conn, rng, worker_name, action)
                stats.successes += 1
                stats.actions[action] += 1
            except Exception as exc:  # noqa: BLE001
                if is_connection_error(exc):
                    stats.reconnects += 1
                    try:
                        conn.close()
                    except Exception:  # noqa: BLE001
                        pass
                    conn = None
                    continue

                reason = expected_error_reason(exc)
                if reason is not None:
                    stats.ignored_errors += 1
                    stats.ignored_by_reason[reason] += 1
                    stats.actions[action] += 1
                    continue

                stats.unexpected = {
                    "worker": worker_name,
                    "action": action,
                    "error": str(exc),
                }
                LOG.exception("unexpected parallel workload error")
                stop.set()
                return

            time.sleep(rng.uniform(0.005, 0.05))
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass


def main() -> int:
    ensure_shared_objects()

    stop = threading.Event()
    deadline = time.monotonic() + RUNTIME_S
    seeds = [helper_random.random_u64() for _ in range(WORKER_THREADS)]
    stats = [WorkerStats() for _ in range(WORKER_THREADS)]
    threads = [
        threading.Thread(
            name=f"parallel-workload-{idx}",
            target=run_worker,
            args=(idx, seeds[idx], deadline, stop, stats[idx]),
        )
        for idx in range(WORKER_THREADS)
    ]

    LOG.info("parallel workload starting; schema=%s threads=%d", SCHEMA, WORKER_THREADS)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    total_successes = sum(worker.successes for worker in stats)
    total_reconnects = sum(worker.reconnects for worker in stats)
    total_ignored = sum(worker.ignored_errors for worker in stats)
    action_counts = Counter[str]()
    ignored_by_reason = Counter[str]()
    unexpected = next((worker.unexpected for worker in stats if worker.unexpected), None)
    for worker in stats:
        action_counts.update(worker.actions)
        ignored_by_reason.update(worker.ignored_by_reason)

    sometimes(
        total_successes >= WORKER_THREADS * 5,
        "parallel workload: randomized concurrent SQL executed successfully",
        {
            "successes": total_successes,
            "threads": WORKER_THREADS,
            "actions": dict(action_counts),
            "reconnects": total_reconnects,
        },
    )
    sometimes(
        action_counts["create_table"]
        + action_counts["drop_table"]
        + action_counts["create_mv"]
        + action_counts["drop_mv"]
        > 0,
        "parallel workload: DDL actions were exercised",
        {
            "create_table": action_counts["create_table"],
            "drop_table": action_counts["drop_table"],
            "create_mv": action_counts["create_mv"],
            "drop_mv": action_counts["drop_mv"],
        },
    )
    sometimes(
        total_ignored > 0,
        "parallel workload: expected concurrent-catalog races were observed",
        {
            "ignored_errors": total_ignored,
            "ignored_by_reason": dict(ignored_by_reason),
        },
    )
    always(
        unexpected is None,
        "parallel workload: no unexpected SQL errors escaped the randomized stress driver",
        {
            "unexpected": unexpected,
            "successes": total_successes,
            "ignored_errors": total_ignored,
            "reconnects": total_reconnects,
            "actions": dict(action_counts),
        },
    )

    LOG.info(
        "parallel workload done; successes=%d ignored=%d reconnects=%d unexpected=%s",
        total_successes,
        total_ignored,
        total_reconnects,
        unexpected,
    )
    return 1 if unexpected is not None else 0


if __name__ == "__main__":
    _ = (PGHOST, PGPORT, PGUSER, PGDATABASE, os)
    sys.exit(main())

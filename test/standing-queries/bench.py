#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Benchmark standing query execution with configurable concurrency.

Usage:
    python bench.py [--connections N] [--duration SECS] [--setup] [--port PORT] [--query QUERY]

Examples:
    # Setup + benchmark with defaults (64 connections, 60s)
    python bench.py --setup

    # Just benchmark (assumes setup already done)
    python bench.py --connections 128 --duration 30

    # Benchmark index SELECT for comparison
    python bench.py --query "SELECT id, customer_id, amount FROM orders WHERE customer_id = 42"
"""

import argparse
import statistics
import threading
import time

import psycopg
from psycopg import sql

NUM_ROWS = 100_000


def setup(port: int) -> None:
    conn = psycopg.connect(
        host="127.0.0.1", port=port, user="materialize", dbname="materialize"
    )
    conn.autocommit = True
    cur = conn.cursor()

    print("Setting up...")
    sys_conn = psycopg.connect(
        host="127.0.0.1", port=6877, user="mz_system", dbname="materialize"
    )
    sys_conn.autocommit = True
    sys_cur = sys_conn.cursor()
    sys_cur.execute(sql.SQL("ALTER SYSTEM SET max_result_size = '10GB'"))
    sys_cur.execute(sql.SQL("ALTER SYSTEM SET max_connections = 65536"))
    sys_cur.execute(sql.SQL("ALTER SYSTEM SET enable_standing_queries = true"))
    sys_conn.close()

    cur.execute(sql.SQL("DROP STANDING QUERY IF EXISTS orders_by_customer"))
    cur.execute(sql.SQL("DROP STANDING QUERY IF EXISTS order_by_id"))
    cur.execute(sql.SQL("DROP TABLE IF EXISTS orders CASCADE"))
    cur.execute(sql.SQL("CREATE TABLE orders (id INT, customer_id INT, amount INT)"))
    cur.execute(
        sql.SQL(
            "INSERT INTO orders SELECT g, g % 100, g * 10 FROM generate_series(1, {}) AS g"
        ).format(sql.Literal(NUM_ROWS))
    )
    cur.execute(sql.SQL("CREATE INDEX orders_by_customer_idx ON orders (customer_id)"))
    cur.execute(sql.SQL("CREATE INDEX orders_by_id_idx ON orders (id)"))
    cur.execute(
        sql.SQL(
            "CREATE STANDING QUERY orders_by_customer (cid INT) "
            "AS SELECT id, customer_id, amount FROM orders WHERE customer_id = cid"
        )
    )
    cur.execute(
        sql.SQL(
            "CREATE STANDING QUERY order_by_id (oid INT) "
            "AS SELECT id, customer_id, amount FROM orders WHERE id = oid"
        )
    )
    conn.close()
    print("Setup complete.")


def worker(
    port: int,
    query: str,
    duration: float,
    latencies: list[float],
    stop_event: threading.Event,
) -> None:
    conn = psycopg.connect(
        host="127.0.0.1", port=port, user="materialize", dbname="materialize"
    )
    conn.autocommit = True
    cur = conn.cursor()

    local_latencies: list[float] = []
    while not stop_event.is_set():
        start = time.monotonic()
        cur.execute(sql.SQL(query))  # type: ignore[arg-type]
        cur.fetchall()
        elapsed = time.monotonic() - start
        local_latencies.append(elapsed)

    conn.close()
    latencies.extend(local_latencies)


def run_benchmark(port: int, query: str, connections: int, duration: float) -> None:
    print(f"Benchmarking: {query}")
    print(f"  connections={connections}, duration={duration}s")

    stop_event = threading.Event()
    all_latencies: list[list[float]] = [[] for _ in range(connections)]
    threads: list[threading.Thread] = []

    for i in range(connections):
        t = threading.Thread(
            target=worker,
            args=(port, query, duration, all_latencies[i], stop_event),
            daemon=True,
        )
        threads.append(t)

    start_time = time.monotonic()
    for t in threads:
        t.start()

    time.sleep(duration)
    stop_event.set()

    for t in threads:
        t.join(timeout=30)

    wall_time = time.monotonic() - start_time

    latencies = []
    for lat_list in all_latencies:
        latencies.extend(lat_list)

    if not latencies:
        print("  No queries completed!")
        return

    latencies.sort()
    total = len(latencies)
    qps = total / wall_time

    print("\n  Results:")
    print(f"    Total queries: {total}")
    print(f"    Wall time:     {wall_time:.1f}s")
    print(f"    QPS:           {qps:.1f}")
    print("    Latency (ms):")
    print(f"      mean:   {statistics.mean(latencies) * 1000:.2f}")
    print(f"      median: {latencies[total // 2] * 1000:.2f}")
    print(f"      p90:    {latencies[int(total * 0.90)] * 1000:.2f}")
    print(f"      p99:    {latencies[int(total * 0.99)] * 1000:.2f}")
    print(f"      p999:   {latencies[int(total * 0.999)] * 1000:.2f}")
    print(f"      max:    {latencies[-1] * 1000:.2f}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark standing queries")
    parser.add_argument(
        "--connections",
        type=int,
        default=64,
        help="Number of concurrent connections (default: 64)",
    )
    parser.add_argument(
        "--duration", type=float, default=60, help="Duration in seconds (default: 60)"
    )
    parser.add_argument(
        "--port", type=int, default=6875, help="Materialize port (default: 6875)"
    )
    parser.add_argument(
        "--setup",
        action="store_true",
        help="Create table, indexes, and standing queries first",
    )
    parser.add_argument(
        "--query",
        type=str,
        default="EXECUTE STANDING QUERY orders_by_customer (42)",
        help="Query to benchmark",
    )
    args = parser.parse_args()

    if args.setup:
        setup(args.port)

    run_benchmark(args.port, args.query, args.connections, args.duration)


if __name__ == "__main__":
    main()

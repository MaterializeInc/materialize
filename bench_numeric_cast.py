#!/usr/bin/env python3
"""Benchmark for numeric-to-float casting optimization.

Tests the performance of casting NUMERIC values to FLOAT4/FLOAT8
by running queries against a Materialize instance.

Usage:
    # First start Materialize:
    #   bin/environmentd --optimized --reset
    # Then login as mz_system and enable frontend peek sequencing:
    #   psql -p 6877 -U mz_system -d materialize -c "alter system set enable_frontend_peek_sequencing=true;"
    # Then run:
    python3 bench_numeric_cast.py
"""

import time
import psycopg2


def run_benchmark():
    # Connect as mz_system to the internal port
    conn_system = psycopg2.connect(
        host="localhost", port=6877, user="mz_system", dbname="materialize"
    )
    conn_system.autocommit = True
    cur_system = conn_system.cursor()

    # Enable frontend peek sequencing
    cur_system.execute("ALTER SYSTEM SET enable_frontend_peek_sequencing = true;")

    # Connect as regular user
    conn = psycopg2.connect(
        host="localhost", port=6875, user="materialize", dbname="materialize"
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Setup: create a table with numeric values
    cur.execute("DROP TABLE IF EXISTS bench_numeric CASCADE;")
    cur.execute("""
        CREATE TABLE bench_numeric (
            id INT,
            val NUMERIC
        );
    """)

    # Insert test data - 10000 rows with various numeric values
    print("Inserting test data...")
    for batch in range(100):
        values = ", ".join(
            f"({batch * 100 + i}, {i * 1.23456789 + batch * 0.001})"
            for i in range(100)
        )
        cur.execute(f"INSERT INTO bench_numeric VALUES {values};")

    print(f"Inserted 10000 rows")

    # Create an index for fast path
    cur.execute("CREATE DEFAULT INDEX ON bench_numeric;")
    time.sleep(2)  # Wait for index to hydrate

    # Warmup
    print("\nWarming up...")
    for _ in range(5):
        cur.execute("SELECT val::float8 FROM bench_numeric;")
        cur.fetchall()

    # Benchmark: NUMERIC -> FLOAT8 cast
    print("\nBenchmarking NUMERIC -> FLOAT8 cast (10000 rows)...")
    times = []
    for i in range(50):
        start = time.perf_counter()
        cur.execute("SELECT val::float8 FROM bench_numeric;")
        cur.fetchall()
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    avg_time = sum(times) / len(times)
    min_time = min(times)
    p50 = sorted(times)[len(times) // 2]
    p99 = sorted(times)[int(len(times) * 0.99)]
    print(f"  avg={avg_time*1000:.2f}ms  min={min_time*1000:.2f}ms  p50={p50*1000:.2f}ms  p99={p99*1000:.2f}ms")

    # Benchmark: NUMERIC -> FLOAT4 cast
    print("\nBenchmarking NUMERIC -> FLOAT4 cast (10000 rows)...")
    times = []
    for i in range(50):
        start = time.perf_counter()
        cur.execute("SELECT val::float4 FROM bench_numeric;")
        cur.fetchall()
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    avg_time = sum(times) / len(times)
    min_time = min(times)
    p50 = sorted(times)[len(times) // 2]
    p99 = sorted(times)[int(len(times) * 0.99)]
    print(f"  avg={avg_time*1000:.2f}ms  min={min_time*1000:.2f}ms  p50={p50*1000:.2f}ms  p99={p99*1000:.2f}ms")

    # Benchmark: Arithmetic that uses numeric-to-float internally
    print("\nBenchmarking SUM(val::float8) aggregation...")
    times = []
    for i in range(50):
        start = time.perf_counter()
        cur.execute("SELECT SUM(val::float8) FROM bench_numeric;")
        cur.fetchall()
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    avg_time = sum(times) / len(times)
    min_time = min(times)
    p50 = sorted(times)[len(times) // 2]
    p99 = sorted(times)[int(len(times) * 0.99)]
    print(f"  avg={avg_time*1000:.2f}ms  min={min_time*1000:.2f}ms  p50={p50*1000:.2f}ms  p99={p99*1000:.2f}ms")

    # Benchmark: Multiple casts per row
    print("\nBenchmarking val::float8 + val::float4 (double cast per row)...")
    times = []
    for i in range(50):
        start = time.perf_counter()
        cur.execute("SELECT val::float8, val::float4 FROM bench_numeric;")
        cur.fetchall()
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    avg_time = sum(times) / len(times)
    min_time = min(times)
    p50 = sorted(times)[len(times) // 2]
    p99 = sorted(times)[int(len(times) * 0.99)]
    print(f"  avg={avg_time*1000:.2f}ms  min={min_time*1000:.2f}ms  p50={p50*1000:.2f}ms  p99={p99*1000:.2f}ms")

    # Cleanup
    cur.execute("DROP TABLE IF EXISTS bench_numeric CASCADE;")
    cur.close()
    conn.close()
    cur_system.close()
    conn_system.close()


if __name__ == "__main__":
    run_benchmark()

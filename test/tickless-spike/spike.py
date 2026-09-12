# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Measure source freshness under event-driven remap bindings."""

import argparse
import threading
import time
from dataclasses import dataclass, field

import psycopg

PG_DSN = "host=localhost port=5434 user=postgres password=postgres dbname=postgres"
MZ_DSN = "host=localhost port=6875 user=materialize dbname=materialize"
MZ_SYSTEM_DSN = "host=localhost port=6877 user=mz_system dbname=materialize"


@dataclass
class Mode:
    name: str
    event_driven: bool
    lead_ms: int
    keepalive_ms: int


MODES = [
    Mode("baseline", False, 0, 1000),
    Mode("baseline+keepalive250", False, 0, 250),
    Mode("event-driven lead0", True, 0, 250),
    Mode("event-driven lead400", True, 400, 250),
]


@dataclass
class Samples:
    staleness_ms: list[float] = field(default_factory=list)
    latency_ms: list[float] = field(default_factory=list)
    binding_timestamps: set[int] = field(default_factory=set)
    errors: int = 0


def pg_setup() -> None:
    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        conn.execute("DROP PUBLICATION IF EXISTS mz_pub")
        conn.execute("DROP TABLE IF EXISTS t")
        conn.execute(
            "CREATE TABLE t (id bigserial PRIMARY KEY, ts timestamptz NOT NULL DEFAULT clock_timestamp())"
        )
        conn.execute("ALTER TABLE t REPLICA IDENTITY FULL")
        conn.execute("CREATE PUBLICATION mz_pub FOR TABLE t")


def mz_system(sql: str) -> None:
    with psycopg.connect(MZ_SYSTEM_DSN, autocommit=True) as conn:
        # Encoded to bytes so the dynamically built command satisfies
        # psycopg's LiteralString-typed query parameter.
        conn.execute(sql.encode())


def mz_setup(mode: Mode) -> None:
    mz_system("ALTER SYSTEM SET default_timestamp_interval = '1s'")
    mz_system(
        f"ALTER SYSTEM SET storage_event_driven_bindings = {'true' if mode.event_driven else 'false'}"
    )
    mz_system(f"ALTER SYSTEM SET storage_binding_lead = '{mode.lead_ms}ms'")
    with psycopg.connect(MZ_DSN, autocommit=True) as conn:
        conn.execute("DROP SOURCE IF EXISTS pg_src CASCADE")
        conn.execute("DROP CONNECTION IF EXISTS pg CASCADE")
        conn.execute("DROP SECRET IF EXISTS pgpass")
        conn.execute("CREATE SECRET pgpass AS 'postgres'")
        conn.execute(
            "CREATE CONNECTION pg TO POSTGRES (HOST 'localhost', PORT 5434, USER postgres, PASSWORD SECRET pgpass, DATABASE postgres)"
        )
        conn.execute(
            "CREATE SOURCE pg_src FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_pub') "
            "EXPOSE PROGRESS AS pg_src_progress"
        )
        conn.execute("CREATE TABLE t FROM SOURCE pg_src (REFERENCE t)")
        deadline = time.monotonic() + 60
        last_status = None
        while time.monotonic() < deadline:
            row = conn.execute(
                "SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'pg_src'"
            ).fetchone()
            last_status = row[0] if row is not None else None
            if last_status == "running":
                break
            time.sleep(0.5)
        else:
            raise RuntimeError(
                f"pg_src did not reach running, last status: {last_status}"
            )
        last_error = None
        while time.monotonic() < deadline:
            try:
                conn.execute("SELECT count(*) FROM t").fetchone()
                break
            except psycopg.Error as e:
                last_error = str(e)
                time.sleep(0.5)
        else:
            raise RuntimeError(f"table t not queryable, last error: {last_error}")
    # Set after CREATE SOURCE so the source keeps a 1s TIMESTAMP INTERVAL and
    # only the coordinator keepalive speeds up.
    mz_system(f"ALTER SYSTEM SET default_timestamp_interval = '{mode.keepalive_ms}ms'")
    time.sleep(3)


def writer(stop: threading.Event, rate_hz: float) -> None:
    period = 1.0 / rate_hz
    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        while not stop.is_set():
            conn.execute("INSERT INTO t DEFAULT VALUES")
            time.sleep(period)


def reader(stop: threading.Event, samples: Samples) -> None:
    with psycopg.connect(MZ_DSN, autocommit=True) as conn:
        conn.execute("SET transaction_isolation = 'strict serializable'")
        while not stop.is_set():
            t0 = time.time()
            try:
                row = conn.execute("SELECT max(ts) FROM t").fetchone()
            except psycopg.Error:
                samples.errors += 1
                time.sleep(0.1)
                continue
            t1 = time.time()
            samples.latency_ms.append((t1 - t0) * 1000.0)
            if row is not None and row[0] is not None:
                samples.staleness_ms.append((t1 - row[0].timestamp()) * 1000.0)
            time.sleep(0.05)


def binding_counter(stop: threading.Event, samples: Samples) -> None:
    with psycopg.connect(MZ_DSN, autocommit=False) as conn:
        cur = conn.cursor()
        cur.execute("DECLARE c CURSOR FOR SUBSCRIBE (SELECT * FROM pg_src_progress)")
        while not stop.is_set():
            rows = cur.execute("FETCH ALL c WITH (timeout = '1s')").fetchall()
            for row in rows:
                samples.binding_timestamps.add(int(row[0]))


def pct(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    values = sorted(values)
    idx = min(len(values) - 1, int(round(p * (len(values) - 1))))
    return values[idx]


def run_mode(
    mode: Mode, duration_s: float, rate_hz: float
) -> tuple[Mode, Samples, float]:
    mz_setup(mode)
    samples = Samples()
    stop = threading.Event()
    threads = [
        threading.Thread(target=writer, args=(stop, rate_hz), daemon=True),
        threading.Thread(target=reader, args=(stop, samples), daemon=True),
        threading.Thread(target=binding_counter, args=(stop, samples), daemon=True),
    ]
    for th in threads:
        th.start()
    t_start = time.time()
    time.sleep(duration_s)
    stop.set()
    for th in threads:
        th.join(timeout=5)
    return mode, samples, time.time() - t_start


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=30.0)
    parser.add_argument("--rate", type=float, default=20.0)
    parser.add_argument("--modes", nargs="*", default=[m.name for m in MODES])
    args = parser.parse_args()

    pg_setup()
    results = []
    try:
        for mode in MODES:
            if mode.name not in args.modes:
                continue
            results.append(run_mode(mode, args.duration, args.rate))
    finally:
        mz_system("ALTER SYSTEM RESET default_timestamp_interval")
        mz_system("ALTER SYSTEM RESET storage_event_driven_bindings")
        mz_system("ALTER SYSTEM RESET storage_binding_lead")

    print(
        "| mode | staleness p50 ms | staleness p95 ms | read p50 ms | read p95 ms | bindings/s | reads | errors |"
    )
    print("|---|---|---|---|---|---|---|---|")
    for mode, s, elapsed in results:
        print(
            f"| {mode.name} | {pct(s.staleness_ms, 0.5):.0f} | {pct(s.staleness_ms, 0.95):.0f} "
            f"| {pct(s.latency_ms, 0.5):.1f} | {pct(s.latency_ms, 0.95):.1f} "
            f"| {len(s.binding_timestamps) / elapsed:.2f} | {len(s.latency_ms)} | {s.errors} |"
        )


if __name__ == "__main__":
    main()

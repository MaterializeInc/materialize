# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Measures the speedup of libpq/psycopg pipeline mode against Materialize and,
for reference, against Postgres.

Pipeline mode batches extended-protocol messages: the client sends every
Parse/Bind/Execute before reading any response, instead of waiting for the
result of each query before sending the next. Its only effect is removing the
per-query network round-trip.

Two workloads isolate different costs:

- `SELECT 1` is a constant. It does almost no server-side work (and, being a
  constant, skips timestamp selection entirely), so the measured difference
  between sequential and pipeline is essentially the round-trip.
- The indexed point lookup `SELECT v FROM t WHERE k = $1` is a real peek. Under
  strict serializability (Materialize's default) every such query pays a
  timestamp-oracle round-trip, which pipeline mode cannot overlap today because
  the pgwire connection processes statements strictly serially. This is the
  case that motivates server-side pipelining.

The benchmark reports, per workload and system, the sequential (baseline)
throughput, the pipelined throughput, and the resulting speedup, then compares
the two systems.
"""

import statistics
import time
from collections.abc import Callable, Sequence

import psycopg

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.postgres import Postgres
from materialize.util import PgConnInfo

SERVICES = [
    Materialized(),
    Postgres(),
]

N_ROWS = 10000

# Identical DDL on both systems: a keyed table with a secondary index, so
# `WHERE k = $1` is an indexed point lookup rather than a scan. INSERT runs
# before CREATE INDEX so the index is built with data already present.
SETUP = [
    b"DROP TABLE IF EXISTS pipeline_bench CASCADE",
    b"CREATE TABLE pipeline_bench (k int4, v int4)",
    f"INSERT INTO pipeline_bench SELECT g, g FROM generate_series(0, {N_ROWS - 1}) g".encode(),
    b"CREATE INDEX pipeline_bench_k_idx ON pipeline_bench (k)",
]

# (label, query, params-factory). The factory returns the psycopg params tuple
# for iteration i, or None for a parameterless query. Keys vary across the
# table to avoid single-entry caching effects.
Workload = tuple[str, bytes, Callable[[int], tuple] | None]
WORKLOADS: list[Workload] = [
    ("SELECT 1 (constant)", b"SELECT 1", None),
    (
        "point lookup (indexed)",
        b"SELECT v FROM pipeline_bench WHERE k = %s",
        lambda i: (i % N_ROWS,),
    ),
]


def run_setup(conn: psycopg.Connection) -> None:
    cur = conn.cursor()
    for stmt in SETUP:
        cur.execute(stmt)


def run_sequential(
    conn: psycopg.Connection, query: bytes, params: Sequence[tuple | None]
) -> float:
    """One request/response round-trip per query (extended protocol + Sync)."""
    cur = conn.cursor()
    start = time.perf_counter()
    for p in params:
        cur.execute(query, p)
        cur.fetchall()
    return time.perf_counter() - start


def run_pipeline(
    conn: psycopg.Connection, query: bytes, params: Sequence[tuple | None]
) -> float:
    """All queries are sent before any response is read.

    Uses a distinct cursor per query so every result set is retained. Exiting
    the pipeline block flushes and syncs, populating each cursor.
    """
    curs = [conn.cursor() for _ in params]
    start = time.perf_counter()
    with conn.pipeline():
        for cur, p in zip(curs, params):
            cur.execute(query, p)
    for cur in curs:
        cur.fetchall()
    return time.perf_counter() - start


# Targets, in report order. The bool is the value the
# `enable_pipelined_peek_shared_timestamp` dyncfg is set to (via mz_system)
# before that target's Materialize connection is opened; None means Postgres.
# The flag is cached per connection at startup, so it must be set before connect.
TARGETS: list[tuple[str, bool | None]] = [
    ("Materialize", False),
    ("Materialize+sharedTS", True),
    ("Postgres", None),
]


def print_workload(
    label: str, n: int, samples: dict[tuple[str, str], list[float]]
) -> None:
    """`samples` maps (target, mode) to per-trial total-time samples."""
    print(f"\n### {label}")
    header = (
        f"{'Target':<22} {'Mode':<11} {'median us':>10} {'best us':>10} "
        f"{'median QPS':>12} {'best QPS':>12}"
    )
    print(header)
    print("-" * len(header))
    for target, _ in TARGETS:
        for mode in ("sequential", "pipeline"):
            s = samples[(target, mode)]
            med, best = statistics.median(s) / n, min(s) / n
            print(
                f"{target:<22} {mode:<11} {med * 1e6:>10.1f} {best * 1e6:>10.1f} "
                f"{1 / med:>12,.0f} {1 / best:>12,.0f}"
            )
    # #2 effect on the best (least-contended) pipelined sample, the cleanest
    # signal under load. The sequential rows are a control: they should match
    # across off/on, since sequential mode opens no burst.
    base = min(samples[("Materialize", "pipeline")])
    shared = min(samples[("Materialize+sharedTS", "pipeline")])
    pg = min(samples[("Postgres", "pipeline")])
    print(
        f"  #2 shared-ts (best-case pipelined): {n / base:,.0f} -> {n / shared:,.0f} QPS"
        f" ({base / shared:.2f}x)   |   vs Postgres: {(n / shared) / (n / pg):.2f}x"
    )


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--queries", type=int, default=2000, help="Queries per trial.")
    parser.add_argument(
        "--trials",
        type=int,
        default=9,
        help="Interleaved timed trials per (target, mode); median and best reported.",
    )
    args = parser.parse_args()
    n, trials = args.queries, args.trials

    c.up("materialized", "postgres")

    mz_system = PgConnInfo(
        user="mz_system",
        database="materialize",
        host="127.0.0.1",
        port=c.port("materialized", 6877),
        autocommit=True,
    )

    def set_shared_ts(enabled: bool) -> None:
        conn = mz_system.connect()
        try:
            conn.cursor().execute(
                b"ALTER SYSTEM SET enable_pipelined_peek_shared_timestamp = "
                + (b"true" if enabled else b"false")
            )
        finally:
            conn.close()

    def mz_conn() -> PgConnInfo:
        return PgConnInfo(
            user="materialize",
            database="materialize",
            host="127.0.0.1",
            port=c.default_port("materialized"),
            autocommit=True,
        )

    pg_conn = PgConnInfo(
        user="postgres",
        password="postgres",
        database="postgres",
        host="127.0.0.1",
        port=c.default_port("postgres"),
        autocommit=True,
    )

    # Open one connection per target. The dyncfg is cached at connect, so set it
    # (system-wide, via mz_system) right before opening each Materialize
    # connection. Keeping all connections open lets us interleave trials so
    # host-load drift hits every target roughly equally within a trial.
    conns: dict[str, psycopg.Connection] = {}
    for label, dyncfg in TARGETS:
        if dyncfg is None:
            conns[label] = pg_conn.connect()
        else:
            set_shared_ts(dyncfg)
            conns[label] = mz_conn().connect()

    try:
        # One table per backend; the two Materialize connections share a DB.
        run_setup(conns["Materialize"])
        run_setup(conns["Postgres"])

        print(
            f"\n=== Pipeline-mode benchmark ({n} queries/trial,"
            f" {trials} interleaved trials) ==="
        )
        for label, query, param_fn in WORKLOADS:
            params: list[tuple | None] = [
                param_fn(i) if param_fn else None for i in range(n)
            ]
            # Warm every target (connection, server caches, index hydration).
            for conn in conns.values():
                run_sequential(conn, query, params)
                run_pipeline(conn, query, params)

            samples: dict[tuple[str, str], list[float]] = {
                (target, mode): []
                for target, _ in TARGETS
                for mode in ("sequential", "pipeline")
            }
            for _ in range(trials):
                for target, _ in TARGETS:
                    samples[(target, "sequential")].append(
                        run_sequential(conns[target], query, params)
                    )
                for target, _ in TARGETS:
                    samples[(target, "pipeline")].append(
                        run_pipeline(conns[target], query, params)
                    )
            print_workload(label, n, samples)
    finally:
        for conn in conns.values():
            conn.close()
        set_shared_ts(False)

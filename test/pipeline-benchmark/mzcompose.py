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

Pipeline mode batches extended-protocol messages so a client sends every
Parse/Bind/Execute before reading any response, removing the per-query network
round-trip. Two workloads isolate different costs:

- `SELECT 1`, a constant, where server work is near-zero (and, being a constant,
  it skips timestamp selection) so the round-trip dominates.
- an indexed point lookup `SELECT v FROM t WHERE k = $1`, a real peek that under
  strict serializability pays a timestamp-oracle round-trip and a compute-peek
  round-trip per query.

For Materialize it also A/Bs two server-side pipelining optimizations, toggled
via mz_system before connecting:

- `enable_pipelined_peek_shared_timestamp` (+sharedTS): one oracle read_ts per
  pipelined burst.
- `enable_pipelined_peek_overlap` (+overlap): issue every peek in the burst
  before draining any result, overlapping the compute-result awaits.

On loopback the network round-trip is tiny, which understates pipeline mode.
`--latency-ms N` routes the connections through Toxiproxy with ~N ms of added
round-trip latency, which is closer to a real deployment.
"""

import json
import statistics
import time
import urllib.request
from collections.abc import Callable, Sequence

import psycopg

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.toxiproxy import Toxiproxy
from materialize.util import PgConnInfo

# Toxiproxy needs to publish the proxy listen ports (not just its API port) so
# the benchmark, which runs on the host, can connect through them.
_TOXIPROXY = Toxiproxy()
_TOXIPROXY.config["ports"] = [8474, 6875, 5432]

SERVICES = [
    Materialized(),
    Postgres(),
    _TOXIPROXY,
]

N_ROWS = 10000

# Identical DDL on both systems: a keyed table with a secondary index, so
# `WHERE k = $1` is an indexed point lookup rather than a scan. INSERT runs
# before CREATE INDEX so the index is built with data already present. `v == k`,
# which the correctness check relies on.
SETUP = [
    b"DROP TABLE IF EXISTS pipeline_bench CASCADE",
    b"CREATE TABLE pipeline_bench (k int4, v int4)",
    f"INSERT INTO pipeline_bench SELECT g, g FROM generate_series(0, {N_ROWS - 1}) g".encode(),
    b"CREATE INDEX pipeline_bench_k_idx ON pipeline_bench (k)",
]

POINT_LOOKUP = b"SELECT v FROM pipeline_bench WHERE k = %s"

# (label, query, params-factory). The factory returns the psycopg params tuple
# for iteration i, or None for a parameterless query. Keys vary across the
# table to avoid single-entry caching effects.
Workload = tuple[str, bytes, Callable[[int], tuple] | None]
WORKLOADS: list[Workload] = [
    ("SELECT 1 (constant)", b"SELECT 1", None),
    ("point lookup (indexed)", POINT_LOOKUP, lambda i: (i % N_ROWS,)),
]

# Targets, in report order. `(shared_ts, overlap)` are the dyncfg values set (via
# mz_system) before the target's Materialize connection is opened; the flags are
# cached per connection at startup, so they must be set before connect. Postgres
# is marked with `None`.
Target = tuple[str, tuple[bool, bool] | None]
TARGETS: list[Target] = [
    ("Materialize", (False, False)),
    ("Materialize+sharedTS", (True, False)),
    ("Materialize+overlap", (True, True)),
    ("Postgres", None),
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


def check_pipeline(conn: psycopg.Connection) -> None:
    """Verify pipelined point lookups return the right rows.

    Guards the server-side pipelining paths (+overlap defers and reorders wire
    responses): a protocol bug there would surface as wrong or missing rows.
    """
    keys = list(range(64))
    curs = [conn.cursor() for _ in keys]
    with conn.pipeline():
        for cur, k in zip(curs, keys):
            cur.execute(POINT_LOOKUP, (k,))
    for cur, k in zip(curs, keys):
        rows = cur.fetchall()
        assert rows == [(k,)], f"pipeline correctness: k={k} returned {rows}"


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
    # Effect of each optimization on the best (least-contended) pipelined sample,
    # relative to the Materialize baseline. Sequential rows are a control: they
    # should match across targets, since sequential mode opens no burst.
    base = min(samples[("Materialize", "pipeline")])
    for target, cfg in TARGETS:
        if cfg is None or target == "Materialize":
            continue
        this = min(samples[(target, "pipeline")])
        print(
            f"  {target} best-case pipelined: {n / base:,.0f} -> {n / this:,.0f} QPS"
            f" ({base / this:.2f}x vs baseline)"
        )
    pg = min(samples[("Postgres", "pipeline")])
    print(f"  Postgres best-case pipelined: {n / pg:,.0f} QPS")


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--queries", type=int, default=2000, help="Queries per trial.")
    parser.add_argument(
        "--trials",
        type=int,
        default=9,
        help="Interleaved timed trials per (target, mode); median and best reported.",
    )
    parser.add_argument(
        "--latency-ms",
        type=int,
        default=0,
        help="If >0, route connections through Toxiproxy with ~N ms added round-trip latency.",
    )
    args = parser.parse_args()
    n, trials, latency = args.queries, args.trials, args.latency_ms

    services = ["materialized", "postgres"]
    if latency > 0:
        services.append("toxiproxy")
    c.up(*services)

    mz_host, mz_port = "127.0.0.1", c.default_port("materialized")
    pg_host, pg_port = "127.0.0.1", c.default_port("postgres")
    if latency > 0:
        configure_toxiproxy(c, latency)
        mz_port = c.port("toxiproxy", 6875)
        pg_port = c.port("toxiproxy", 5432)

    # mz_system is used only to set dyncfgs; it stays on the direct connection.
    mz_system = PgConnInfo(
        user="mz_system",
        database="materialize",
        host="127.0.0.1",
        port=c.port("materialized", 6877),
        autocommit=True,
    )

    def set_flags(shared_ts: bool, overlap: bool) -> None:
        conn = mz_system.connect()
        try:
            cur = conn.cursor()
            cur.execute(
                b"ALTER SYSTEM SET enable_pipelined_peek_shared_timestamp = "
                + (b"true" if shared_ts else b"false")
            )
            cur.execute(
                b"ALTER SYSTEM SET enable_pipelined_peek_overlap = "
                + (b"true" if overlap else b"false")
            )
        finally:
            conn.close()

    def mz_conn() -> PgConnInfo:
        return PgConnInfo(
            user="materialize",
            database="materialize",
            host=mz_host,
            port=mz_port,
            autocommit=True,
        )

    pg_conn = PgConnInfo(
        user="postgres",
        password="postgres",
        database="postgres",
        host=pg_host,
        port=pg_port,
        autocommit=True,
    )

    # Open one connection per target. The dyncfgs are cached at connect, so set
    # them (system-wide, via mz_system) right before opening each Materialize
    # connection. Keeping all connections open lets us interleave trials so
    # host-load drift hits every target roughly equally within a trial.
    conns: dict[str, psycopg.Connection] = {}
    for label, cfg in TARGETS:
        if cfg is None:
            conns[label] = pg_conn.connect()
        else:
            set_flags(*cfg)
            conns[label] = mz_conn().connect()

    try:
        # One table per backend; the Materialize connections share a DB.
        run_setup(conns["Materialize"])
        run_setup(conns["Postgres"])

        # Verify the server-side pipelining paths return correct results before
        # trusting their timings.
        for label, cfg in TARGETS:
            if cfg is not None:
                check_pipeline(conns[label])

        print(
            f"\n=== Pipeline-mode benchmark ({n} queries/trial,"
            f" {trials} interleaved trials, latency={latency}ms) ==="
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
        set_flags(False, False)


def configure_toxiproxy(c: Composition, latency_ms: int) -> None:
    """Create Toxiproxy proxies in front of Materialize and Postgres with a
    latency toxic in each direction, approximating `latency_ms` round-trip.
    """
    api = c.port("toxiproxy", 8474)

    def post(path: str, body: dict) -> None:
        req = urllib.request.Request(
            f"http://127.0.0.1:{api}{path}",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req).read()

    for name, listen, upstream in [
        ("mz", "0.0.0.0:6875", "materialized:6875"),
        ("pg", "0.0.0.0:5432", "postgres:5432"),
    ]:
        post("/proxies", {"name": name, "listen": listen, "upstream": upstream})
        for stream in ("upstream", "downstream"):
            post(
                f"/proxies/{name}/toxics",
                {
                    "name": f"{name}_{stream}",
                    "type": "latency",
                    "stream": stream,
                    "attributes": {"latency": latency_ms // 2, "jitter": 0},
                },
            )

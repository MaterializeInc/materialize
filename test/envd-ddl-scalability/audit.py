#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""DDL/catalog scaling audit harness.

See README.md for usage. Connects to a running environmentd, pads the
catalog with N objects of a chosen type, and times CREATE/DROP/ALTER/RENAME
of various object types at each scale point.
"""

import argparse
import statistics
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass

import psycopg

# Object name prefixes / schemas / clusters used by the harness. The
# `audit_` prefix lets `--reset` and ad-hoc cleanup target them safely.
PAD_SCHEMA = "audit_pad"
MEAS_SCHEMA = "audit_meas"
MEAS_CLUSTER = "audit_meas_c"
PAD_CLUSTER_PREFIX = "audit_pad_c_"

# Pad MVs/indexes per cluster. Keeps dataflow count per replica modest so
# small replica sizes can host the pad load.
PAD_DATAFLOWS_PER_CLUSTER = 400


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------


class TraceCollector:
    """Collects `trace id: ...` NOTICE lines from a Materialize connection.

    Materialize emits one such NOTICE per statement when
    `emit_trace_id_notice` is enabled. We track them in a list and the
    measurement loop pops the most recent one after each timed statement.
    """

    def __init__(self) -> None:
        self.trace_ids: list[str] = []

    def handle(self, diag: psycopg.errors.Diagnostic) -> None:
        msg = diag.message_primary or ""
        if msg.startswith("trace id: "):
            self.trace_ids.append(msg[len("trace id: ") :].strip())


def connect(url: str, trace_collector: TraceCollector) -> psycopg.Connection:
    conn = psycopg.connect(url, autocommit=True)
    conn.add_notice_handler(trace_collector.handle)
    return conn


def run(conn: psycopg.Connection, sql: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql.encode())


def time_one(
    conn: psycopg.Connection, sql: str, traces: TraceCollector
) -> tuple[float, str | None]:
    """Time a single DDL statement; return (ms, trace_id_or_None)."""
    before = len(traces.trace_ids)
    start = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(sql.encode())
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    trace_id = traces.trace_ids[-1] if len(traces.trace_ids) > before else None
    return elapsed_ms, trace_id


# ---------------------------------------------------------------------------
# Padding strategies
# ---------------------------------------------------------------------------


class PadStrategy:
    """Fills the catalog with N objects. Designed to ramp incrementally so
    a scale ladder (e.g. 0,1000,5000) doesn't redo work between points."""

    name: str

    def init(self, conn: psycopg.Connection) -> None:
        raise NotImplementedError

    def add_n(self, conn: psycopg.Connection, current_n: int, target_n: int) -> None:
        raise NotImplementedError

    def teardown(self, conn: psycopg.Connection) -> None:
        raise NotImplementedError


def _drop_pad_clusters(conn: psycopg.Connection) -> None:
    cur = conn.execute(
        f"SELECT name FROM mz_clusters WHERE name LIKE '{PAD_CLUSTER_PREFIX}%'"
    )
    for (name,) in cur.fetchall():
        run(conn, f"DROP CLUSTER IF EXISTS {name} CASCADE")


def _reset_pad_schema(conn: psycopg.Connection) -> None:
    run(conn, f"DROP SCHEMA IF EXISTS {PAD_SCHEMA} CASCADE")
    run(conn, f"CREATE SCHEMA {PAD_SCHEMA}")


class TablesPadding(PadStrategy):
    name = "tables"

    def init(self, conn: psycopg.Connection) -> None:
        _reset_pad_schema(conn)

    def add_n(self, conn: psycopg.Connection, current_n: int, target_n: int) -> None:
        for i in range(current_n + 1, target_n + 1):
            run(
                conn,
                f"CREATE TABLE IF NOT EXISTS {PAD_SCHEMA}.pad_t_{i} "
                f"(a int, b text)",
            )

    def teardown(self, conn: psycopg.Connection) -> None:
        run(conn, f"DROP SCHEMA IF EXISTS {PAD_SCHEMA} CASCADE")


class ViewsPadding(PadStrategy):
    name = "views"

    def init(self, conn: psycopg.Connection) -> None:
        _reset_pad_schema(conn)

    def add_n(self, conn: psycopg.Connection, current_n: int, target_n: int) -> None:
        for i in range(current_n + 1, target_n + 1):
            # Parameterise on i so views are structurally distinct.
            run(
                conn,
                f"CREATE VIEW {PAD_SCHEMA}.pad_v_{i} AS "
                f"SELECT {i}::int AS x, '{i}'::text AS y",
            )

    def teardown(self, conn: psycopg.Connection) -> None:
        run(conn, f"DROP SCHEMA IF EXISTS {PAD_SCHEMA} CASCADE")


class MvsPadding(PadStrategy):
    name = "mvs"

    def __init__(self, pad_cluster_size: str) -> None:
        self.pad_cluster_size = pad_cluster_size
        self._pad_clusters = 0

    def init(self, conn: psycopg.Connection) -> None:
        _drop_pad_clusters(conn)
        _reset_pad_schema(conn)
        run(conn, f"CREATE TABLE {PAD_SCHEMA}.pad_base (id int, val text)")
        run(conn, f"INSERT INTO {PAD_SCHEMA}.pad_base VALUES (1, 'x')")

    def _ensure_cluster(self, conn: psycopg.Connection, idx: int) -> None:
        while self._pad_clusters <= idx:
            k = self._pad_clusters
            run(
                conn,
                f"CREATE CLUSTER IF NOT EXISTS {PAD_CLUSTER_PREFIX}{k} "
                f"SIZE '{self.pad_cluster_size}'",
            )
            self._pad_clusters += 1

    def add_n(self, conn: psycopg.Connection, current_n: int, target_n: int) -> None:
        i = current_n + 1
        while i <= target_n:
            cluster_idx = (i - 1) // PAD_DATAFLOWS_PER_CLUSTER
            self._ensure_cluster(conn, cluster_idx)
            cluster_end = min(target_n, (cluster_idx + 1) * PAD_DATAFLOWS_PER_CLUSTER)
            for j in range(i, cluster_end + 1):
                run(
                    conn,
                    f"CREATE MATERIALIZED VIEW IF NOT EXISTS "
                    f"{PAD_SCHEMA}.pad_mv_{j} "
                    f"IN CLUSTER {PAD_CLUSTER_PREFIX}{cluster_idx} "
                    f"AS SELECT id, val FROM {PAD_SCHEMA}.pad_base "
                    f"WHERE id < {j}",
                )
            i = cluster_end + 1

    def teardown(self, conn: psycopg.Connection) -> None:
        run(conn, f"DROP SCHEMA IF EXISTS {PAD_SCHEMA} CASCADE")
        _drop_pad_clusters(conn)
        self._pad_clusters = 0


class IndexesPadding(PadStrategy):
    name = "indexes"

    def __init__(self, pad_cluster_size: str) -> None:
        self.pad_cluster_size = pad_cluster_size
        self._pad_clusters = 0

    def init(self, conn: psycopg.Connection) -> None:
        _drop_pad_clusters(conn)
        _reset_pad_schema(conn)
        run(conn, f"CREATE TABLE {PAD_SCHEMA}.pad_base (a int, b int, c int)")
        run(conn, f"INSERT INTO {PAD_SCHEMA}.pad_base VALUES (1, 2, 3)")

    def _ensure_cluster(self, conn: psycopg.Connection, idx: int) -> None:
        while self._pad_clusters <= idx:
            k = self._pad_clusters
            run(
                conn,
                f"CREATE CLUSTER IF NOT EXISTS {PAD_CLUSTER_PREFIX}{k} "
                f"SIZE '{self.pad_cluster_size}'",
            )
            self._pad_clusters += 1

    def add_n(self, conn: psycopg.Connection, current_n: int, target_n: int) -> None:
        i = current_n + 1
        while i <= target_n:
            cluster_idx = (i - 1) // PAD_DATAFLOWS_PER_CLUSTER
            self._ensure_cluster(conn, cluster_idx)
            cluster_end = min(target_n, (cluster_idx + 1) * PAD_DATAFLOWS_PER_CLUSTER)
            for j in range(i, cluster_end + 1):
                # Distinct expression per index so we don't dedupe.
                run(
                    conn,
                    f"CREATE INDEX IF NOT EXISTS pad_idx_{j} "
                    f"IN CLUSTER {PAD_CLUSTER_PREFIX}{cluster_idx} "
                    f"ON {PAD_SCHEMA}.pad_base ((a + {j}))",
                )
            i = cluster_end + 1

    def teardown(self, conn: psycopg.Connection) -> None:
        run(conn, f"DROP SCHEMA IF EXISTS {PAD_SCHEMA} CASCADE")
        _drop_pad_clusters(conn)
        self._pad_clusters = 0


PAD_STRATEGIES: dict[str, Callable[[argparse.Namespace], PadStrategy]] = {
    "tables": lambda args: TablesPadding(),
    "views": lambda args: ViewsPadding(),
    "mvs": lambda args: MvsPadding(args.pad_cluster_size),
    "indexes": lambda args: IndexesPadding(args.pad_cluster_size),
}


# ---------------------------------------------------------------------------
# Measured operations
# ---------------------------------------------------------------------------


@dataclass
class MeasuredOp:
    name: str
    # SQL run once before all reps of this op at a given scale point.
    setup_once: Callable[[], list[str]] = lambda: []
    # SQL run before each rep (timed work excluded). Receives rep index.
    setup_each: Callable[[int], list[str]] = lambda rep: []
    # The single SQL statement we time. Receives rep index.
    query: Callable[[int], str] = lambda rep: ""
    # SQL run after each rep (timed work excluded). Receives rep index.
    after_each: Callable[[int], list[str]] = lambda rep: []


def _ensure_meas_table(conn: psycopg.Connection) -> None:
    run(conn, f"CREATE TABLE IF NOT EXISTS {MEAS_SCHEMA}.m_base (a int)")
    # idempotent — insert only if empty
    cur = conn.execute(f"SELECT count(*) FROM {MEAS_SCHEMA}.m_base")
    (n,) = cur.fetchone() or (0,)
    if n == 0:
        run(conn, f"INSERT INTO {MEAS_SCHEMA}.m_base VALUES (1)")


# Op definitions. Each closure binds nothing — schema/cluster come from
# module-level constants. Per-rep setup keeps measurement-side state
# clean between reps without re-timing.

MEASURED_OPS: dict[str, MeasuredOp] = {
    "create_table": MeasuredOp(
        name="create_table",
        setup_each=lambda rep: [f"DROP TABLE IF EXISTS {MEAS_SCHEMA}.m_tmp CASCADE"],
        query=lambda rep: f"CREATE TABLE {MEAS_SCHEMA}.m_tmp (a int)",
        after_each=lambda rep: [f"DROP TABLE IF EXISTS {MEAS_SCHEMA}.m_tmp CASCADE"],
    ),
    "drop_table": MeasuredOp(
        name="drop_table",
        setup_each=lambda rep: [
            f"DROP TABLE IF EXISTS {MEAS_SCHEMA}.m_tmp CASCADE",
            f"CREATE TABLE {MEAS_SCHEMA}.m_tmp (a int)",
        ],
        query=lambda rep: f"DROP TABLE {MEAS_SCHEMA}.m_tmp",
    ),
    "alter_table_add_col": MeasuredOp(
        name="alter_table_add_col",
        setup_once=lambda: [
            f"DROP TABLE IF EXISTS {MEAS_SCHEMA}.m_tmp CASCADE",
            f"CREATE TABLE {MEAS_SCHEMA}.m_tmp (a int)",
        ],
        # Columns accumulate across reps — that's fine; the cost we care
        # about is the catalog transaction, not the resulting schema width.
        query=lambda rep: (
            f"ALTER TABLE {MEAS_SCHEMA}.m_tmp " f"ADD COLUMN m_col_{rep} text"
        ),
    ),
    "rename_table": MeasuredOp(
        name="rename_table",
        setup_each=lambda rep: [
            f"DROP TABLE IF EXISTS {MEAS_SCHEMA}.m_tmp CASCADE",
            f"DROP TABLE IF EXISTS {MEAS_SCHEMA}.m_tmp_renamed CASCADE",
            f"CREATE TABLE {MEAS_SCHEMA}.m_tmp (a int)",
        ],
        query=lambda rep: (
            f"ALTER TABLE {MEAS_SCHEMA}.m_tmp " f"RENAME TO m_tmp_renamed"
        ),
        after_each=lambda rep: [
            f"DROP TABLE IF EXISTS {MEAS_SCHEMA}.m_tmp_renamed CASCADE",
        ],
    ),
    "create_view": MeasuredOp(
        name="create_view",
        setup_each=lambda rep: [f"DROP VIEW IF EXISTS {MEAS_SCHEMA}.m_v CASCADE"],
        query=lambda rep: (f"CREATE VIEW {MEAS_SCHEMA}.m_v AS SELECT {rep}::int AS x"),
        after_each=lambda rep: [f"DROP VIEW IF EXISTS {MEAS_SCHEMA}.m_v CASCADE"],
    ),
    "drop_view": MeasuredOp(
        name="drop_view",
        setup_each=lambda rep: [
            f"DROP VIEW IF EXISTS {MEAS_SCHEMA}.m_v CASCADE",
            f"CREATE VIEW {MEAS_SCHEMA}.m_v AS SELECT {rep}::int AS x",
        ],
        query=lambda rep: f"DROP VIEW {MEAS_SCHEMA}.m_v",
    ),
    "create_mv": MeasuredOp(
        name="create_mv",
        setup_each=lambda rep: [
            f"DROP MATERIALIZED VIEW IF EXISTS {MEAS_SCHEMA}.m_mv CASCADE",
        ],
        query=lambda rep: (
            f"CREATE MATERIALIZED VIEW {MEAS_SCHEMA}.m_mv "
            f"IN CLUSTER {MEAS_CLUSTER} "
            f"AS SELECT a + {rep} AS x FROM {MEAS_SCHEMA}.m_base"
        ),
        after_each=lambda rep: [
            f"DROP MATERIALIZED VIEW IF EXISTS {MEAS_SCHEMA}.m_mv CASCADE",
        ],
    ),
    "drop_mv": MeasuredOp(
        name="drop_mv",
        setup_each=lambda rep: [
            f"DROP MATERIALIZED VIEW IF EXISTS {MEAS_SCHEMA}.m_mv CASCADE",
            f"CREATE MATERIALIZED VIEW {MEAS_SCHEMA}.m_mv "
            f"IN CLUSTER {MEAS_CLUSTER} "
            f"AS SELECT a + {rep} AS x FROM {MEAS_SCHEMA}.m_base",
        ],
        query=lambda rep: f"DROP MATERIALIZED VIEW {MEAS_SCHEMA}.m_mv",
    ),
    "create_index": MeasuredOp(
        name="create_index",
        setup_each=lambda rep: [
            f"DROP INDEX IF EXISTS {MEAS_SCHEMA}.m_idx CASCADE",
        ],
        query=lambda rep: (
            f"CREATE INDEX m_idx IN CLUSTER {MEAS_CLUSTER} "
            f"ON {MEAS_SCHEMA}.m_base ((a + {rep}))"
        ),
        after_each=lambda rep: [f"DROP INDEX IF EXISTS {MEAS_SCHEMA}.m_idx CASCADE"],
    ),
    "drop_index": MeasuredOp(
        name="drop_index",
        setup_each=lambda rep: [
            f"DROP INDEX IF EXISTS {MEAS_SCHEMA}.m_idx CASCADE",
            f"CREATE INDEX m_idx IN CLUSTER {MEAS_CLUSTER} "
            f"ON {MEAS_SCHEMA}.m_base ((a + {rep}))",
        ],
        query=lambda rep: f"DROP INDEX {MEAS_SCHEMA}.m_idx",
    ),
}


# ---------------------------------------------------------------------------
# Measurement loop
# ---------------------------------------------------------------------------


@dataclass
class Sample:
    padding: str
    scale: int
    op: str
    rep: int
    ms: float
    trace_id: str | None


def measure_op(
    conn: psycopg.Connection,
    traces: TraceCollector,
    op: MeasuredOp,
    padding: str,
    scale: int,
    reps: int,
) -> list[Sample]:
    for sql in op.setup_once():
        run(conn, sql)
    samples: list[Sample] = []
    for rep in range(reps):
        for sql in op.setup_each(rep):
            run(conn, sql)
        ms, trace_id = time_one(conn, op.query(rep), traces)
        for sql in op.after_each(rep):
            run(conn, sql)
        samples.append(Sample(padding, scale, op.name, rep, ms, trace_id))
    return samples


def setup_measurement_state(conn: psycopg.Connection, meas_cluster_size: str) -> None:
    """Idempotent setup of the schema/cluster used by measured ops."""
    run(conn, f"CREATE SCHEMA IF NOT EXISTS {MEAS_SCHEMA}")
    run(
        conn,
        f"CREATE CLUSTER IF NOT EXISTS {MEAS_CLUSTER} " f"SIZE '{meas_cluster_size}'",
    )
    _ensure_meas_table(conn)


def teardown_measurement_state(conn: psycopg.Connection) -> None:
    run(conn, f"DROP SCHEMA IF EXISTS {MEAS_SCHEMA} CASCADE")
    run(conn, f"DROP CLUSTER IF EXISTS {MEAS_CLUSTER} CASCADE")


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def _percentile(values: list[float], pct: float) -> float:
    s = sorted(values)
    if not s:
        return 0.0
    k = max(0, min(len(s) - 1, int(round(pct / 100.0 * (len(s) - 1)))))
    return s[k]


def print_summary(samples: list[Sample]) -> None:
    """Aggregate samples by (padding, scale, op) and print a table."""
    by_key: dict[tuple[str, int, str], list[float]] = {}
    for s in samples:
        by_key.setdefault((s.padding, s.scale, s.op), []).append(s.ms)
    header = (
        f"{'padding':<10} {'scale':>8} {'op':<22} "
        f"{'n':>3} {'p50_ms':>8} {'p95_ms':>8} "
        f"{'max_ms':>8} {'mean_ms':>9}"
    )
    print(header)
    print("-" * len(header))
    for (padding, scale, op), ms_list in sorted(by_key.items()):
        n = len(ms_list)
        p50 = _percentile(ms_list, 50)
        p95 = _percentile(ms_list, 95)
        mx = max(ms_list)
        mean = statistics.fmean(ms_list)
        print(
            f"{padding:<10} {scale:>8d} {op:<22} {n:>3d} "
            f"{p50:>8.1f} {p95:>8.1f} {mx:>8.1f} {mean:>9.1f}"
        )


def write_trace_csv(path: str, samples: list[Sample]) -> None:
    """Write one row per sample: padding, scale, op, rep, ms, trace_id."""
    with open(path, "w") as f:
        f.write("padding,scale,op,rep,ms,trace_id\n")
        for s in samples:
            f.write(
                f"{s.padding},{s.scale},{s.op},{s.rep},"
                f"{s.ms:.3f},{s.trace_id or ''}\n"
            )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="DDL/catalog scaling audit harness",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--url",
        default="postgres://materialize@localhost:6875/materialize",
        help="psycopg connection URL to a running environmentd",
    )
    p.add_argument(
        "--padding",
        required=True,
        choices=sorted(PAD_STRATEGIES.keys()),
        help="which type of object fills the catalog",
    )
    p.add_argument(
        "--scale",
        default="0,1000,5000",
        help="comma-separated N values; pad ramps incrementally",
    )
    p.add_argument(
        "--ops",
        default=(
            "create_table,drop_table,alter_table_add_col,rename_table,"
            "create_view,drop_view,create_mv,drop_mv,"
            "create_index,drop_index"
        ),
        help="comma-separated measured ops",
    )
    p.add_argument("--reps", type=int, default=10, help="reps per (scale, op)")
    p.add_argument(
        "--pad-cluster-size",
        default="50cc",
        help="SIZE for pad clusters (mvs / indexes padding)",
    )
    p.add_argument(
        "--meas-cluster-size",
        default="50cc",
        help="SIZE for the measurement cluster",
    )
    p.add_argument(
        "--trace-out",
        default=None,
        help="optional CSV path to write per-sample timings + trace ids",
    )
    p.add_argument(
        "--keep-state",
        action="store_true",
        help="skip teardown of pad+meas state at the end",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    scale_ladder = sorted({int(s) for s in args.scale.split(",")})
    op_names = [o.strip() for o in args.ops.split(",") if o.strip()]
    bad = [o for o in op_names if o not in MEASURED_OPS]
    if bad:
        print(f"unknown ops: {bad}", file=sys.stderr)
        print(f"valid ops: {sorted(MEASURED_OPS.keys())}", file=sys.stderr)
        return 2
    ops = [MEASURED_OPS[o] for o in op_names]
    padding = PAD_STRATEGIES[args.padding](args)

    traces = TraceCollector()
    conn = connect(args.url, traces)
    run(conn, "SET emit_trace_id_notice = true")

    print(
        f"# padding={padding.name} scale={scale_ladder} "
        f"ops={op_names} reps={args.reps}"
    )

    samples: list[Sample] = []
    try:
        setup_measurement_state(conn, args.meas_cluster_size)
        padding.init(conn)

        current_n = 0
        for target_n in scale_ladder:
            if target_n > current_n:
                t0 = time.time()
                padding.add_n(conn, current_n, target_n)
                print(
                    f"# padded {padding.name} "
                    f"{current_n} -> {target_n} "
                    f"({time.time() - t0:.1f}s)"
                )
                current_n = target_n
            for op in ops:
                op_samples = measure_op(
                    conn, traces, op, padding.name, target_n, args.reps
                )
                samples.extend(op_samples)
                ms = [s.ms for s in op_samples]
                print(
                    f"  scale={target_n:>6d} {op.name:<22} "
                    f"p50={_percentile(ms, 50):>7.1f}ms "
                    f"p95={_percentile(ms, 95):>7.1f}ms "
                    f"max={max(ms):>7.1f}ms"
                )
    finally:
        if not args.keep_state:
            try:
                padding.teardown(conn)
            except Exception as e:
                print(f"warning: padding teardown failed: {e}", file=sys.stderr)
            try:
                teardown_measurement_state(conn)
            except Exception as e:
                print(f"warning: meas teardown failed: {e}", file=sys.stderr)

    print()
    print_summary(samples)
    if args.trace_out:
        write_trace_csv(args.trace_out, samples)
        print(f"\nwrote {len(samples)} samples to {args.trace_out}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

# envd DDL scalability audit

A tight, standalone reproducer for measuring how DDL and catalog-transaction
latency scale with the number of objects in the catalog. Intended for
**profiling**, not for CI tracking — the canonical numbers live in
`test/cluster-spec-sheet`.

The harness connects to a **running** `environmentd` instance via psycopg.
It does **not** start its own envd. That makes the iteration loop tight:
start envd once (optionally with tracing or attached to a profiler) and run
the harness against it as many times as you like.

## Quick start

### 1. Start environmentd

For tracing (Tempo / Grafana on `localhost:3000`):

```bash
bin/mzcompose --find monitoring run default
bin/environmentd --optimized --monitoring --reset
```

Or for plain fast iteration (no tracing):

```bash
bin/environmentd --reset
```

### 2. Run the audit

```bash
python3 test/envd-ddl-scalability/audit.py \
    --padding tables \
    --scale 0,1000,5000 \
    --ops create_table,drop_table,alter_table_add_col,rename_table \
    --reps 10
```

Replace `--padding tables` with `mvs`, `views`, or `indexes` to vary the
catalog along a different axis. `--ops` accepts a comma-separated subset of
the operations listed below.

## What it measures

For each `(padding, scale_point, op)` cell, the harness runs `--reps`
repetitions of `op`, timing only the measured DDL statement (each rep has
its own per-rep `setup_each` / `after_each` that is **not** timed). It
reports p50 / p95 / max latency in milliseconds.

### Padding axes (`--padding`)

| value | what fills the catalog |
| --- | --- |
| `tables` | N empty tables (no dataflows; pure catalog) |
| `views` | N views with distinct projections (no dataflows; pure catalog + plan cache) |
| `mvs` | N materialized views, sharded across pad clusters |
| `indexes` | N indexes on a single base table, sharded across pad clusters |

### Measured ops (`--ops`)

| op | what it times |
| --- | --- |
| `create_table` | `CREATE TABLE m_tmp (a int)` |
| `drop_table` | `DROP TABLE m_tmp` (pre-created in setup_each) |
| `alter_table_add_col` | `ALTER TABLE m_tmp ADD COLUMN m_col_<rep> text` |
| `rename_table` | `ALTER TABLE m_tmp RENAME TO m_tmp_renamed` |
| `create_view` | `CREATE VIEW m_v AS SELECT <rep>::int AS x` |
| `drop_view` | `DROP VIEW m_v` (pre-created in setup_each) |
| `create_mv` | `CREATE MATERIALIZED VIEW m_mv IN CLUSTER m_c AS SELECT a + <rep> FROM m_base` |
| `drop_mv` | `DROP MATERIALIZED VIEW m_mv` (pre-created in setup_each) |
| `create_index` | `CREATE INDEX m_idx IN CLUSTER m_c ON m_base ((a + <rep>))` |
| `drop_index` | `DROP INDEX m_idx` (pre-created in setup_each) |

## Profiling

### Tracing (Tempo)

Pass `--trace-out /tmp/audit-traces.csv` to record a trace ID per measured
statement (requires envd started with `--monitoring`; the harness enables
`emit_trace_id_notice` automatically). Look up trace IDs at
`http://localhost:3200/api/traces/<id>` or in Grafana, and analyse them with
`.claude/skills/mz-query-tracing/trace_tree.py`.

The `--system-parameter-default 'opentelemetry_filter=debug'` flag on envd
is useful for deeper spans.

### CPU profile (samply)

While envd is running, attach `samply`:

```bash
samply record -p $(pgrep -f 'target/release/environmentd') &
python3 test/envd-ddl-scalability/audit.py --padding tables --scale 5000 --ops create_table --reps 100
# Ctrl-C samply; open the flame graph
```

### Targeted instrumentation

Once tracing/CPU has narrowed the surface, add scoped
`#[tracing::instrument]` or `Instant::now()` timers in the suspected code
paths and re-run. Trace IDs from the harness map directly to the new spans.

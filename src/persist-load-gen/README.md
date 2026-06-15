# persist-load-gen

A standalone load generator that drives **real persist shards** against a consensus backend,
reproducing the *breadth* workload a Materialize deployment imposes on its consensus store — with
no environmentd/clusterd/dataflow in the way.

## What it does

It opens `--shards` real persist shards via `mz-persist-client`. For each shard:

- a single **writer** advances the shard's write frontier once per `--tick-ms` by appending an
  **empty batch** (`compare_and_append` of zero updates from `upper` → `upper+1`) — exactly what a
  materialized view's persist sink does on each timestamp tick. The persist state machine turns each
  advance into a compare-and-set against consensus (plus periodic rollups/GC).
- `--readers-per-shard` (default **2**) **leased readers** are held open. Their lease renewals and
  periodic `since` downgrades add the reader-side consensus traffic of a real deployment and let
  state get compacted/GCed.

Because it goes through the full client, the consensus CaS rate is *higher* than the raw
frontier-advance rate (readers + rollups + retries all hit consensus) — i.e. it's the real load,
not a synthetic approximation.

It reports per-interval frontier **advances/s**, their latency (p50/p99), and mismatch/error counts,
and serves the **full persist metrics** — including `mz_persist_external_{succeeded,failed}_count{op="consensus_cas"}`
and the CaS latency histogram, the same instruments used in the cluster experiments — at
`--metrics-listen-addr`/metrics.

## Build & run

```bash
cargo build --release -p mz-persist-load-gen

./target/release/persist-load-gen \
  --url 'postgres://materialize:PW@HOST:5432/materialize?sslmode=require' \
  --shards 4000 --readers-per-shard 2 --pool-max-size 50 --duration-secs 180
```

Needs a consensus URL (`--url`, Postgres) and a blob store. `--blob-url` defaults to a temp
`file://` dir (self-contained against just a Postgres); pass `s3://bucket/prefix` for full fidelity.
Each run uses fresh shard ids, so it never collides with a previous run (no `--reset` needed).

## Knobs

| flag | meaning |
|---|---|
| `--shards` | number of shards (the breadth axis; each ≈ one ticking MV) |
| `--readers-per-shard` | active leased readers per shard (default 2) — lease renewals + `since` downgrades |
| `--tick-ms` | per-shard min interval between frontier advances (default 1000 ≈ 1 tick/s). Under DB slowness a shard advances as fast as appends return (the real batching backpressure) |
| `--pool-max-size` | `persist_consensus_connection_pool_max_size` — concurrent consensus connections (admission control) |
| `--tuned-queries` | toggle `persist_use_postgres_tuned_queries` |
| `--since-lag` / `--since-every` | how far behind `upper` readers keep `since`, and how often to downgrade it. `--since-every 1` ≈ the real controller's per-tick downgrades; `0` pins `since` (no truncation/GC — table grows) |
| `--batch-rows` / `--row-pad-bytes` | rows appended per tick and their padding (default 0 = empty batches). Small batches are stored *inline* in the consensus state row (`persist_inline_writes_single_max_bytes` = 4096), so this grows `consensus.consensus.data` like real MV shards' batch metadata does |
| `--blob-url` | blob store (default temp `file://`; `s3://...` for fidelity) |
| `--duration-secs` / `--report-secs` / `--ramp-secs` | run length / report cadence / shard-startup stagger |
| `--metrics-listen-addr` | serve persist Prometheus metrics here (default `127.0.0.1:6878`) |

## Suggested experiments

- **Throughput vs load:** sweep `--shards` at fixed `--pool-max-size`; watch advances/s and the
  `consensus_cas` rate/latency (`/metrics`) — does it plateau or collapse, and where?
- **Admission control:** at a collapsing shard count, lower `--pool-max-size` and check whether the
  consensus CaS rate recovers to a flat plateau.
- **tuned_queries:** same ramp with/without `--tuned-queries`.
- **0dt-style amplification:** raise `--readers-per-shard` (e.g. 2 → 4) to mimic a second generation
  registering extra readers per shard.
- Inspect the DB side concurrently (`pg_locks`, `pg_stat_activity` wait events) to see the SSI
  predicate-lock / tuple-lock contention directly.

## Smoke check (local Docker Postgres)

```bash
docker run -d --name pg -e POSTGRES_PASSWORD=pw -e POSTGRES_USER=materialize \
  -e POSTGRES_DB=materialize -p 55432:5432 postgres:18
./target/release/persist-load-gen \
  --url 'postgres://materialize:pw@localhost:55432/materialize?sslmode=disable' \
  --shards 20 --readers-per-shard 2 --duration-secs 20
# ~20 advances/s (20 shards × 1/s); consensus table populated; /metrics shows consensus_cas counts.
```

# Storage Usage Coordinator Blocking — Optimization Log

## Current Setup

```bash
# Always use --optimized for measurements. Debug builds are too slow — the
# off-thread shard fetch dominates and starves collection cycles, making cycle
# timing data unreliable. Use debug only for compile checks during development.
bin/environmentd --optimized --build-only
bin/environmentd --optimized -- --system-parameter-default=log_filter=info

# Connect as materialize user (external port)
psql -U materialize -h localhost -p 6875 materialize

# Connect as mz_system (internal port — needed for ALTER SYSTEM SET)
psql -U mz_system -h localhost -p 6877 materialize

# Set fast collection interval for testing
psql -U mz_system -h localhost -p 6877 materialize -c \
  "ALTER SYSTEM SET storage_usage_collection_interval = '10s';"
```

**Don't use `--reset` between runs.** Creating thousands of tables takes a long
time. Reuse existing state across runs and builds.

After a fresh `--reset`, raise limits:
```bash
psql -U mz_system -h localhost -p 6877 materialize -c "
  ALTER SYSTEM SET max_tables = 100000;
  ALTER SYSTEM SET max_objects_per_schema = 100000;
"
```

Bulk-create tables (each table = 1 persist shard):
```bash
for i in $(seq 1 5000); do
  echo "CREATE TABLE su_$i (a int);"
done > /tmp/bulk_create.sql
psql -U materialize -h localhost -p 6875 materialize -f /tmp/bulk_create.sql
```

## Measurement Approach

The key metric is coordinator stall time per storage usage collection cycle.

**Primary metric:** `mz_slow_message_handling{message_kind="storage_usage_update"}`
— this is a histogram tracking wall-clock time of the `storage_usage_update`
handler on the coordinator main thread.

**Capture method:** Snapshot Prometheus metrics before and after several
collection cycles, then compute per-cycle averages from histogram deltas.

```bash
# Snapshot before
curl -s http://localhost:6878/metrics | grep -E 'mz_slow_message_handling' > /tmp/before.txt

# Wait for N collection cycles (N * collection_interval seconds)
sleep 60  # e.g., 6 cycles at 10s interval

# Snapshot after
curl -s http://localhost:6878/metrics | grep -E 'mz_slow_message_handling' > /tmp/after.txt

# Compare — look at _count and _sum for storage_usage_update
diff /tmp/before.txt /tmp/after.txt
```

**Additional metrics to watch:**
- `mz_catalog_transact_seconds` — time in catalog_transact_inner
- `mz_storage_usage_collection_time_seconds` — off-thread shard scan (not the bottleneck)
- `mz_ts_oracle_seconds{op="write_ts"}` — oracle round-trip time

**For deeper instrumentation:** Add `tracing::info!` with elapsed timings or
custom histograms inside `storage_usage_update` to isolate which of the 5 costs
(oracle, transact_op loop, persist write, persist read, group commit) dominates.

## Optimization Log

### Session 1 — Baseline measurements (2026-03-06)

**Setup:** optimized build, `--storage-usage-collection-interval-sec=10s`,
existing persist state with su_1..su_10000 tables.

**Baseline results** (`mz_slow_message_handling{message_kind="storage_usage_update"}`):

| Shards | Avg coordinator stall per cycle |
|--------|---------------------------------|
| ~2,441 | ~51ms |
| ~5,087 | ~150ms |
| ~10,087 | ~499ms |

Scaling is roughly linear with shard count. At 10k shards the coordinator
blocks for ~500ms every collection cycle.

Histogram distribution at 10k shards: most cycles land in 256-512ms bucket.

### Session 2 — Instrumentation results (2026-03-06)

Added timing logs to `storage_usage_update` and `catalog::transact`. Results at
10,087 shards (optimized build):

| Phase | Time | % of total |
|-------|------|-----------|
| Oracle call #1 (storage_usage_update) | ~2ms | 0.4% |
| Open transaction | ~10ms | 2% |
| **transact_inner (10k op loop)** | **~430ms** | **91%** |
| Persist commit (write + read) | ~6ms | 1.3% |
| Group commit (builtin_table_update.execute) | ~15ms | 3% |
| **Total** | **~475ms** | **100%** |

**Finding:** The dominant cost is the N-iteration `transact_inner` loop, not
persist I/O or oracle calls. Each iteration allocates a storage usage id,
packs a row, and does catalog bookkeeping. The persist commit and group commit
are surprisingly cheap (~20ms combined).

**Implication:** Bypassing `catalog_transact_inner` should reduce cost from
~475ms to ~17ms (oracle + group commit only). The op loop, open_tx, and
persist commit would all be eliminated.

**Next:** Implement the fix — pack builtin table rows directly, bypass
catalog_transact_inner.

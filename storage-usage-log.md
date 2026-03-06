# Storage Usage Coordinator Blocking — Optimization Log

## Current Setup

```bash
# Debug build (faster to compile, good for diagnosing scaling patterns)
bin/environmentd --build-only
bin/environmentd -- --system-parameter-default=log_filter=info

# Optimized build (for confirming fixes against realistic absolute numbers)
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

(No sessions yet)

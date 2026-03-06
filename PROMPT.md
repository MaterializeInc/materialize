I'm working on diagnosing and fixing coordinator blocking caused by periodic
storage usage collection (`storage_usage_update`). We have a design doc at
@misc/storage-usage-coord-blocking.md and a session log at
@storage-usage-log.md where we record findings for continuity across sessions.

The problem: every collection interval (default 1 hour), the coordinator
processes `Message::StorageUsageUpdate` on its main thread. This calls
`storage_usage_update` (message_handler.rs:239), which routes N shard updates
through `catalog_transact_inner` — incurring 2 oracle round-trips, N
`transact_op` calls, a persist write (id allocator bump), a persist read
(sync_updates), and a group commit wait. In environments with hundreds or
thousands of shards, this blocks the coordinator for seconds.

The shard-size fetch itself (`storage_usage_fetch`) runs off-thread and is not
the problem. The fix is to stop routing updates through `catalog_transact_inner`
and instead append builtin table rows directly.

Figure out where coordinator time is going, confirm with measurements, and fix
it. Use profiling, metrics, code analysis, and custom logging as needed.

## Workflow

* Each solid progress (baseline measurement, diagnosis finding, optimization)
  gets committed as a jj change with a good description, then continue to the
  next step.
* Before committing, verify that what you produced is high quality and works.
* Code should be simple and clean, well-commented explaining what/how/why.
* Minimal changes — if we iterate and try multiple things, clean up to the
  minimum required fix at the end.
* **Read this file again after each context compaction.**
* Update @storage-usage-log.md after each milestone with a new session entry.
* Update this file's "Current status" and "Immediate next steps" sections after
  each milestone.
* Update the design doc (@misc/storage-usage-coord-blocking.md) if experimental
  findings contradict or refine it.

## Steps

1. Reproduce/measure the problem (establish baselines at various shard counts)
2. Profile and diagnose where time is spent on the coordinator thread
3. Fix the bottleneck (following the design doc proposal or better)
4. Re-measure to confirm improvement
5. Repeat — there may be multiple layers of bottlenecks

## Setup

```bash
# Build — always use --optimized for measurements. Debug builds are too slow
# (the off-thread shard fetch dominates, starving collection cycles) so cycle
# timing data from debug builds is unreliable. Use debug only for compilation
# checks during development, not for running measurements.
bin/environmentd --optimized

# Connect
psql -U materialize -h localhost -p 6875 materialize
psql -U mz_system -h localhost -p 6877 materialize  # for ALTER SYSTEM SET

# Trigger frequent collection (default is 3600s) — this is a startup flag, not
# an ALTER SYSTEM SET variable:
#   bin/environmentd --optimized -- --storage-usage-collection-interval-sec=10s

# Prometheus metrics
curl -s http://localhost:6878/metrics > /tmp/metrics.txt
```

To create many shards, create many tables (each table = 1 shard):
```bash
for i in $(seq 1 5000); do
  echo "CREATE TABLE su_$i (a int);"
done > /tmp/bulk_create.sql
psql -U materialize -h localhost -p 6875 materialize -f /tmp/bulk_create.sql
```

Don't use `--reset` between runs — reuse existing state.

## Key Metrics

| Metric | What it measures |
|--------|-----------------|
| `mz_slow_message_handling{message_kind="storage_usage_update"}` | Wall-clock time of `storage_usage_update` on the coord thread |
| `mz_slow_message_handling{message_kind="storage_usage_fetch"}` | Time to dispatch the fetch (should be near-zero, fetch is off-thread) |
| `mz_storage_usage_collection_time_seconds` | Off-thread shard scan duration (not the problem) |
| `mz_catalog_transact_seconds` | Catalog transaction time (includes the persist write/read) |

Capture before/after a collection cycle:
```bash
curl -s http://localhost:6878/metrics | grep -E 'mz_slow_message_handling|storage_usage_collection' > /tmp/before.txt
# wait for a collection cycle
curl -s http://localhost:6878/metrics | grep -E 'mz_slow_message_handling|storage_usage_collection' > /tmp/after.txt
```

## Key Code Paths

| Component | Location |
|-----------|----------|
| `storage_usage_update` | `src/adapter/src/coord/message_handler.rs:239` |
| `storage_usage_fetch` | `src/adapter/src/coord/message_handler.rs:208` |
| `schedule_storage_usage_collection` | `src/adapter/src/coord/message_handler.rs:316` |
| `Op::WeirdStorageUsageUpdates` | `src/adapter/src/catalog/transact.rs:240` |
| Op processing (allocate_id + pack) | `src/adapter/src/catalog/transact.rs:2582` |
| `builtin_table_update().execute()` | `src/adapter/src/coord/appends.rs:797` |
| `pack_storage_usage_update` | `src/adapter/src/catalog/builtin_table_updates.rs:2082` |
| `VersionedStorageUsage` | `src/audit-log/src/lib.rs` |
| Collection interval config | `src/environmentd/src/environmentd/main.rs:381` |

## Cost Breakdown on Coordinator Thread (from design doc, unverified)

| Cost | Source |
|------|--------|
| 2 oracle round-trips | `get_local_write_ts()` in `storage_usage_update` and `catalog_transact_inner` |
| N `transact_op` calls | Loop in `transact_inner` (increment counter + pack row each) |
| Persist write | `commit_transaction` (compare_and_append for id_allocator bump) |
| Persist read | `sync_updates` after commit |
| Group commit wait | `builtin_table_update().execute()` |

## Current Status

Baseline measurements complete (optimized build):
- ~2.4k shards: ~51ms/cycle
- ~5k shards: ~150ms/cycle
- ~10k shards: ~499ms/cycle
Scaling is linear. 10k su_ tables exist in persist state (don't use --reset).
Instrumentation shows 91% of time is in transact_inner op loop (~430ms for 10k
shards). Persist and oracle are cheap. Next: implement fix to bypass
catalog_transact_inner.

## Immediate Next Steps

1. **Baseline measurement.** Start environmentd with many tables (1k, 5k, 10k, 20k),
   set `storage_usage_collection_interval` to 10s, capture
   `mz_slow_message_handling{message_kind="storage_usage_update"}` histograms
   across several collection cycles. Establish how coordinator stall scales
   with shard count.

2. **Instrument `storage_usage_update`.** Add timing logs or histogram sub-metrics
   to isolate which of the 5 costs dominates: oracle calls, transact_op loop,
   persist write, persist read, or group commit.

3. **Implement the fix.** Bypass `catalog_transact_inner`: pack builtin table rows
   directly and append via `builtin_table_update().execute()`. Drop or replace
   the durable id allocator with a cheap local counter.

4. **Clean up.** Remove `Op::WeirdStorageUsageUpdates`, the durable
   `STORAGE_USAGE_ID_ALLOC_KEY` allocator, and simplify `VersionedStorageUsage`.

5. **Re-measure.** Confirm coordinator stall is reduced to just group-commit time.

## Work-in-progress log (update after each milestone)

- (nothing yet)

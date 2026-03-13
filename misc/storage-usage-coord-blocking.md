# Storage usage collection blocks the coordinator for seconds in large environments

The periodic storage usage collection (`storage_usage_update`) blocks the coordinator main loop for an unexpectedly long time in customer environments with many shards. Here's what it does, what we measured, and the fix.

## What happens today (before fix)

Every collection interval, the coordinator processes `Message::StorageUsageUpdate` on the main thread (`message_handler.rs:239`). This calls `storage_usage_update`, which:

1. **Fetches a write timestamp** from the oracle (`get_local_write_ts`) — one persist round-trip
2. Creates one `Op::WeirdStorageUsageUpdates` per shard (N ops for N shards)
3. Calls `catalog_transact_inner` (`ddl.rs:293`), which:
   - **Fetches another write timestamp** from the oracle (`get_local_write_ts` at `ddl.rs:447`) — a second persist round-trip
   - Opens a durable catalog `Transaction`
   - Loops N times through `transact_op`, each calling `tx.allocate_storage_usage_ids()` → `get_and_increment_id(STORAGE_USAGE_ID_ALLOC_KEY)`, which bumps the in-memory id allocator counter in the transaction
   - Each op returns its row as a `weird_builtin_table_update`. The `id_allocator` mutation is explicitly excluded from `get_op_updates()` (`transaction.rs:2315`), so `get_and_commit_op_updates()` returns empty — no per-op `apply_updates` calls happen on the COW catalog state
   - **Persist write**: `tx.commit()` → `commit_internal` → consolidates all transaction tables and calls `commit_transaction` (compare_and_append to persist) — this durably writes the id allocator bump
   - **Persist read**: `sync_updates` after commit
   - `builtin_table_update().execute(builtin_table_updates)` submits rows to group commit and waits

The shard-size fetch itself (step 1 of the overall flow in `storage_usage_fetch`) runs off-thread and is not the problem.

### Cost breakdown on the coordinator main thread (predicted)

| Cost | Source | Notes |
|------|--------|-------|
| 2 oracle round-trips | `get_local_write_ts()` in both `storage_usage_update` and `catalog_transact_inner` | Redundant — only needed once |
| N `transact_op` calls | Loop in `transact_inner` | Cheap per call (increment counter + pack row), but N can be hundreds/thousands |
| Persist write | `commit_transaction` (compare_and_append) | Writes the id_allocator bump — the only durable change |
| Persist read | `sync_updates` after commit | Drains updates from persist |
| Group commit wait | `builtin_table_update().execute()` | Waits for builtin table append |

### Measured cost breakdown (10k shards, optimized build)

We added timing instrumentation to `storage_usage_update` and `catalog::transact`
to measure each phase individually.

| Phase | Time | % of total |
|-------|------|-----------|
| Oracle call #1 (storage_usage_update) | ~2ms | 0.4% |
| Open transaction | ~10ms | 2% |
| **transact_inner (10k op loop)** | **~430ms** | **91%** |
| Persist commit (write + read) | ~6ms | 1.3% |
| Group commit (builtin_table_update.execute) | ~15ms | 3% |
| **Total** | **~475ms** | **100%** |

**Key finding:** The op loop dominates — not persist I/O or oracle calls as
initially suspected. Each of the 10k iterations allocates a storage usage id,
packs a row, and does catalog bookkeeping (Cow clone checks, `get_and_commit_op_updates`, etc.).
The per-iteration cost is ~43µs, which is individually cheap but adds up across
thousands of shards.

### Scaling behavior

Coordinator stall scales linearly with shard count:

| Shards | Avg coordinator stall per cycle |
|--------|---------------------------------|
| ~2,400 | ~51ms |
| ~5,100 | ~150ms |
| ~10,100 | ~499ms |

## Why does it go through `catalog_transact_inner` at all?

Only to increment the durable `STORAGE_USAGE_ID_ALLOC_KEY` allocator, which populates the `id` column of `mz_storage_usage_by_shard`. But this `id` column is unused:

- `mz_storage_usage` joins on `(shard_id, collection_timestamp)`, ignores `id`
- `mz_recent_storage_usage` same — joins on `shard_id` and `collection_timestamp`
- No other view or query references the `id` column
- `VersionedStorageUsage::sortable_id()` exists but is never called (it was modeled after audit log events, where the ID is actually used for ordering/dedup during catalog open)

The data itself is append-only during normal operation. Retractions only happen once at startup during pruning (`prune_storage_usage_events_on_startup`), through the `builtin_table_update().execute()` path — which already bypasses `catalog_transact_inner`. The pruning code even asserts that consolidated contents have no retractions (`assert_eq!(diff, 1, ...)`).

## Fix

Stop routing storage usage updates through `catalog_transact_inner`. The new
`storage_usage_update` does:

1. Get a write timestamp from the oracle (unchanged)
2. Pack N `BuiltinTableUpdate` rows directly using `pack_storage_usage_update`
3. Submit via `builtin_table_update().execute()` — the same group commit path
   that pruning already uses

The durable `STORAGE_USAGE_ID_ALLOC_KEY` allocator is replaced with a cheap
local counter (`storage_usage_next_id` on the Coordinator struct). The `id`
column value is unused by any view or query, so durability is unnecessary.

This eliminates:
- The persist write (compare_and_append for the id allocator bump)
- The persist read (sync_updates)
- The redundant oracle timestamp
- The N-iteration `transact_op` loop
- Opening a catalog transaction

The coord-blocking work becomes: get one write timestamp (~2ms), pack N rows
(< 1ms), submit to group commit (~18ms).

### Results (prototype, optimized build, 10k shards)

| | Before | After | Speedup |
|--|--------|-------|---------|
| Avg per cycle | ~499ms | ~20ms | **25x** |

All cycles now land in the 16–32ms histogram bucket (previously 256–512ms).
Data correctness verified: 10,087 rows written per collection cycle,
`mz_storage_usage` view returns correct results.

## Cleanup (done)

Dead code removed:
- `Op::WeirdStorageUsageUpdates` variant and match arms
- `weird_builtin_table_update` return value from `transact_op`
- `allocate_storage_usage_ids` method and `STORAGE_USAGE_ID_ALLOC_KEY` constant
- Diagnostic `info!` log from `storage_usage_update`

`VersionedStorageUsage` and its `id` field left as-is — it's a versioned
persisted type and a V2 migration just to drop an unused field isn't worth it.

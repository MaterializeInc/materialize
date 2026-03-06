# Storage usage collection blocks the coordinator for seconds in large environments

The periodic storage usage collection (`storage_usage_update`) blocks the coordinator main loop for an unexpectedly long time in customer environments with many shards. Here's what it does and a proposal to fix it.

## What happens today

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

### Cost breakdown on the coordinator main thread

| Cost | Source | Notes |
|------|--------|-------|
| 2 oracle round-trips | `get_local_write_ts()` in both `storage_usage_update` and `catalog_transact_inner` | Redundant — only needed once |
| N `transact_op` calls | Loop in `transact_inner` | Cheap per call (increment counter + pack row), but N can be hundreds/thousands |
| Persist write | `commit_transaction` (compare_and_append) | Writes the id_allocator bump — the only durable change |
| Persist read | `sync_updates` after commit | Drains updates from persist |
| Group commit wait | `builtin_table_update().execute()` | Waits for builtin table append |

**Measured (10k shards, optimized build):** The op loop dominates at ~430ms
(91% of ~475ms total). Oracle (~2ms), persist commit (~6ms), and group commit
(~15ms) are all cheap. The fix eliminates ~450ms of the ~475ms total.

## Why does it go through `catalog_transact_inner` at all?

Only to increment the durable `STORAGE_USAGE_ID_ALLOC_KEY` allocator, which populates the `id` column of `mz_storage_usage_by_shard`. But this `id` column is unused:

- `mz_storage_usage` joins on `(shard_id, collection_timestamp)`, ignores `id`
- `mz_recent_storage_usage` same — joins on `shard_id` and `collection_timestamp`
- No other view or query references the `id` column
- `VersionedStorageUsage::sortable_id()` exists but is never called (it was modeled after audit log events, where the ID is actually used for ordering/dedup during catalog open)

The data itself is append-only during normal operation. Retractions only happen once at startup during pruning (`prune_storage_usage_events_on_startup`), through the `builtin_table_update().execute()` path — which already bypasses `catalog_transact_inner`. The pruning code even asserts that consolidated contents have no retractions (`assert_eq!(diff, 1, ...)`).

## Proposal

Stop routing storage usage updates through `catalog_transact_inner`. Instead:
- Pack the builtin table rows directly and append them via `builtin_table_update().execute()` (the same path pruning uses)
- Either drop the `id` column from `mz_storage_usage_by_shard` (it's unused), or populate it with a cheap local counter instead of a durable allocator

This eliminates:
- The persist write (compare_and_append for the id allocator bump)
- The persist read (sync_updates)
- The redundant oracle timestamp
- The N-iteration `transact_op` loop

The coord-blocking work becomes: get one write timestamp, pack N rows, submit to group commit.

The `WeirdStorageUsageUpdates` op variant (it's literally called "Weird" in the code), the durable ID allocator, and the `VersionedStorageUsage` type in the audit-log crate could all be cleaned up as part of this.

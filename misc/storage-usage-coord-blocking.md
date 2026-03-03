# Storage usage collection blocks the coordinator for seconds in large environments

The periodic storage usage collection (`storage_usage_update`) blocks the coordinator main loop for an unexpectedly long time in customer environments with many shards. Here's what it does and a proposal to fix it.

## What happens today

Every collection interval, we:
1. Fetch shard sizes off-thread (already async, not the problem)
2. Back on the coord main thread, create one `Op::WeirdStorageUsageUpdates` per shard
3. Run all ops through `catalog_transact_inner`, which:
   - Opens a durable catalog transaction
   - Loops over every op, and for each one: allocates a storage usage ID via the durable ID allocator, packs a builtin table row, and calls `apply_updates` on a COW clone of the full `CatalogState`
   - Commits the transaction to persist (`compare_and_append`)
   - Syncs updates back
4. Spawns a background task to append the builtin table rows (this part is fine)

For an environment with N shards, this is N iterations through the full catalog transact machinery, plus a persist round-trip to commit — all blocking the coordinator.

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

This turns the coord-blocking work from "open catalog transaction, process N ops with per-op state cloning, commit to persist" into "pack N rows, submit to group commit" — which is what we already do for the pruning retractions.

The `WeirdStorageUsageUpdates` op variant (it's literally called "Weird" in the code), the durable ID allocator, and the `VersionedStorageUsage` type in the audit-log crate could all be cleaned up as part of this.

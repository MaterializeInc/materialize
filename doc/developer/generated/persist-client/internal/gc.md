---
source: src/persist-client/src/internal/gc.rs
revision: 665d33182e
---

# persist-client::internal::gc

Implements `GarbageCollector`, a background task that deletes blob objects and truncates consensus entries that are no longer reachable from any live state version.
GC is triggered by a `GcReq` specifying the new `seqno_since`; it uses `GcRollups` to identify rollup checkpoints at or before `seqno_since`, then incrementally deletes unreferenced batch part and rollup blobs and truncates consensus for each rollup boundary in sequence, with blob deletions performed at configurable concurrency (`GC_BLOB_DELETE_CONCURRENCY_LIMIT`).
Multiple pending GC requests for the same shard are coalesced into a single pass before execution.
A `GC_USE_ACTIVE_GC` feature flag controls whether the diff scan starts from a fetched rollup plus live diffs or falls back to scanning all live states.

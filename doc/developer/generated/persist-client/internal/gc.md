---
source: src/persist-client/src/internal/gc.rs
revision: 8c74de9000
---

# persist-client::internal::gc

Implements `GarbageCollector`, a background task that deletes blob objects and truncates consensus entries that are no longer reachable from any live state version.
GC is triggered by a `GcReq` specifying the new `seqno_since`; it uses `GcRollups` to identify rollup checkpoints at or before `seqno_since`, then incrementally deletes unreferenced batch part and rollup blobs and truncates consensus for each rollup boundary in sequence, with blob deletions performed at configurable concurrency (`GC_BLOB_DELETE_CONCURRENCY_LIMIT`).
Multiple pending GC requests for the same shard are coalesced into a single pass before execution.
A `GC_USE_ACTIVE_GC` feature flag controls whether the diff scan starts from a fetched rollup plus live diffs or falls back to scanning all live states.
The rollup invariant assertion (verifying that the current state contains a rollup to the earliest fetched state) is skipped under active GC, because that path only fetches diffs through `seqno_since` and the reconstructed state at that point legitimately may not yet contain the rollup entry; the active GC path validates the same invariant against fresh data when it resolves the rollup for the initial seqno.

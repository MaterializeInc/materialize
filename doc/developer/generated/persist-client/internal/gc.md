---
source: src/persist-client/src/internal/gc.rs
revision: 1a8344fa07
---

# persist-client::internal::gc

Implements `GarbageCollector`, a background task that deletes blob objects and truncates consensus entries that are no longer reachable from any live state version.
GC is triggered by a `GcReq` specifying the new `seqno_since`; it walks the diff log, identifies unreferenced rollup and batch part blobs, and deletes them with configurable concurrency (`GC_BLOB_DELETE_CONCURRENCY_LIMIT`).

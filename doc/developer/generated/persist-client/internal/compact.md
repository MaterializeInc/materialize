---
source: src/persist-client/src/internal/compact.rs
revision: 4267863081
---

# persist-client::internal::compact

Implements `Compactor`, the background worker that executes asynchronous compaction of batch parts.
Given a `CompactReq` (a set of `HollowBatch`es from a `FueledMergeReq`), the compactor reads all input parts, consolidates them, writes the merged output parts to blob, and reports the result back via `FueledMergeRes`.
Compaction runs on the `IsolatedRuntime` and respects memory budget limits (`COMPACTION_MEMORY_BOUND_BYTES`).

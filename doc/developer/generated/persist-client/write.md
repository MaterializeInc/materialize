---
source: src/persist-client/src/write.rs
revision: b33ffcb977
---

# persist-client::write

Defines `WriteHandle`, the primary public API for writing to a shard, and `WriterId` (a UUID identifying a writer session).
`WriteHandle` exposes `compare_and_append` (atomic upper-check-then-append), `append` (unconditional retry loop), and `batch`/`batch_builder` (build a `Batch` without appending it yet).
Writer sessions heartbeat to register their presence; the upper of the shard advances only through successful `compare_and_append` operations.
Optionally creates a `Compactor` at construction time if compaction is enabled, allowing the writer to claim and process compaction work after appends.
Inline writes below a size threshold can be combined into batch metadata via `COMBINE_INLINE_WRITES`.

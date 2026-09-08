---
source: src/persist-client/src/write.rs
revision: 6ebd534824
---

# persist-client::write

Defines `WriteHandle`, the primary public API for writing to a shard, and `WriterId` (a UUID identifying a writer session).
`WriteHandle` exposes `compare_and_append` (atomic upper-check-then-append), `append` (unconditional retry loop), and `batch`/`batch_builder` (build a `Batch` without appending it yet).
Writer sessions heartbeat to register their presence; the upper of the shard advances only through successful `compare_and_append` operations. `compare_and_append` no longer takes a heartbeat timestamp parameter; the timestamp is sampled inside the retry closure so each retry commits a fresh value rather than one computed before the first attempt.
Optionally creates a `Compactor` at construction time if compaction is enabled, allowing the writer to claim and process compaction work after appends.
Inline writes below a size threshold can be combined into batch metadata via `COMBINE_INLINE_WRITES`.
`compare_and_append_batch` calls `validate_truncate_batch` for each input batch; if the returned bool is true (the batch is bounds-truncated), it calls `RunMeta::set_bounds_truncated` on every run_meta entry for that batch before the state is committed to consensus.

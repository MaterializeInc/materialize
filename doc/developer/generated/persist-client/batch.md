---
source: src/persist-client/src/batch.rs
revision: 6ebd534824
---

# persist-client::batch

Defines `Batch` (a handle to a set of updates written to blob storage but not yet appended to a shard) and `BatchBuilder` (the incremental writer that accumulates updates, flushes parts to blob, and produces a `Batch`).
`BatchBuilder` pipelines part uploads using a configurable outstanding-parts limit and optionally computes per-part statistics for read-time pushdown.
`Batch` must be either consumed by an append or explicitly deleted to avoid leaking blob objects.
`validate_truncate_batch` returns `Result<bool, ...>`: the bool indicates whether the append desc is narrower than the batch's own written desc (bounds-truncated), meaning parts may physically hold updates outside the registered bounds.
When `compare_and_append_batch` detects that a batch is bounds-truncated, it calls `RunMeta::set_bounds_truncated` on every run_meta entry for that batch before committing the state, so that downstream stats-based accounting skips the `diffs_sum` shortcut for those runs.

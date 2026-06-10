---
source: src/persist-client/src/batch.rs
revision: 4267863081
---

# persist-client::batch

Defines `Batch` (a handle to a set of updates written to blob storage but not yet appended to a shard) and `BatchBuilder` (the incremental writer that accumulates updates, flushes parts to blob, and produces a `Batch`).
`BatchBuilder` pipelines part uploads using a configurable outstanding-parts limit and optionally computes per-part statistics for read-time pushdown.
`Batch` must be either consumed by an append or explicitly deleted to avoid leaking blob objects.

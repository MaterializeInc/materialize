---
source: src/persist-client/src/read.rs
revision: b33ffcb977
---

# persist-client::read

Defines `ReadHandle`, the primary public API for reading from a shard, along with `LeasedReaderId` (a UUID identifying a leased reader), `Subscribe` (snapshot-then-listen streaming), `Listen` (streaming listener for new batches beyond a frontier), and `Cursor` (a consolidating iterator over snapshot data).
`ReadHandle` supports snapshot reads (materializing all data up to the since via `Cursor`), listening (streaming updates from a frontier forward), and combined snapshot-then-listen workflows via `Subscribe`.
Leased readers heartbeat periodically via a background task to hold their since capability; failure to heartbeat causes the lease to expire and the reader to be cleaned up.
`Cursor` wraps a `Consolidator` and yields consolidated `(K, V, T, D)` tuples in batches bounded by configurable size limits.

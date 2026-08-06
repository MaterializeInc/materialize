---
source: src/persist-client/src/read.rs
revision: 1af97ffb4d
---

# persist-client::read

Defines `ReadHandle`, the primary public API for reading from a shard, along with `LeasedReaderId` (a UUID identifying a leased reader), `Subscribe` (snapshot-then-listen streaming), `Listen` (streaming listener for new batches beyond a frontier), and `Cursor` (a consolidating iterator over snapshot data).
`ReadHandle` supports snapshot reads (materializing all data up to the since via `Cursor`), listening (streaming updates from a frontier forward), and combined snapshot-then-listen workflows via `Subscribe`.
Leased readers heartbeat periodically via a background task to hold their since capability; failure to heartbeat causes the lease to expire and the reader to be cleaned up. `downgrade_since` no longer takes a heartbeat timestamp parameter; the timestamp is sampled inside the retry closure so each retry uses a fresh value.
`Cursor` wraps a `Consolidator` and yields consolidated `(K, V, T, D)` tuples in batches bounded by configurable size limits.
Each `LeasedBatchPart` produced by `lease_batch_parts` records the `LeasedReaderId` of the minting handle, enabling downstream diagnostic checks when a blob fetch fails.
`ReadHandle` also exposes `shared_upper()`, which returns a less-stale cached version of the shard-global upper frontier by acquiring it via mutex from the shared `Applier`; this is distinct from `since()` and is always less than or equal to the true shard-global upper.

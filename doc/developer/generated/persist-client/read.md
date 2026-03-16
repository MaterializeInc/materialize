---
source: src/persist-client/src/read.rs
revision: 4267863081
---

# persist-client::read

Defines `ReadHandle`, the primary public API for reading from a shard, along with `LeasedReaderId` (a UUID identifying a leased reader) and `Subscribe` (a streaming listener for new batches beyond a frontier).
`ReadHandle` supports snapshot reads (materializing all data up to the since), listening (streaming updates from a frontier forward), and combined snapshot-then-listen workflows.
Leased readers heartbeat periodically to hold their since capability; failure to heartbeat causes the lease to expire and the reader to be cleaned up.

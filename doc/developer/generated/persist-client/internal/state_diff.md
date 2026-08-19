---
source: src/persist-client/src/internal/state_diff.rs
revision: f0cdedca04
---

# persist-client::internal::state_diff

Defines `StateDiff`, the incremental diff between two consecutive `State` versions that is written to consensus.
A `StateDiff` records field-level changes (`StateFieldDiff`) as insert/update/delete operations on each collection in `StateCollections`, enabling efficient forward replay to reconstruct any state version without reading a full rollup.
`StateDiff` is the primary unit of data exchanged over the PubSub channel.
`ProtoStateFieldDiffs::validate()` uses checked arithmetic when summing `data_lens` entries to detect overflow. An unchecked sum can wrap to a small value in optimized builds, pass the byte-length check, and cause `ProtoStateFieldDiffsIter` to slice `data_bytes` far out of range. The overflow case is returned as a decode error.

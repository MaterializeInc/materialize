---
source: src/persist-client/src/internal/state_diff.rs
revision: 832d16d8e3
---

# persist-client::internal::state_diff

Defines `StateDiff`, the incremental diff between two consecutive `State` versions that is written to consensus.
A `StateDiff` records field-level changes (`StateFieldDiff`) as insert/update/delete operations on each collection in `StateCollections`, enabling efficient forward replay to reconstruct any state version without reading a full rollup.
`StateDiff` is the primary unit of data exchanged over the PubSub channel.

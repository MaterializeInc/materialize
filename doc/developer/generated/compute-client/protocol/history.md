---
source: src/compute-client/src/protocol/history.rs
revision: c0a1c584d1
---

# mz-compute-client::protocol::history

Implements `ComputeCommandHistory`, a compacted log of `ComputeCommand`s used to replay state to a newly connected replica.
The history automatically reduces itself by eliminating superseded commands (e.g., compacting `AllowCompaction` commands and dropping dataflows whose compaction frontier is empty) whenever the command sequence doubles in size.
Tracks command and dataflow counts via `HistoryMetrics`.

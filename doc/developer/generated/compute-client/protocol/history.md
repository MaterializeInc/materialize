---
source: src/compute-client/src/protocol/history.rs
revision: f4f99cbc37
---

# mz-compute-client::protocol::history

Implements `ComputeCommandHistory`, a compacted log of `ComputeCommand`s used to replay state to a newly connected replica.
The history automatically reduces itself by eliminating superseded commands (e.g., compacting `AllowCompaction` commands, dropping dataflows whose compaction frontier is empty, deduplicating configuration updates, and retaining only live peeks) whenever the command sequence doubles in size.
Reduction also handles `Schedule` commands (retaining only those for live dataflows), `AllowWrites` commands (retaining only those for live dataflows), and `InitializationComplete`.
Tracks command and dataflow counts via `HistoryMetrics`. Also provides `discard_peeks` to drop all peek commands and `update_source_uppers` to refresh storage import upper frontiers before connecting a new replica.

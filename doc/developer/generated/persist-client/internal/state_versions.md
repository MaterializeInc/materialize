---
source: src/persist-client/src/internal/state_versions.rs
revision: 181b1e7efc
---

# persist-client::internal::state_versions

Implements `StateVersions`, the durable log of shard state versions stored in consensus (as incremental `StateDiff`s) and blob (as periodic `rollup` snapshots).
Provides operations to initialize a shard, append new diffs, fetch the current state (by replaying diffs from the latest rollup), and truncate old entries once they are no longer needed.
The invariant that every live diff range has a covering rollup ensures that state can always be reconstructed from blob alone.

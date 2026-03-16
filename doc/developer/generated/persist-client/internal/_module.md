---
source: src/persist-client/src/internal/state.rs
revision: 901d0526a1
---

# persist-client::internal

Contains the private implementation layer of `mz-persist-client`, organized around the shard state machine.

Key components and their relationships:

* `state` and `state_diff` — the core data model: `State`/`TypedState` and the incremental `StateDiff`s that transition between versions.
* `state_versions` — durable log of state versions in consensus (diffs) and blob (rollups); the source of truth for shard history.
* `machine` — the retry loop that drives all state transitions by repeatedly applying pure transition functions via `apply`.
* `apply` — the narrow CaS interface between `Machine` and the durable stores; also publishes diffs to PubSub.
* `trace` — the `Spine`-based compaction index tracking all `HollowBatch` pointers for a shard.
* `compact` and `gc` — background workers for compaction and blob/consensus cleanup respectively.
* `maintenance` — collects and schedules background work after each successful CaS.
* `encoding` — proto serialization for all internal types.
* `metrics` — Prometheus instrumentation for all operations.
* `paths` — blob and consensus key naming.
* `cache`, `merge`, `watch`, `restore`, `service`, `datadriven` — supporting utilities.

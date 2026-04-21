---
source: src/compute-client/src/as_of_selection.rs
revision: b55d3dee25
---

# mz-compute-client::as_of_selection

Implements as-of selection for compute dataflows during system initialization.
For each compute collection in a set of `DataflowDescription`s, it determines a compatible `as_of` timestamp by maintaining `AsOfBounds` (lower/upper bound pairs) that are iteratively refined by applying `Constraint`s derived from collection dependencies and storage since frontiers.
The final upper bound is chosen to maximize compute reconciliation effectiveness and minimize historical data reads.
The public entry point is `run`, which accepts a `read_only_mode` flag; when set, collections depending on dropped storage collections (those with empty read frontiers) are pruned before constraints are applied, to avoid hard-constraint failures during 0dt upgrades. Dataflows sinking into sealed persist shards are pruned unconditionally.

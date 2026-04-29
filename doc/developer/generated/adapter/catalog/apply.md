---
source: src/adapter/src/catalog/apply.rs
revision: 9d0a7c3c6f
---

# adapter::catalog::apply

Implements the logic for applying `StateUpdate` diffs from the durable catalog store to the in-memory `CatalogState`.
`apply_updates` processes a batch of `StateUpdate` values grouped by timestamp; within each timestamp group, updates are sorted into a pseudo-topological order before being applied.
An `InProgressRetractions` struct caches denormalized state stripped during retractions so that the corresponding additions can reuse it without rebuilding from scratch.
The `ApplyState` state machine batches consecutive updates of the same type when beneficial, then delegates to `apply_updates_inner`, which calls per-type `apply_*_update` methods for roles, role auth, databases, schemas, clusters, network policies, items, system configuration, and more.
After processing retractions that are not replaced by a corresponding addition (i.e., truly dropped items), `apply_updates` calls `drop_optimizer_notices` to clean up any optimizer notices associated with the dropped items and emits the corresponding `mz_notices` retractions.
`CatalogState` methods defined in this module (`set_optimized_plan`, `set_physical_plan`, `set_dataflow_metainfo`, `drop_optimizer_notices`) mutate the plan and notice fields stored directly on `Index` and `MaterializedView` catalog items.
This is the central reconciliation path used both during initial catalog open and when reacting to concurrent remote catalog mutations.

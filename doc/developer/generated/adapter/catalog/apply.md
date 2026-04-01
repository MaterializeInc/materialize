---
source: src/adapter/src/catalog/apply.rs
revision: f75acad2d2
---

# adapter::catalog::apply

Implements the logic for applying `StateUpdate` diffs from the durable catalog store to the in-memory `CatalogState`.
`apply_updates` processes a batch of `StateUpdate` values grouped by timestamp; within each timestamp group, updates are sorted into a pseudo-topological order before being applied.
An `InProgressRetractions` struct caches denormalized state stripped during retractions so that the corresponding additions can reuse it without rebuilding from scratch.
The `ApplyState` state machine batches consecutive updates of the same type when beneficial, then delegates to `apply_updates_inner`, which calls per-type `apply_*_update` methods for roles, role auth, databases, schemas, clusters, network policies, items, system configuration, and more.
This is the central reconciliation path used both during initial catalog open and when reacting to concurrent remote catalog mutations.

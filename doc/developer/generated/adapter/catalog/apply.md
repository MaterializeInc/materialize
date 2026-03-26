---
source: src/adapter/src/catalog/apply.rs
revision: af9155582e
---

# adapter::catalog::apply

Implements the logic for applying `StateUpdate` diffs from the durable catalog store to the in-memory `CatalogState`.
`apply_updates` processes a batch of `(StateUpdateKind, Diff)` pairs in dependency order, handling inserts, updates, and deletes for all catalog object types (databases, schemas, roles, clusters, replicas, items, audit log entries, and more).
This is the central reconciliation path used both during initial catalog open and when reacting to concurrent remote catalog mutations.
Supports `Builtin::MaterializedView` items during bootstrap (parsing, inserting, and extending keys from the builtin definition).
Builtin item update sorting partitions into sources (applied before clusters, since clusters depend on introspection logs) and all other builtins (applied after clusters).

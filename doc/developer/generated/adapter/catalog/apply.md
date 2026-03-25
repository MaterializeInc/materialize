---
source: src/adapter/src/catalog/apply.rs
revision: 2c413b395c
---

# adapter::catalog::apply

Implements the logic for applying `StateUpdate` diffs from the durable catalog store to the in-memory `CatalogState`.
`apply_updates` processes a batch of `(StateUpdateKind, Diff)` pairs in dependency order, handling inserts, updates, and deletes for all catalog object types (databases, schemas, roles, clusters, replicas, items, audit log entries, and more).
This is the central reconciliation path used both during initial catalog open and when reacting to concurrent remote catalog mutations.

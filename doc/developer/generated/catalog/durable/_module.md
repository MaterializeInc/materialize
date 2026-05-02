---
source: src/catalog/src/durable.rs
revision: af5783aa4f
---

# catalog::durable

Provides the durable (persistent) storage layer of the catalog, backed by a persist shard.
The central trait `DurableCatalogState` exposes the full read-write API: snapshots, transactions, audit-log iteration, and state subscriptions.
`OpenableDurableCatalogState` handles the open/boot lifecycle including epoch fencing and migration.
`Transaction` is the primary mutation interface, collecting batched DDL changes for atomic commit.
The submodules are: `persist` (the concrete persist-backed implementation), `objects` (on-disk data types), `upgrade` (schema migration), `initialize` (bootstrap logic), `debug` (manual inspection/repair), `error` (error types), `metrics` (Prometheus counters), and `traits` (orphan-rule workarounds).

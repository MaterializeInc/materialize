---
source: src/catalog/src/durable.rs
revision: 8598d82c1c
---

# catalog::durable

Provides the durable (persistent) storage layer of the catalog, backed by a persist shard.
The central trait `DurableCatalogState` exposes the full read-write API: snapshots, transactions, and state subscriptions.
`OpenableDurableCatalogState` handles the open/boot lifecycle including epoch fencing and migration. `open()` and `open_savepoint()` return `Result<Box<dyn DurableCatalogState>, CatalogError>` directly. Audit log entries are served from `mz_internal.mz_catalog_raw` via the `mz_audit_events` materialized view and are no longer returned from `open()`.
`Transaction` is the primary mutation interface, collecting batched DDL changes for atomic commit.
The submodules are: `persist` (the concrete persist-backed implementation), `objects` (on-disk data types), `upgrade` (schema migration), `initialize` (bootstrap logic), `debug` (manual inspection/repair), `error` (error types), `metrics` (Prometheus counters), and `traits` (orphan-rule workarounds).

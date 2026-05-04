# adapter::catalog

The adapter's catalog layer: wraps `mz_catalog` (the durable store and in-memory
objects) with the adapter-specific logic needed to drive DDL and serve catalog queries.
Approximately 16,458 LOC.

## File map

| File | What it owns |
|---|---|
| `catalog.rs` *(parent)* | `Catalog` struct: holds `CatalogState`, `ExpressionCacheHandle`, and a durable-store handle; 116 public methods, most delegating to `self.state.*` |
| `state.rs` | `CatalogState` — the authoritative in-memory view: `imbl::OrdMap` collections for all catalog objects; implements `SessionCatalog` for SQL name resolution; also defines `LocalExpressionCache` |
| `transact.rs` | `Catalog::transact` — opens a durable `Transaction`, executes `Op` sequence (Create/Drop/Rename/Grant/…), flushes to durable storage, enforces referential integrity + privilege checks, writes audit log; also manages 0dt deployment gating |
| `apply.rs` | `apply_updates` — reconciles `StateUpdate` diffs from durable store into `CatalogState`; used during bootstrap and for live remote mutations; `ApplyState` batches consecutive same-type updates before delegating to per-type `apply_*` methods |
| `open.rs` | `Catalog::open` — full initialization at environment startup: open durable store, run AST migrations, apply persisted diffs, bootstrap built-in objects, run builtin-schema migrations |
| `open/builtin_schema_migration.rs` | Builtin-schema migration helpers called from `open.rs` |
| `migrate.rs` | AST-level migrations: parse-transform-reserialize stored SQL for incompatible syntax changes; also `durable_migrate` for type changes (Table → MaterializedView) |
| `builtin_table_updates.rs` | `pack_*` functions: convert `CatalogEntry` objects into `BuiltinTableUpdate` row diffs for system catalog tables (e.g. `mz_tables`, `mz_clusters`, `mz_roles`) |
| `builtin_table_updates/notice.rs` | Special handling for `mz_recent_activity_log` and related notice tables |
| `consistency.rs` | Debug-mode invariant checking across `CatalogState` |
| `timeline.rs` | Timeline-context resolution for timestamp selection |

## Key concepts

- **`Catalog`** — thin facade over `CatalogState` + `ExpressionCacheHandle` + durable store; ~112 of its 116 public methods delegate to `self.state.*`. The facade separates the persistence handle (async, fallible) from the in-memory read view.
- **Write path**: `Coordinator::catalog_transact` (in `coord/ddl.rs`) → `Catalog::transact` (builds `Op` list, opens durable `Transaction`, flushes) → returns `StateUpdate` diffs → `apply_catalog_implications` (in `coord/catalog_implications.rs`) applies controller mutations → `Catalog::apply_updates` reconciles diffs into `CatalogState`.
- **Read path**: `Catalog::for_session(session)` produces a `SessionCatalog` view used by the SQL planner and RBAC subsystem; it borrows `CatalogState` without touching the durable store.
- **`LocalExpressionCache`** — tracks optimizer expressions found/missing in the persistent expression cache during `open` and `transact`, avoiding redundant re-optimization.
- **`ApplyState`** — internal state machine in `apply.rs` that batches same-type updates for efficiency, then dispatches to per-type `apply_*` methods. Also caches denormalized state (`InProgressRetractions`) so retractions can efficiently re-use it during additions.
- **0dt deployment gating** — `transact.rs` gates certain DDL operations behind deployment-readiness checks during the cutover window.

## Architecture notes

- The catalog layer spans two crates: `mz_catalog` owns the durable store protocol and in-memory object types; `adapter::catalog` owns the adapter-specific transaction logic and system-table generation. The boundary is clean: `mz_catalog::durable::DurableCatalogState` is a trait, and `adapter::catalog` depends on it through `Arc<Mutex<Box<dyn DurableCatalogState>>>`.
- `apply.rs` (2,823 LOC) and `transact.rs` (3,313 LOC) are the two largest files. Both are mechanically large because they cover every catalog object type; the per-type handlers are individually short.
- `builtin_table_updates.rs` (2,194 LOC) is similarly large-by-coverage: one `pack_*` function per catalog object type. Adding a new system table requires adding a `pack_*` here.
- The dual read-path (`CatalogState` in-memory vs durable store) means there is no single read authority: SQL planning reads `CatalogState` (cheap, sync) while DDL validation also reads `CatalogState` (within the same coordinator task). This is intentional and not a friction.

## Cross-references

- Caller: `Coordinator` (via `coord/ddl.rs` → `Catalog::transact`; via `coord/catalog_implications.rs` → `Catalog::apply_updates`).
- Downstream: `mz_catalog::durable::DurableCatalogState` (persist-backed or in-memory for tests).
- Generated developer docs: `doc/developer/generated/adapter/catalog/`.

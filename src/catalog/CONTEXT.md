# src/catalog — crate `mz-catalog`

Materialize's catalog crate. Provides persistent metadata storage (durable
persist-backed shard) and an in-memory object model consumed by the coordinator.

## Structure (LOC ≈ 37,222)

| Path | LOC | What it owns |
|---|---|---|
| `src/builtin.rs` | 14,931 | All hardcoded system objects (`BUILTINS_STATIC: LazyLock<Vec<Builtin<NameReference>>>`): types, views, MVs, logs, tables, sources, funcs, connections, roles, clusters, replicas. ~176 static constants. `BUILTINS` module exposes typed iterators (`logs()`, `types()`, `views()`, …). Ordered by dependency; ordering enforced by test. |
| `src/durable/` | 13,781 | Persist-backed storage engine. See [`src/durable/CONTEXT.md`](src/durable/CONTEXT.md). |
| `src/memory/objects.rs` | 3,985 | Rich in-memory catalog types (`CatalogItem`, `CatalogEntry`, `Database`, `Schema`, `Role`, `Cluster`, `ClusterReplica`, `NetworkPolicy`). `UpdateFrom<durable::*>` impls are the primary durable→memory conversion seam. `StateUpdate`/`StateUpdateKind` drive incremental replay. |
| `src/memory/error.rs` | 228 | In-memory constraint violation error types. |
| `src/expr_cache.rs` | 826 | `ExpressionCache` — persist-backed cache for optimized MIR and physical plans. Invalidated per-entry on optimizer feature change or index ID mismatch. Background compaction task. |
| `src/durable.rs` | 561 | Trait definitions: `OpenableDurableCatalogState`, `ReadOnlyDurableCatalogState`, `DurableCatalogState`. Also `BootstrapArgs`, `Epoch`, `AuditLogIterator`, test helpers. |
| `src/config.rs` | 345 | `Config` and `StateConfig` — catalog open parameters (build info, env ID, replica size map, license key, feature flags, etc.). `ClusterReplicaSizeMap`. |
| `src/lib.rs` | 25 | Re-exports: `builtin`, `config`, `durable`, `expr_cache`, `memory`. Defines `SYSTEM_CONN_ID`. |
| `tests/` | 2,184 | Integration tests: `open.rs` (catalog boot/open lifecycle), `read-write.rs` (Transaction DDL round-trips), `debug.rs` (DebugCatalogState inspection). |

## Key concepts

- **Durable / in-memory seam.** `durable::objects` key-value types are internal; only their combined structs (e.g. `durable::Database`) are re-exported. `memory::objects` consumes these via `UpdateFrom<durable::*>` — the sole approved conversion path. Adapter consumers touch only `memory::objects` types.
- **`DurableCatalogState`** — single concrete implementation: `PersistCatalogState` in `durable/persist.rs`. All catalog DDL goes through `Transaction::commit → TransactionBatch → compare_and_append`.
- **`BUILTINS_STATIC`** — dependency-ordered `LazyLock` of all system objects. Adding a builtin = add a `const` definition + insert into `BUILTINS_STATIC` in dependency order.
- **`ExpressionCache`** — separate persist shard; not part of the main catalog shard. Lifecycle is independent of `DurableCatalogState`.
- **Upgrade chain** — versioned and frozen. Current version: 81; minimum supported: 74. Each step isolated in its own submodule with frozen proto/serde types.

## What this crate does

- **Durable store.** A single persist shard ("catalog") holds all catalog state as typed key-value collections. `DurableCatalogState` (trait) / `PersistCatalogState` (sole Implementation) provide the read-write API. `Transaction` batches all DDL into atomic `compare_and_append` commits.
- **In-memory view.** `memory::objects` defines rich Rust types (`CatalogItem`, `CatalogEntry`, `Cluster`, `Role`, etc.) that are richer than on-disk representations. `UpdateFrom<durable::*>` impls are the only approved durable→memory conversion path.
- **Builtins.** `builtin.rs` declares all ~176 hardcoded system objects (types, views, logs, tables, roles, clusters) as `const` definitions assembled into `BUILTINS_STATIC`, ordered by dependency.
- **Expression cache.** `expr_cache.rs` provides a separate persist-backed cache for MIR and physical plans, invalidated on optimizer feature change.
- **Schema migrations.** `durable/upgrade/` chains version-step modules (v74→v81); each step is frozen-type isolated.

## Boundaries

- **Up:** consumed almost exclusively by `mz-adapter` (coordinator) and the `catalog-debug` CLI. No adapter logic in this crate.
- **Down:** `mz-persist-client` (storage), `mz-catalog-protos` (schema + `CATALOG_VERSION`), `mz-sql` (name resolution types), `mz-repr`, `mz-storage-types`, `mz-compute-types`.

## Key architecture concern (from durable/ARCH_REVIEW.md §1)

`Transaction` in `durable/transaction.rs` is a 4,117-LOC struct with 19
parallel `TableTransaction` fields and 103 public methods. Adding a catalog
collection requires changes at five lockstep sites: field, method group,
`Snapshot`, `TransactionBatch`, and serialization. This is the primary
structural friction point in the crate.

## What should bubble up to src/CONTEXT.md

- `mz-catalog` is the single source of truth for all persistent catalog
  metadata in Materialize; it is a clean leaf (no upward dependency on adapter).
- The durable/in-memory type boundary (`UpdateFrom<durable::*>`) is a critical
  seam — adapter code must not import `durable::objects` key-value types directly.
- `Transaction` god-struct risk: 19 collections, 103 methods, five-site coupling
  per new collection. Candidate for a collection-registry refactor.
- Upgrade chain is well-isolated (frozen types per version step); `mz-catalog-protos`
  owns the authoritative `CATALOG_VERSION` constant.

## Cross-references

- [`src/durable/CONTEXT.md`](src/durable/CONTEXT.md) — durable submodule.
- [`src/durable/ARCH_REVIEW.md`](src/durable/ARCH_REVIEW.md) — structural findings.
- Primary consumer: `mz-adapter` (coordinator `open_catalog`, `catalog_transact`).
- Persist integration: `mz-persist-client` via `durable/persist.rs`.
- Schema protos: `mz-catalog-protos` (owns `CATALOG_VERSION`).
- Generated developer docs: `doc/developer/generated/catalog/`.

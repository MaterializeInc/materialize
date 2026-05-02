# src/catalog — crate `mz-catalog`

Materialize's catalog crate. Provides persistent metadata storage (durable
persist-backed shard) and an in-memory object model consumed by the coordinator.

## Structure (LOC ≈ 37,222)

| Path | LOC | What it owns |
|---|---|---|
| `src/` | 35,038 | All production Rust; see [`src/CONTEXT.md`](src/CONTEXT.md). |
| `tests/` | 2,184 | Integration tests: `open.rs` (catalog boot/open lifecycle), `read-write.rs` (Transaction DDL round-trips), `debug.rs` (DebugCatalogState inspection). |

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

- [`src/CONTEXT.md`](src/CONTEXT.md) — module-level detail.
- [`src/durable/CONTEXT.md`](src/durable/CONTEXT.md) — durable submodule.
- [`src/durable/ARCH_REVIEW.md`](src/durable/ARCH_REVIEW.md) — structural findings.
- Generated developer docs: `doc/developer/generated/catalog/`.

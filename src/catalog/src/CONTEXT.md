# catalog (crate: mz-catalog)

Materialize's catalog layer: persistent metadata storage for the coordinator,
providing both a durable persist-backed store and a rich in-memory view of all
catalog objects.

## Modules (LOC ≈ 35,038 across `src/catalog/src/`)

| Module / file | LOC | What it owns |
|---|---|---|
| `builtin.rs` | 14,931 | All hardcoded system objects (`BUILTINS_STATIC: LazyLock<Vec<Builtin<NameReference>>>`): types, views, MVs, logs, tables, sources, funcs, connections, roles, clusters, replicas. ~176 static constants. `BUILTINS` module exposes typed iterators (`logs()`, `types()`, `views()`, …). Ordered by dependency; ordering enforced by test. |
| `durable/` | 13,781 | Persist-backed storage engine. See [`durable/CONTEXT.md`](durable/CONTEXT.md). |
| `memory/objects.rs` | 3,985 | Rich in-memory catalog types (`CatalogItem`, `CatalogEntry`, `Database`, `Schema`, `Role`, `Cluster`, `ClusterReplica`, `NetworkPolicy`). `UpdateFrom<durable::*>` impls are the primary durable→memory conversion seam. `StateUpdate`/`StateUpdateKind` drive incremental replay. |
| `memory/error.rs` | 228 | In-memory constraint violation error types. |
| `expr_cache.rs` | 826 | `ExpressionCache` — persist-backed cache for optimized MIR and physical plans. Invalidated per-entry on optimizer feature change or index ID mismatch. Background compaction task. |
| `durable.rs` | 561 | Trait definitions: `OpenableDurableCatalogState`, `ReadOnlyDurableCatalogState`, `DurableCatalogState`. Also `BootstrapArgs`, `Epoch`, `AuditLogIterator`, test helpers. |
| `config.rs` | 345 | `Config` and `StateConfig` — catalog open parameters (build info, env ID, replica size map, license key, feature flags, etc.). `ClusterReplicaSizeMap`. |
| `lib.rs` | 25 | Re-exports: `builtin`, `config`, `durable`, `expr_cache`, `memory`. Defines `SYSTEM_CONN_ID`. |

## Key concepts

- **Durable / in-memory seam.** `durable::objects` key-value types are internal; only their combined structs (e.g. `durable::Database`) are re-exported. `memory::objects` consumes these via `UpdateFrom<durable::*>` — the sole approved conversion path. Adapter consumers touch only `memory::objects` types.
- **`DurableCatalogState`** — single concrete Implementation: `PersistCatalogState` in `durable/persist.rs`. All catalog DDL goes through `Transaction::commit → TransactionBatch → compare_and_append`.
- **`BUILTINS_STATIC`** — dependency-ordered `LazyLock` of all system objects. Adding a builtin = add a `const` definition + insert into `BUILTINS_STATIC` in dependency order.
- **`ExpressionCache`** — separate persist shard; not part of the main catalog shard. Lifecycle is independent of `DurableCatalogState`.
- **Upgrade chain** — versioned and frozen. Current version: 81; minimum supported: 74. Each step isolated in its own submodule with frozen proto/serde types.

## Architecture notes

- `mz-catalog` is consumed almost exclusively by `mz-adapter` (the coordinator) and the `catalog-debug` CLI tool. It has no upward dependency on adapter logic.
- The main structural concern bubbled from `durable/`: `Transaction` is a 4,117-LOC god struct with 19 parallel `TableTransaction` fields and 103 public methods — adding a catalog collection requires changes at five sites. See `durable/ARCH_REVIEW.md` §1.
- `builtin.rs` at 14,931 LOC is large but homogeneous: it is a data file, not a logic file. All entries are `const` definitions plus one `LazyLock`. The deletion test holds — each `const` is a distinct system object. No deepening warranted.

## Cross-references

- Primary consumer: `mz-adapter` (coordinator `open_catalog`, `catalog_transact`).
- Persist integration: `mz-persist-client` via `durable/persist.rs`.
- Schema protos: `mz-catalog-protos` (owns `CATALOG_VERSION`).
- Generated developer docs: `doc/developer/generated/catalog/`.

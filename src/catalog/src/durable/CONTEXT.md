# catalog::durable

Persist-backed durable storage layer for the Materialize catalog.
Exposes two open/close traits and one primary mutation interface.

## Files and subdirectories (LOC ≈ 13,781 across this tree)

| Path | LOC | What it owns |
|---|---|---|
| `durable.rs` *(parent module file)* | 561 | Three public traits: `OpenableDurableCatalogState` (open/boot lifecycle), `ReadOnlyDurableCatalogState` (snapshots, audit iteration, subscriptions), `DurableCatalogState` (adds mutation). `AuditLogIterator`, `TestCatalogStateBuilder`, `persist_desc()`. |
| `transaction.rs` | 4,117 | `Transaction<'a>` — batches DDL mutations across 19 `TableTransaction` fields and commits atomically via `TransactionBatch → DurableCatalogState::commit_transaction`. `TableTransaction<K,V>` is defined here (private to this file). |
| `persist.rs` | 2,144 | `UnopenedPersistCatalogState` and `PersistCatalogState` — the sole concrete implementations of all three durable traits. `FenceableToken` state machine (`Initializing → Unfenced | Fenced`) detects concurrent writers via `compare_and_append`. Manages a single persist shard. |
| `objects.rs` | 1,586 | On-disk key-value split types (`DatabaseKey`/`DatabaseValue`, etc.) and `DurableType` trait for encode/decode. Public re-exports exposed via `crate::durable`. |
| `objects/serialization.rs` | 935 | Protobuf ↔ Rust conversions for all durable object types. |
| `objects/state_update.rs` | 936 | `StateUpdate` / `StateUpdateKind` pipeline from durable shard to in-memory incremental updates. |
| `initialize.rs` | 816 | `initialize()` — populates a brand-new catalog with all system defaults inside a single `Transaction`. Defines well-known config-collection key constants and `BootstrapArgs`. |
| `upgrade.rs` | 492 | `run_upgrade` chains version-step functions v74→v81 (`CATALOG_VERSION = 81`, `MIN_CATALOG_VERSION = 74`). Each step operates on frozen `objects_vN` types. `objects!` macro handles proto (v74-v78) vs. serde (v79+) snapshot format. |
| `upgrade/v{N}_to_v{N+1}.rs` | 1,865 total | 7 version-step modules (v74–v81); v78→v79 is the largest (868 LOC, proto→serde migration). |
| `debug.rs` | 457 | `DebugCatalogState` wrapping `UnopenedPersistCatalogState`; `Collection` trait + `CollectionTrace` / `Trace` types for manual catalog inspection and repair. |
| `metrics.rs` | 74 | Prometheus counters for durable operations. |
| `traits.rs` | 32 | `UpgradeFrom<T>` / `UpgradeInto<U>` — orphan-rule workarounds for protobuf conversions. |
| `error.rs` | 174 | `CatalogError`, `DurableCatalogError`, `FenceError`. |

## Key concepts

- **`OpenableDurableCatalogState`** — Interface for the open/boot lifecycle: epoch fencing, schema migration, and the transition to a fully open `DurableCatalogState`.
- **`DurableCatalogState`** — Full read-write Interface; single concrete Implementation: `PersistCatalogState`.
- **`Transaction`** — Primary mutation seam. All catalog DDL is expressed as mutations to one of 19 `TableTransaction` fields; the whole batch commits atomically as a `TransactionBatch`.
- **`TableTransaction<K,V>`** — Private in-file type; holds `initial: BTreeMap<K,V>` + `pending` updates with optional uniqueness enforcement. Defined in `transaction.rs:2925`.
- **`FenceableToken`** — Epoch-fencing state machine in `persist.rs:112`; detects concurrent writers by generation/epoch comparison on each `compare_and_append`.
- **Upgrade chain** — Steps are isolated in frozen-type submodules; adding a schema version = add one file + increment `CATALOG_VERSION` in `mz-catalog-protos`.

## Cross-references

- Consumers: `crate::memory::objects` (via `UpdateFrom<durable::*>` impls) and `mz-adapter` coordinator.
- Persist shard ID: `shard_id(env_id)` in `persist.rs` — deterministic mapping from environment ID.
- Generated developer docs: `doc/developer/generated/catalog/durable/`.

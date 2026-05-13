# src/ — Materialize Rust workspace

The Rust source tree for the Materialize database (≈ 695K LOC across 27
top-level crates ≥ 5K LOC each, plus several smaller crates). All crates
here are members of the workspace defined in `../Cargo.toml`.

## Crates by tier

### Process / coordinator

| Crate | LOC | Role |
|---|---|---|
| [`environmentd/`](environmentd/CONTEXT.md) | 28K | Process entry point: hosts pgwire, http, and the adapter coordinator. 0dt deployment protocol lives here. |
| [`adapter/`](adapter/CONTEXT.md) | 71K | Coordinator: catalog, sequencer, peek/subscribe orchestration, RBAC, statement logging. Single-threaded loop with `Staged` cooperative-multitasking machinery. |

### SQL frontend

| Crate | LOC | Role |
|---|---|---|
| [`sql-parser/`](sql-parser/CONTEXT.md) | 22K | SQL text → `AST<Raw>`. Hand-written recursive descent. Build-time visitor generation via `mz-walkabout`. |
| [`sql/`](sql/CONTEXT.md) | 66K | Two-phase compiler: async purify (Phase 1) + pure plan (Phase 2). `SessionCatalog` (67 methods, single impl in adapter) is the SQL↔catalog seam. Output is `Plan` (82 variants). |

### IR + optimizer

| Crate | LOC | Role |
|---|---|---|
| [`expr/`](expr/CONTEXT.md) | 37K | The IR hub: `MirRelationExpr`, `MirScalarExpr`. `mz-sql` writes it, `mz-transform` optimizes it, `mz-compute` executes it. Intentional no-`mz-sql` dep. `#[sqlfunc]` proc-macro defines builtins. |
| [`transform/`](transform/CONTEXT.md) | 24K | MIR optimizer: pipeline of `Transform` impls. Stateless w.r.t. catalog/compute — `IndexOracle` and `StatisticsOracle` injected from adapter. |
| [`compute-types/`](compute-types/CONTEXT.md) | 9K | LIR plan (post-optimization, pre-render). `Plan`, `RenderPlan`, `Interpreter` tagless-final encoding. |

### Foundational data layer

| Crate | LOC | Role |
|---|---|---|
| [`repr/`](repr/CONTEXT.md) | 37K | `Datum`, `Row`, `RelationDesc`, `ScalarType`. The dual-type `Sql*`/`Repr*` split is the planning↔execution type-system seam. |

### Catalog

| Crate | LOC | Role |
|---|---|---|
| [`catalog/`](catalog/CONTEXT.md) | 37K | Single source of truth for persistent catalog metadata. `UpdateFrom<durable::*>` is the in-memory↔durable boundary; adapter must not import `durable::objects` directly. |
| [`catalog-protos/`](catalog-protos/CONTEXT.md) | 31K | Frozen proto schemas + the catalog upgrade chain. `CATALOG_VERSION` lives here; MD5-guarded immutability for past versions. |

### Compute

| Crate | LOC | Role |
|---|---|---|
| [`compute/`](compute/CONTEXT.md) | 25K | The Timely/differential execution engine. Compute↔storage boundary is persist exclusively. `Worker::reconcile` is the compute-side complement to controller command history. |
| [`compute-client/`](compute-client/CONTEXT.md) | 10K | Client API to compute. `as_of_selection` is currently misplaced here (compute-bootstrap logic in protocol crate). |

### Storage

| Crate | LOC | Role |
|---|---|---|
| [`storage/`](storage/CONTEXT.md) | 38K | Self-contained timely cluster crate. `SourceRender` is the connector seam; `InternalStorageCommand` bus has timely ordering guarantees. `ENABLE_UPSERT_V2` migration in flight. |
| [`storage-controller/`](storage-controller/CONTEXT.md) | 9K | Controller for storage workers. `lib.rs` at 4K LOC is acknowledged "Leviathan" refactor target. Replication unsupported (database-issues#5051). |
| [`storage-client/`](storage-client/CONTEXT.md) | 7K | Central storage contract crate. `StorageCollectionsImpl` (3.3K LOC) lives here but is instantiated by combined `mz-controller` — an inversion that enables compute/storage sharing. |
| [`storage-types/`](storage-types/CONTEXT.md) | 15K | Shared source/sink/connection types. `AlterCompatible` is the primary correctness invariant for online schema changes. |

### Persistence

| Crate | LOC | Role |
|---|---|---|
| [`persist/`](persist/CONTEXT.md) | 9K | The `Blob`/`Consensus` seam definition + lower-level indexed log. `Determinate`/`Indeterminate` `ExternalError` split is the retry-protocol boundary. |
| [`persist-client/`](persist-client/CONTEXT.md) | 44K | High-level persist client. `PersistClientCache` is a process-scoped resource pool (do not instantiate twice). Schema evolution first-class via `compare_and_evolve_schema`. |
| [`persist-types/`](persist-types/CONTEXT.md) | 6K | Shared `Codec` trait + `stats/` (Seam to storage pushdown optimizer). |
| [`txn-wal/`](txn-wal/CONTEXT.md) | 7K | Atomic multi-shard txns over persist. One-shard linearization by design. `txns_progress` Timely operator is mandatory for any reader. |

### Operations / infra

| Crate | LOC | Role |
|---|---|---|
| [`orchestratord/`](orchestratord/CONTEXT.md) | 5K | Kubernetes operator binary. `generation.rs` (1.5K LOC) encodes per-minor-version K8s resource shapes as distinct structs. |
| [`cloud-resources/`](cloud-resources/CONTEXT.md) | 6K | K8s CRDs (`Materialize`, `Balancer`, `Console`, `VpcEndpoint`). `vpc-endpoints` feature flag is the cloud-vs-local seam. `CloudResourceController` impl lives in `mz-controller`. |
| [`frontegg-mock/`](frontegg-mock/CONTEXT.md) | 5K | Local mock of Frontegg auth. No trait seam between mock and real Frontegg — URL substitution only (latent divergence risk). |
| [`testdrive/`](testdrive/CONTEXT.md) | 9K | SQL-script integration test runner. `mz-adapter` + `mz-catalog` are direct prod deps (catalog consistency checks) — the tightest test↔prod coupling in the workspace. |
| [`avro/`](avro/CONTEXT.md) | 11K | Materialize fork of Apache Avro decoder. Schema resolution baked into `SchemaPiece` at parse time (perf-critical). |

### Standard library

| Crate | LOC | Role |
|---|---|---|
| [`ore/`](ore/CONTEXT.md) | 15K | Workspace stdlib. ~50 independent modules, no inter-module deps, feature-flag clean. Source of four workspace-wide seams (see below). |
| [`timely-util/`](timely-util/CONTEXT.md) | 6K | Timely-side helpers. `builder_async` is the async-operator seam consumed by compute and storage. `reclock.rs` carries formal math invariants. |

## Workspace-wide seams (load-bearing across many crates)

Pinning these here so future architectural reviews don't reinvent abstractions
that already exist:

- **`mz_ore::now::NowFn`** — single injectable clock; ~87 crates depend on it. Any new time abstraction must extend rather than parallel it.
- **`mz_ore::task::AbortOnDropHandle`** — RAII-structured Tokio concurrency convention; ~132 crates.
- **`mz_ore::soft_assert_*`** — production-safe assertion toggle via `MZ_SOFT_ASSERTIONS`; ~114 crates; operationally significant.
- **`mz_ore::metric::MetricsRegistry` + `DeleteOnDropWrapper`** — Prometheus seam; ~121 crates; the workspace standard for metric label lifecycle.
- **`mz_sql::catalog::SessionCatalog`** — SQL↔catalog seam (67 methods, single impl).
- **`mz_persist::location::{Blob, Consensus}`** — persistence-backend seam.
- **`mz_storage_types::controller::AlterCompatible`** — online schema-change correctness invariant.

## Cross-cutting friction (from per-crate ARCH_REVIEW.md files)

Surfaced findings worth tracking workspace-wide. Each is local in scope but
reflects recurring shapes:

| Pattern | Where | Reference |
|---|---|---|
| Parallel match-on-enum dispatch | `adapter/coord/sequencer/inner` (`Staged` impls); `adapter/coord` (`Message::*StageReady` variants) | `adapter/src/coord/sequencer/inner/ARCH_REVIEW.md`, `adapter/src/coord/sequencer/ARCH_REVIEW.md` |
| Single-file monolith ≥ 4K LOC | `sql/plan/statement/ddl.rs` (8K, 110 fns); `storage-controller/src/lib.rs` (4K, "Leviathan"); `mzcompose/composition.py` (1.9K) | sql, storage-controller, misc/python ARCH/CONTEXT |
| Two-implementation in-flight migration | `storage::upsert` v1/v2 (gated by `ENABLE_UPSERT_V2`); `compute::sink::correction` V1/V2 | storage, compute ARCH_REVIEW |
| Parallel-list dispatch | `parallel_workload/action.py` (~70 classes); `output_consistency/all_operations_provider.py`; `session/vars.rs` `SESSION_SYSTEM_VARS` | misc/python, sql ARCH/CONTEXT |
| God-struct with N parallel fields | `catalog::durable::transaction::Transaction` (19 fields, 103 methods, 4K LOC) | catalog ARCH_REVIEW |
| Wildcard match-arm correctness gap | `mz_sql_parser::WithOptionName::redact_value` (36 impls, many `_ => true`/`false`) — silent PII leak risk | sql-parser ARCH_REVIEW |
| Trait with one concrete impl | `compute-types::BottomUpTransform` (only `RelaxMustConsolidate`); `mz_sql::SessionCatalog` (only `ConnCatalog`) — first is candidate to inline; second is justified test seam | compute-types, sql ARCH_REVIEW |
| Cross-crate layer leak | `repr::OptimizerFeatures` (optimizer concept in data crate); `repr::explain::NamedPlan` requires `mz-sql-parser` build-heavy dep for one 6-variant enum | repr ARCH_REVIEW |

## See also

- Generated developer docs: `../doc/developer/generated/<crate>/_crate.md` and per-module files. These are the authoritative source for "what does this module do" — the per-crate `CONTEXT.md` files here lean on them.
- Design docs (functioning as ADRs): `../doc/developer/design/`.
- LOC accounting and tree topology: `../loc_report.md` (untracked) and `../loc_tree.py`.

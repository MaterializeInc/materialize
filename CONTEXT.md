# Materialize — repo CONTEXT

Materialize is a streaming database: PostgreSQL-compatible SQL surface, but
materialized views are kept incrementally up-to-date as their inputs change,
backed by Timely Dataflow + Differential Dataflow + persist (a shared,
strongly-consistent log over Blob+Consensus).

This repo holds the database (Rust), the Python tooling, the web console
(TypeScript), and the integration test suite. Total ≈ 908K LOC across four
top-level subtrees.

## Top-level subtrees

| Subtree | LOC | What it is |
|---|---|---|
| [`src/`](src/CONTEXT.md) | 695K | Rust workspace — 27 top-level crates ≥5K LOC each (the database itself). |
| [`misc/`](#misc) | 102K | Python tooling (test infrastructure, release automation, dbt adapter). |
| [`console/`](console/CONTEXT.md) | 54K | TypeScript/React web console (cloud + self-managed + impersonation deployment modes). |
| [`test/`](test/CONTEXT.md) | 53K | Integration tests across four frameworks (testdrive, sqllogictest, mzcompose, pytest). |

### misc/

| Subtree | LOC | What it is |
|---|---|---|
| [`misc/python/`](misc/python/CONTEXT.md) | 95K | The `materialize` Python package: mzcompose harness, platform-checks, output-consistency framework, parallel-workload, CLIs. |
| [`misc/dbt-materialize/`](misc/dbt-materialize/CONTEXT.md) | 6K | dbt adapter (PostgresAdapter inheritance chain) + blue-green deploy macros. |

## Core architecture (load-bearing)

These are the structural commitments worth knowing before touching any of
them. Each is documented in detail in its crate's `CONTEXT.md`.

1. **Single-threaded coordinator with `Staged` cooperative-multitasking.**
   `mz-adapter::Coordinator` runs all SQL command sequencing on one task;
   heavy work (optimizer, timestamp linearization, RTR) is moved off-thread
   via `StageResult::Handle` and re-enqueued on the message channel. This is
   the load-bearing concurrency model for the entire control plane.

2. **Two-phase SQL compiler.** `mz-sql` purifies (Phase 1, async, inlines
   external state) then plans (Phase 2, pure, sync). The contract to the
   adapter is the `Plan` enum (82 variants) consumed exclusively by
   `adapter::sequencer`.

3. **`mz-expr` as IR hub.** `mz-sql` writes it; `mz-transform` optimizes it;
   `mz-compute` executes it; `mz-adapter` evaluates `unmaterializable`
   functions. Intentional no-`mz-sql`-dep keeps the IR self-contained. The
   `#[sqlfunc]` proc-macro defines builtins.

4. **Compute↔storage boundary is persist exclusively.** Compute dataflows
   never read external sources directly; everything goes through
   `mz-persist-client`. `txn-wal` provides cross-shard atomic txns over
   persist.

5. **Catalog: `mz-catalog` is durable + in-memory.** `UpdateFrom<durable::*>`
   is the in-memory↔durable seam — adapter must not import
   `durable::objects` directly. `mz-catalog-protos` owns frozen proto
   schemas + the upgrade chain (`CATALOG_VERSION`).

6. **Process boundary: `mz-environmentd`.** Hosts pgwire, http, and the
   adapter coordinator. The 0dt deployment protocol (preflight +
   `DeploymentState` machine + internal HTTP promotion API) is unique to
   this crate.

## Workspace-wide seams

Pinned in [`src/CONTEXT.md`](src/CONTEXT.md):

- `mz_ore::now::NowFn` (~87 crates) — single clock seam.
- `mz_ore::task::AbortOnDropHandle` (~132 crates) — RAII Tokio convention.
- `mz_ore::soft_assert_*` (~114 crates) — production-safe assertions.
- `mz_ore::metric::MetricsRegistry` (~121 crates) — Prometheus seam.
- `mz_sql::catalog::SessionCatalog` — SQL↔catalog seam.
- `mz_persist::location::{Blob, Consensus}` — persistence-backend seam.
- `mz_storage_types::controller::AlterCompatible` — online schema-change
  correctness invariant.

## Architectural friction surfaced (workspace-level)

From the per-crate `ARCH_REVIEW.md` files. Each is local in scope but
reflects recurring shapes worth tracking:

- **Parallel match-on-enum dispatch** in `adapter::Staged` impls + 9
  `Message::*StageReady` variants ([adapter sequencer ARCH_REVIEW](src/adapter/src/coord/sequencer/ARCH_REVIEW.md)).
- **Single-file monoliths ≥4K LOC**: `sql/plan/statement/ddl.rs` (8K, 110
  fns); `storage-controller/src/lib.rs` (4K, "Leviathan");
  `mzcompose/composition.py` (1.9K).
- **Two-implementation in-flight migrations**: `storage::upsert` v1/v2
  (gated by `ENABLE_UPSERT_V2`); `compute::sink::correction` V1/V2.
- **Parallel-list dispatch**: `parallel_workload/action.py` (~70 classes);
  `output_consistency/all_operations_provider.py`; `session/vars.rs`
  `SESSION_SYSTEM_VARS`.
- **God-struct**: `catalog::durable::transaction::Transaction` (19 fields,
  103 methods, 4K LOC).
- **Wildcard match-arm correctness gap**: `mz_sql_parser::WithOptionName::redact_value`
  (36 impls, many `_ => true/false` defaults — silent PII leak risk on new
  variants).
- **Trait with one concrete impl**: `compute-types::BottomUpTransform`
  (only `RelaxMustConsolidate` — candidate to inline).
- **Cross-crate layer leak**: `repr::OptimizerFeatures` (optimizer concept
  in foundational data crate); `repr::explain::NamedPlan` requires
  `mz-sql-parser` build-heavy dep for one 6-variant enum.
- **Test↔prod coupling**: `mz-testdrive` directly imports `mz-adapter` +
  `mz-catalog` for `consistency.rs` catalog checks — the tightest
  test↔prod coupling in the workspace.
- **Generated API client churn**: console has dual SQL execution paths
  (`executeSql` v1 + `executeSqlV2`) with v1 marked for deprecation but
  ~15 remaining call sites.

## Round-2 cross-crate review

[`ARCH_REVIEW.md`](ARCH_REVIEW.md) at the repo root holds the round-2 findings:
patterns visible only when the per-crate lenses are combined (parallel
match-on-enum dispatch as a workspace habit; four concurrent v1/v2 migrations
with no shared forcing function; `mz-repr` hosting optimizer + parser
concepts; `mz-testdrive` importing production `mz-adapter`/`mz-catalog`
directly; cross-crate broken `#[deprecated]` in `mz-transform`).

## Navigation

- [`CONTEXT-MAP.md`](CONTEXT-MAP.md) — full index of all `CONTEXT.md` and
  `ARCH_REVIEW.md` files in the repo, organized by subtree.
- [`doc/developer/generated/`](doc/developer/generated/) — per-crate and
  per-module generated developer docs (the authoritative source for "what
  does this module do" — the per-dir `CONTEXT.md` files lean on them).
- [`doc/developer/design/`](doc/developer/design/) — design docs (function
  as ADRs).
- `loc_report.md` (untracked) and `loc_tree.py` — LOC accounting and tree
  topology scripts that drove this review.

## Methodology of this review

This `CONTEXT.md` and the file tree of nested `CONTEXT.md` / `ARCH_REVIEW.md`
files were produced by a single architecture-review pass over every directory
≥5K LOC (96 directories total) in DFS post-order, dispatching parallel
Sonnet subagents for each top-level Rust crate and the non-Rust subtrees,
with Opus serializing commits and writing the synthesis layers. Each commit
documents which subdirectories were reviewed since the previous commit; see
`git log --oneline | grep arch-review` for the chronology.

# Round-2 architecture review — cross-crate patterns

Round 1 scoped each reviewer to a single crate; local friction appeared reasonable in isolation. Round 2 looks across the 15 reviewed crates for patterns that only become visible when the lenses are combined. Every candidate below is verified by `rg` or `grep` before being stated.

---

## 1. Parallel `validity()`/`stage()` dispatch is a workspace-wide habit, not a local choice

**Where:**
- `src/adapter/src/coord/sequencer/inner/peek.rs:58` — `PeekStage` twin match arms (9 variants)
- `src/adapter/src/coord/sequencer/inner/subscribe.rs:46` — `SubscribeStage` twin match arms
- 7 more `impl Staged for XxxStage` in the same directory (create_index, create_materialized_view, create_view, secret, cluster, explain_timestamp, introspection) — verified at those paths
- `src/compute/src/sink/correction.rs:46-98` — `Correction<D>` shim with 5 identical `match self { V1(c) => c.method(..), V2(c) => c.method(..) }` arms
- `src/sql-parser/src/ast/defs/statement.rs` and `ddl.rs` — 36 `impl WithOptionName for XxxOptionName` blocks repeating the parallel `redact_value()` match pattern

**Problem:** Three separate crates each invented a "parallel match on enum variants" pattern where two predicates (`validity()`/`stage()`, V1/V2 method delegation, safe/unsafe redaction) are maintained in lockstep across enum arms. Each crate's reviewer judged its local choice reasonable; together they constitute a workspace-wide refactoring target.

**Solution:** In `adapter`, one type-erased `Message::StagedReady { stage: Box<dyn Staged> }` variant (10 files shrink to 0 for each new staged statement type). In `compute::correction`, introduce a `CorrectionBuffer<D>` trait with the 5 methods; the dispatch enum collapses to 0 lines. In `sql-parser`, replace wildcard `_ => true/false` arms with exhaustive matches (blocks compile-time enforcement of redaction decisions).

**Benefits:** Locality — each addition touches one struct, not three; Leverage — the driver (Staged loop, correction shim, redact trait) is written once; Test surface — each step is independently constructable.

---

## 2. Four in-flight v1/v2 migrations with no shared forcing function

**Where:**
- `src/storage/src/render/sources.rs:337` — `if ENABLE_UPSERT_V2 { upsert_v2(...) } else { upsert(...) }` (3,350 LOC across three modules)
- `src/compute/src/sink/correction.rs:59` — `if ENABLE_CORRECTION_V2.get(config) { ... }` (695+1,378 LOC in V1+V2)
- `console/src/api/materialize/executeSql.tsx` and `executeSqlV2.ts` — v2 docstring marks v1 deprecated pending issue #1176; ~15 v1 call sites remain
- `src/transform/src/lib.rs:736` — `#[deprecated = "Create an Optimize instance..."]` on `logical_optimizer`, still called live from `src/adapter/src/optimize.rs`

**Problem:** All four migrations are individually justified (capability upgrade, performance, API modernization). As a workspace pattern, none has a hard forcing function — completion is driven only by attention. The migrations accumulate: each is a parallel implementation that doubles the correctness-audit surface for bug fixes. The deprecated `logical_optimizer` case is the most acute: it is `#[deprecated]` in `mz-transform` but the compiler warning is suppressed by the `mz-adapter` call site (no `#[allow(deprecated)]` annotation was added deliberately, so the warning is silent).

**Solution:** For each migration: (a) file or reference a tracking ticket at the dyncfg/deprecated call site naming the v1-only capabilities that must be validated before removal (see storage ARCH_REVIEW checklist as the model); (b) add `#[allow(deprecated)]` with a ticket reference at the `logical_optimizer` call in `adapter/src/optimize.rs` so the suppression is explicit and searchable. This does not change behavior but makes the migration debt findable and auditable rather than invisible.

**Benefits:** Leverage — one grep for `#[allow(deprecated)]` or `ENABLE_*_V2` gives a complete in-flight migration inventory; Depth — when a v2 ships, deletion is driven by a pre-stated checklist not post-hoc archaeology.

---

## 3. `mz-repr` as an upward-leaking host for optimizer and parser concepts

**Where:**
- `src/repr/src/optimize.rs:21` — `OptimizerFeatures` struct (macro-generated, 20+ flags) imported by `mz-adapter` (15 files), `mz-expr`, `mz-catalog` — verified at those paths
- `src/repr/src/explain/tracing.rs:15` — `use mz_sql_parser::ast::NamedPlan;`; `NamedPlan` is a 6-variant enum describing optimizer plan stages; `mz-repr/Cargo.toml` carries `mz-sql-parser` as a dep for this one use

**Problem:** `mz-repr` is the foundational data layer; `mz-adapter`, `mz-compute`, and `mz-catalog` depend on it. Placing optimizer flag policy (`OptimizerFeatures`) here forces that policy to live *below* every layer that governs it. Adding a new flag grows a crate that should be stable. `NamedPlan` in `repr::explain::tracing` pulls in `mz-sql-parser` — a build-heavy dependency — for a 6-variant enum that has no intrinsic relationship to SQL parsing.

**Solution:** Move `NamedPlan` to `mz-repr::explain` (no new crate, no external dep). For `OptimizerFeatures`: defer migration until a `mz-compute-types` restructure is in flight (the mechanical cost is real — ~10 import sites), but record the intent.

**Benefits:** Seam — `mz-repr` reverts to a pure data layer; removing `mz-sql-parser` from its dep graph reduces build parallelism bottleneck for downstream crates.

---

## 4. `mz-testdrive` directly imports production `mz-adapter` and `mz-catalog`

**Where:**
- `src/testdrive/Cargo.toml` — `mz-adapter = { path = "../adapter" }` and `mz-catalog = { path = "../catalog" }` (verified)
- `src/testdrive/src/action/consistency.rs` — calls internal coordinator and catalog APIs for post-file invariant checks

**Problem:** This is the tightest test/prod coupling in the workspace. Any breaking internal change to `mz-adapter` or `mz-catalog` can fail `mz-testdrive` compilation independently of SQL-level semantics, creating a hidden build-graph dependency from the integration test runner into every crate that adapter or catalog touches. `mz-testdrive` is compiled as part of the test infrastructure, meaning the full adapter stack must build before any testdrive test can run.

**Solution:** Expose the consistency-check logic as a dedicated internal SQL function or HTTP endpoint on the Materialize process. Testdrive becomes a network client only. This is the same seam already used by every other testdrive action.

**Benefits:** Locality — testdrive's build graph is severed from the production compilation path; Depth — the consistency endpoint becomes testable via the SQL protocol like any other feature.

---

## 5. `mz-transform::Transform` trait — dead surface and broken deprecation across crate boundary

**Where:**
- `src/transform/src/lib.rs:213-264` — `Transform` trait: implementors write `actually_perform_transform`, callers invoke `transform`; `debug()` method unused
- `src/transform/src/lib.rs:736` — `#[deprecated]` on `logical_optimizer`
- `src/transform/src/ordering.rs:1-12` — empty `pub mod ordering` (12 lines, no types, no impls)
- `src/adapter/src/optimize.rs` — calls `Optimizer::logical_optimizer(ctx)` (verified)

**Problem:** Three separate dead-surface signals in one crate, but the most actionable is cross-crate: `logical_optimizer` is `#[deprecated]` in `mz-transform` yet still called from `mz-adapter`. The deprecation cannot be enforced (it lives in a different crate; rustc will emit a warning, but without an explicit `#[allow(deprecated)]` + ticket at the call site, the warning can be silently present or suppressed). `actually_perform_transform` is implementor-hostile: every new pass must know to implement a method whose name does not match the invocation name. `ordering.rs` is an empty `pub mod` that adds namespace noise without content.

**Solution:** (a) Rename `actually_perform_transform` → `apply` and seal `transform` as the instrumented wrapper — one mechanical rename across 52 `impl Transform` sites. (b) Complete the `logical_optimizer` deprecation by migrating the one `mz-adapter` call site. (c) Delete `ordering.rs` and its `pub mod` line — deletion test passes; no external references confirmed by `rg`.

**Benefits:** Interface clarity — the public name matches the call name; Leverage — one rename propagates to all 52 impls via `cargo fix`; the empty stub deletion is zero-cost cleanup.

---

## Honest skips

- **`catalog::durable::Transaction` god-struct (19 fields, 4K LOC)** — surfaced in round 1. The deletion test for a registry-based refactor passes structurally, but the `uniqueness_violation` closures and the `compare_and_append` commit path make the migration non-mechanical. This is a _local_ deepening opportunity in `mz-catalog`, not a cross-crate pattern; it does not recur in other crates, so it belongs in the per-crate ARCH_REVIEW rather than here.

- **`typeconv` cross-crate crate-placement (`mz-sql` vs `mz-expr`)** — the 7 `TODO? if typeconv was in expr` comments in `expr/impls/` look like a cross-crate friction point. Verified: moving `typeconv` into `mz-expr` would create an upward `mz-expr → mz-sql` dependency cycle (mz-sql already depends on mz-expr). The deletion test fails — the fix would require a third crate to host the shared metadata. The cost of the split exceeds the optimizer benefit of more precise `introduces_nulls` values. Skipped.

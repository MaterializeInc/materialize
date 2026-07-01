# M4: live integration of the equality-saturation pass

**Goal:**
Register `EqSatTransform` in the real optimizer pipeline behind a default-off
feature flag, so the e-graph pass (and its surviving `DeltaQuery` decision) runs
on real queries when enabled. Plumb the statistics oracle path. Promote the
arity `debug_assert` to a production-safe hard guard.

**Why a crate move is required:**
`logical_optimizer` lives in `mz-transform`. Registering `EqSatTransform` there
needs `mz-transform` to depend on `mz-transform-egraph`, but that crate already
depends on `mz-transform` (for the `Transform` trait), so it would be a cargo
lib cycle. The fix is to move the egraph engine into `mz-transform` as a module
(`mz_transform::eqsat`). This is also its permanent home.

## Global constraints

* No `unsafe`. No new `as` (use `mz_ore::cast`). `#[mz_ore::test]`. Copyright
  headers. `cargo fmt` (never `--check`). No em-dash/semicolon in comments.
* The flag defaults OFF. With it off, the pipeline is byte-identical to today.
* `EqSatTransform` must never panic or emit a wrong-arity plan in production:
  on any arity mismatch it leaves the relation unchanged and logs.
* Keep the differential harness at 3 wins / 4 losses / 13 ties.

## Phase 1: crate move (`src/transform-egraph` -> `mz_transform::eqsat`)

* `git mv` the 12 source files to `src/transform/src/eqsat/`, renaming
  `lib.rs` -> `mod.rs`. `git mv` `rules/` to `src/transform/src/eqsat/rules/`.
  `git mv` the three integration tests to `src/transform/tests/`.
* In every moved submodule file, rewrite intra-engine references
  `crate::X` -> `super::X` (the engine modules become siblings under `eqsat`).
  `mod.rs` keeps child-module references (`engine::`, `cost::`, ...) as-is.
* `mod.rs`: add inner attributes at the top:
  `#![allow(clippy::disallowed_types, clippy::disallowed_methods, clippy::as_conversions, missing_docs, missing_debug_implementations)]`
  (the ported engine uses std hash maps, `as`, and has sparse docs; the
  workspace lints are `warn`, so a module `allow` is sufficient). Fix the
  `include_str!` path to `"rules/relational.rewrite"`.
* `src/transform/src/lib.rs`: add `pub mod eqsat;`.
* `src/transform/Cargo.toml`: remove the `mz-transform-egraph` dev-dependency.
  Confirm `mz-expr`, `mz-repr`, `mz-ore`, and the dev-deps `mz-expr-test-util`
  are present (they are).
* Root `Cargo.toml`: remove the two `src/transform-egraph` workspace member
  entries. Delete `src/transform-egraph/`.
* Integration tests (`roundtrip.rs`, `compare_real.rs`, `wcoj_decision.rs`,
  `test_transforms.rs`): `mz_transform_egraph::` -> `mz_transform::eqsat::`.
* Verify: `cargo check -p mz-transform --all-targets`; `cargo nextest run -p
  mz-transform` green (the moved tests pass under the new paths); differential
  harness still 3/4/13.

## Phase 2: feature flag (default off)

* `src/repr/src/optimize.rs`: add `enable_eqsat_optimizer: bool,` to the
  `optimizer_feature_flags!` invocation (defaults to false via the macro).
* `src/sql/src/session/vars/definitions.rs`:
  * Add a system var: `{ name: enable_eqsat_optimizer, desc: "run the
    equality-saturation MIR optimizer pass", default: false,
    enable_for_item_parsing: false }`.
  * In `impl From<&SystemVars> for OptimizerFeatures`, add
    `enable_eqsat_optimizer: vars.enable_eqsat_optimizer(),`.
  * Add `enable_eqsat_optimizer` to the destructuring in
    `optimizer_features_no_enable_for_item_parsing` (and any `set_var!` list).
* Verify: `cargo check -p mz-repr -p mz-sql`.

## Phase 3: register `EqSatTransform` in `logical_optimizer`

* In `mz-transform`'s `logical_optimizer`, build the transform list mutably,
  inserting `Box::new(eqsat::EqSatTransform)` after the logical fixpoints and
  before the final `Typecheck`, guarded by
  `if ctx.features.enable_eqsat_optimizer`. Placing it late keeps logical passes
  from clobbering the synthesized `DeltaQuery`, and the final `Typecheck`
  validates the pass output. (Physical `JoinImplementation` then leaves the
  `DeltaQuery` tag unre-planned.)
* Verify: `cargo check -p mz-transform`; with the flag off, the list matches
  today plus a no-op branch.

## Phase 4: arity hard guard + statistics

* `eqsat::transform::EqSatTransform::actually_perform_transform`: capture the
  input arity; run `optimize`; if the output arity differs, restore the input
  (no-op), `soft_panic_or_log!` the mismatch, and return `Ok(())`. Never emit a
  wrong-arity plan, never panic in release. Remove the `debug_assert_eq!` in
  `eqsat::optimize` (the guard now lives at the transform boundary).
* Statistics: `TransformCtx` carries a `StatisticsOracle`; the engine's cost
  model is cardinality-free today, so thread the oracle no further than
  documenting that `optimize` ignores it for now (stats stay blank, the common
  case). No behavior change.
* Verify: a test that an arity-changing optimize is rejected (no-op) rather than
  panicking — exercised via a unit test on the transform with a stub, or
  documented if not cheaply testable.

## Phase 5: end-to-end test + lint

* Add an integration test: build the triangle, run the real `logical_optimizer`
  + `physical_optimizer` with `enable_eqsat_optimizer = true`, assert the output
  join is `DeltaQuery`; with the flag off, assert it is not (regression guard
  that the flag actually gates).
* `cargo nextest run -p mz-transform`; `cargo nextest run -p mz-repr -p mz-sql`
  (flag plumbing); `cargo clippy -p mz-transform -p mz-repr -p mz-sql`;
  `cargo fmt`; `bin/lint`.

## Out of scope

* Tuning where in the pipeline eqsat runs for best results (it is behind a
  default-off flag; placement is conservative).
* A cardinality-aware cost model that would consume the statistics oracle.
* Enabling the flag by default / LaunchDarkly rollout.

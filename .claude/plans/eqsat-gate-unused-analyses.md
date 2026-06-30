# Plan A: gate per-round analysis computation on ruleset cond-usage

Goal: stop recomputing e-class analyses that no active rule reads, every saturation
round. Profiling (post-matcher) shows the 4 analyses are 52-77% of the blowup;
`mono` and `cc` are read by no current rule, `cc`/ScalarEquiv has no cond at all.

## Which analysis each cond needs (from codegen.rs:289-303 + egraph cond helpers)
* `Cond::NonNegative` -> `nn`   (cond_non_negative -> an.nn)
* `Cond::IsUniqueKey` -> `keys` (cond_is_unique_key -> an.keys)
* `Cond::Monotonic`   -> `mono` (cond_monotonic -> an.mono)
* `Cond::Unsatisfiable` -> `eq` (always computed for phase 2a; not gated)
* `cc`/ScalarEquiv: no cond exists in the grammar -> never needed now.

## Files
1. `src/transform/src/eqsat/rules.rs`
   * Add `#[derive(Clone, Copy, Debug, Default)] pub(crate) struct AnalysisNeeds { pub nonneg: bool, pub keys: bool, pub monotonic: bool }`.
   * Add `pub(crate) needs: AnalysisNeeds` to `CompiledRule`.
   * Add `CompiledRuleSet::needed_analyses(&self) -> AnalysisNeeds` = OR-fold over `self.rules`.
2. `src/transform/build/codegen.rs` (`emit_compiled`)
   * Per rule, scan `r.conds` for `Cond::NonNegative` / `Cond::IsUniqueKey` / `Cond::Monotonic`.
   * Emit `needs: AnalysisNeeds { nonneg: <b>, keys: <b>, monotonic: <b> }` in the `CompiledRule { .. }` literal.
3. `src/transform/src/eqsat/egraph.rs` (`saturate`)
   * `let needs = rules.needed_analyses();` before the loop.
   * In the loop, compute each analysis conditionally; pass an empty `HashMap` when not needed:
     `nn = if needs.nonneg { run_analysis(NonNeg) } else { HashMap::new() }`, same for keys/mono.
   * Drop the `cc` `run_analysis` entirely (no consumer); pass `HashMap::new()` for `cc`.
   * `eq` unchanged (cached, always; phase 2a + Unsatisfiable read it).

## Soundness
Needs are derived from the SAME conds the codegen uses to emit `find`, so a skipped
analysis is never read. A wrongly-skipped analysis would only ever cause a
`.get(..).unwrap_or(false)` to return false -> rule does not fire -> miss-opt
(sound, never wrong). cc/ConstantColumns impl + tests stay (forward-looking).

## Verify
* `cargo nextest run -p mz-transform` green (117/117) - behavior unchanged (no rule reads mono/cc).
* `cargo run --release -p mz-transform --example eqsat_bench` - filter_over_union / mixed
  per-op should drop (analyses ~halved). Record before/after.
* No `.spec` golden change (output identical; only skipped dead computation).
* clippy/fmt clean.

## Commit
`transform/eqsat: skip per-round analyses no active rule reads`

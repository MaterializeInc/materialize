# SP2b Slice 7: Reroute to CombinedLang + delete standalone `ScalarEGraph` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the combined engine the production scalar canonicalizer and delete the dead standalone `ScalarEGraph`, completing SP2b.

**Architecture:** One-line engine swap at the single injection point `eqsat/scalar.rs:56` (`canonicalize` → `canonicalize_combined`, which already exists with a matching signature), verified while both engines coexist (commit A). Then mechanical deletion of the dead standalone driver plus repointing of the 13 differential-parity tests to frozen snapshots (commit B).

**Tech Stack:** Rust, `mz-transform` crate, sqllogictest.

## Global Constraints

- **Flag Option A:** keep `enable_eqsat_scalar_canonicalize` (default `true`). `true` → combined engine, `false` → `mz_expr` reduce kill-switch. Do NOT remove the flag (deferred follow-up).
- **Two commits, one branch, A before B. Do not squash A into B.** The A boundary is the neutrality proof.
- **Commit A must be neutrality-gated:** full sqllogictest, no `--rewrite`, zero golden diff, rows/errors unchanged, all 13 parity tests green. A golden move is spelling-only churn to be explained and certified rows/errors-neutral, never rubber-stamped. Zero-diff is expected.
- **Never run sqllogictest with `--rewrite`** (behavior-neutral requirement).
- **Keep the shared substrate** the combined engine depends on: `ScalarLang`, `SNode`, `ClassAnalysis`/`make`/`merge`, `snode_of`/`lower_into`, `scalar/lang.rs`. Delete only the standalone-only driver.
- **Keep `canonicalize_predicates`** (scalar.rs:56, the production entry) and **keep `eqsat::rules::scalar_all()` / `SCALAR_COMPILED_RULES`** (the combined DSL ruleset = production). Delete only standalone `canonicalize` (scalar.rs:36) and `scalar::rules::rules()`.
- **Permanent sorry count stays 11.** No new rules, no Lean changes.
- **Retire no parity test.** Repoint all 13 to frozen snapshots; the 2 `assert_ne` guard tests are already oracle-independent, keep them.
- `bin/fmt` + `cargo check --tests` clean before each commit. Stage only your own files (not `.superpowers/` scratch, not untracked design docs). Do not touch `doc/developer/generated/`.
- **Use the `mz-test` skill** for the exact test commands (`bin/sqllogictest --optimized`, `bin/cargo-test`, etc.).

---

### Task 1: Commit A — reroute production to the combined engine + verify

**Files:**
- Modify: `src/transform/src/eqsat/scalar.rs` (the injection-point closure)
- Modify: `src/repr/src/optimize.rs:156` (stale doc comment)
- Modify: `src/transform/src/eqsat/scalar_saturate.rs:14-17` (stale comment + `#![allow(dead_code)]` review)

**Interfaces:**
- Consumes: `crate::eqsat::scalar_saturate::canonicalize_combined(expr: &MirScalarExpr, col_types: &[ReprColumnType]) -> MirScalarExpr` (already exists, `pub(crate)`, `src/transform/src/eqsat/scalar_saturate.rs:182`).
- Produces: the same `canonicalize_predicates` production entry, unchanged signature; its `enable_eqsat_scalar` true-branch now runs the combined engine.

- [ ] **Step 1: Swap the closure to the combined engine.**

In `src/transform/src/eqsat/scalar.rs`, add the import near the existing `use crate::eqsat::scalar::egraph::ScalarEGraph;` (line 27):

```rust
use crate::eqsat::scalar_saturate::canonicalize_combined;
```

Then in `canonicalize_predicates` (line 61-68) change the closure body from the standalone `canonicalize` to `canonicalize_combined`:

```rust
    if enable_eqsat_scalar {
        mz_expr::canonicalize::canonicalize_predicates_with(
            predicates,
            col_types,
            Some(&|e: &mut MirScalarExpr, ct: &[ReprColumnType]| {
                *e = canonicalize_combined(e, ct);
            }),
        );
    } else {
        mz_expr::canonicalize::canonicalize_predicates(predicates, col_types);
    }
```

Leave the standalone `canonicalize` fn (line 36) in place — it is still the oracle for the 13 parity tests, which is what makes this commit verifiable. Leave the `ScalarEGraph` import in place (still used by that fn and the test block). Update the `canonicalize_predicates` doc comment's `[`canonicalize`]` reference (lines 47-48) to point at `canonicalize_combined`, since the true-branch now calls it.

- [ ] **Step 2: Fix the stale flag doc comment.**

In `src/repr/src/optimize.rs` around line 156, change the "Default off" comment on the `enable_eqsat_scalar_canonicalize` field to reflect the live default (`true`). Keep it factual and short, e.g. "Default on. Bound from `SystemVars::enable_eqsat_scalar_canonicalize`." Do not restructure the struct.

- [ ] **Step 3: Update the stale `scalar_saturate.rs` comment and review the dead-code allow.**

In `src/transform/src/eqsat/scalar_saturate.rs` (lines 14-17), the comment says production still routes through the old engine "until a later slice wires this in". That is now false. Replace it with a comment stating that `canonicalize_combined` is the production scalar canonicalizer (reached through `scalar::canonicalize_predicates` when `enable_eqsat_scalar_canonicalize` is set).

Then run `cargo check -p mz-transform` and check whether `#![allow(dead_code)]` (line 17) is still needed. If the crate is warning-clean without it, remove it. If removing it surfaces warnings from genuinely test-only support items in this file, keep it. Do not chase warnings in other files.

- [ ] **Step 4: `cargo check --tests` and eqsat unit tests.**

Run (mz-test skill for exact invocation):
```
bin/cargo-test -p mz-transform --no-run   # or cargo check --tests -p mz-transform
```
Expected: clean compile.

Then run the eqsat unit tests including all 13 parity tests:
```
bin/cargo-test -p mz-transform eqsat
```
Expected: all pass. The 13 `scalar_parity_*` tests (plus the 2 guard tests) still assert `combined == old_scalar` and must be green — this is the proof both engines agree through the reroute.

- [ ] **Step 5: Full sqllogictest, no `--rewrite`, zero golden diff (the neutrality gate).**

Run the full sqllogictest suite per the mz-test skill (`bin/sqllogictest --optimized -- <files>`), NO `--rewrite`. Expected: zero golden diff, rows/errors unchanged.

If any golden moves: it is spelling-only (both engines are sound e-graphs over the same cost model). Do NOT rewrite. Capture the diff, confirm every changed line is plan-text with rows/errors identical, and report it for certification as behavior-neutral churn. Do not proceed to commit until either zero-diff is confirmed or the churn is certified rows/errors-neutral.

- [ ] **Step 6: `bin/fmt` + commit A.**

```
bin/fmt
git add src/transform/src/eqsat/scalar.rs src/repr/src/optimize.rs src/transform/src/eqsat/scalar_saturate.rs
git commit -m "$(cat <<'EOF'
eqsat SP2b slice7: reroute production scalar canonicalize to CombinedLang

Swap the single injection point (scalar::canonicalize_predicates) from the
standalone ScalarEGraph to canonicalize_combined. The standalone engine stays
present as the parity oracle, so both engines coexist and the differential
tests still gate this reroute. Flag enable_eqsat_scalar_canonicalize (default
on) keeps the mz_expr reduce path as the off kill-switch.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01QNZHc5J4bdG29HzPbaCu7H
EOF
)"
```

Stage ONLY the three files above. Do not stage `.superpowers/` or untracked design docs.

---

### Task 2: Commit B — delete the dead standalone engine + repoint the 13 parity tests

**Files:**
- Modify: `src/transform/src/eqsat/scalar_saturate.rs` (repoint 13 parity tests, 36 oracle call sites)
- Delete: `src/transform/src/eqsat/scalar/egraph.rs`, `src/transform/src/eqsat/scalar/rules.rs`, `src/transform/src/eqsat/scalar/raise.rs`
- Modify: `src/transform/src/eqsat/scalar.rs` (strip standalone `canonicalize` + `mod egraph`/`mod rules` + `ScalarEGraph` import + test block; keep `canonicalize_predicates`)
- Modify: `src/transform/src/eqsat/scalar/node.rs`, `src/transform/src/eqsat/scalar/analysis.rs`, `src/transform/src/eqsat/scalar/lower.rs` (partial-strip, keep shared substrate)

**Interfaces:**
- Consumes: nothing new.
- Produces: a codebase where the only scalar engine is CombinedLang; `canonicalize_predicates`, `SNode`, `ClassAnalysis`/`make`/`merge`, `snode_of`/`lower_into`, `ScalarLang` all survive.

- [ ] **Step 1: Repoint the 13 parity tests to frozen snapshots (do this BEFORE deleting the oracle).**

In `src/transform/src/eqsat/scalar_saturate.rs`, the 13 oracle-comparison test fns are: `scalar_parity_not_not`, `scalar_parity_variadic`, `scalar_parity_if`, `scalar_parity_const_eval`, `scalar_parity_slice5`, `scalar_parity_slice6a`, `scalar_parity_slice6c`, `scalar_parity_slice6e`, `absorb_guard_blocks_dropped_extra_error`, `scalar_parity_slice6b`, `null_prop_variadic_guard_blocks_erroring_operand`, `scalar_parity_slice6d`, `scalar_parity_slice6f`. They hold 36 `crate::eqsat::scalar::canonicalize(...)` oracle calls total.

For each per-case oracle comparison, replace the oracle with a frozen expected value. Recipe (per case):
- Keep the input expr construction unchanged.
- Replace `let old = crate::eqsat::scalar::canonicalize(&input, &ct);` (and its `assert_eq!(combined, old)`) with an explicit `expected` built from the test's own helper constructors, representing the rule's documented rewrite, then `assert_eq!(combined, expected)`.
- The frozen value is `combined`'s current output, which equals `old` (tests are green now) which equals correct. Cross-check by running the test: `combined == expected` must pass.

Worked example (illustrative shape, adapt to the actual case): `scalar_parity_not_not` feeds `not(not(x))` and asserts the oracle equals `x`. The frozen form is `let expected = x_expr.clone();` then `assert_eq!(combined, expected);`.

For cases where the rewritten form is not obvious from the test comment, temporarily add `eprintln!("{combined:?}")`, run the single test to capture the exact structure, encode it as `expected` via the helper constructors, then remove the `eprintln!`. The reviewer will sanity-check a sample of frozen values against the documented rewrites — the frozen value must be the CORRECT form (which `combined == old` already proved), not merely whatever `combined` emitted.

The 2 `assert_ne` guard tests (`absorb_guard_blocks_dropped_extra_error`, `null_prop_variadic_guard_blocks_erroring_operand`) already assert an unsoundness is NOT produced. If they still carry a `canonicalize` oracle reference, repoint the positive `assert_eq` part and keep the `assert_ne` unsoundness check; confirm no `crate::eqsat::scalar::canonicalize` reference remains in either.

Also update: the `use crate::eqsat::scalar::analysis::{ClassAnalysis, make, merge};` import (line 26) stays; remove any now-unused `canonicalize` reference. The corpus fixture and the 20 survivor tests (`corpus_covers_*`, `*_terminates`, `canonicalize_combined_is_identity_without_rules`, `not_not_rewrites_via_combined`) are unchanged.

- [ ] **Step 2: Verify the repoint with the standalone engine still present.**

Run:
```
bin/cargo-test -p mz-transform eqsat
```
Expected: all pass, including the 13 repointed snapshots and 20 survivors. At this point the standalone engine still compiles and exists — this isolates "did the repoint preserve the assertions" from "did the deletion compile".

- [ ] **Step 3: Delete the standalone-only driver files.**

```
git rm src/transform/src/eqsat/scalar/egraph.rs \
       src/transform/src/eqsat/scalar/rules.rs \
       src/transform/src/eqsat/scalar/raise.rs
```

- [ ] **Step 4: Strip `scalar.rs` to the production entry only.**

In `src/transform/src/eqsat/scalar.rs`:
- Remove `pub mod egraph;` (line 17) and `pub mod rules;` (line 22).
- Remove `use crate::eqsat::scalar::egraph::ScalarEGraph;` (line 27).
- Remove the standalone `canonicalize` fn (lines 29-42, including its doc comment).
- Remove the `#[cfg(test)] mod tests` block (line 74 onward) that round-trips through the standalone engine.
- KEEP `canonicalize_predicates` (line 56) and its now-`canonicalize_combined`-referencing doc comment. Keep `pub mod analysis; pub mod lang; pub mod lower; pub mod node;`.
- Update the module `//!` doc (lines 6-14) if it references the deleted `egraph`/`raise`/rules phases — trim it to describe the surviving shared substrate plus the `canonicalize_predicates` injection point. Keep it accurate to the post-deletion file.

- [ ] **Step 5: Partial-strip the shared-substrate files.**

- `src/transform/src/eqsat/scalar/node.rs`: keep `SNode`. Fix the `use crate::eqsat::scalar::egraph::Id;` import (line 18) — `Id` now comes from `crate::eqsat::core::Id` (confirm the correct path via the combined-engine imports, e.g. `scalar_saturate.rs:22` uses `crate::eqsat::core::Id`).
- `src/transform/src/eqsat/scalar/lower.rs`: keep `snode_of` and `lower_into`. Remove `pub fn lower()` (the standalone lower, lines 62-65). Fix the `use crate::eqsat::scalar::egraph::{Id, ScalarEGraph};` import (line 16) to drop `ScalarEGraph` and source `Id` from `core`.
- `src/transform/src/eqsat/scalar/analysis.rs`: keep `ClassAnalysis`/`make`/`merge`. Fix the `Id` import (line 17) and the dead doc-link to `ScalarEGraph` (line 20). Rewrite or remove the `#[cfg(test)] mod tests` block (lines 143-356) that depends on `ScalarEGraph::new()` and the deleted `eg.analysis(id)` inherent method. If the analysis logic is still worth unit-testing, port the tests to build a `CombinedLang` `EGraph` (mirror how `scalar_saturate.rs`'s `*_terminates` tests construct one); if that is disproportionate, remove the block and note that `scalar_saturate.rs`'s parity + termination tests now cover the analysis. State which you chose in the report.
- `src/transform/src/eqsat/scalar/lang.rs`: no edit (self-contained shared type).

- [ ] **Step 6: `cargo check --tests` + ALL-IMPLS sweep + re-grep stray callers.**

```
cargo check --tests -p mz-transform
```
Expected: clean. A missed partial-strip fails under `--tests` (not bare `check`), so use `--tests`.

Grep to confirm zero stray references to the deleted items:
```
grep -rn "ScalarEGraph\|scalar::rules::rules\|scalar::egraph\|scalar::raise\|scalar::canonicalize\b" src/ --include=*.rs
```
Expected: no hits outside comments/design-doc prose. In particular no `crate::eqsat::scalar::canonicalize(` remains (the wrapper `canonicalize_predicates` is a different symbol and stays). Fix stale doc references in `literal_constraints.rs:638`, `fusion/filter.rs:87,211` if they name the deleted `scalar::rules::tests::...` or `canonicalize`.

- [ ] **Step 7: Full eqsat tests + full sqllogictest + Lean/permanent-count check.**

```
bin/cargo-test -p mz-transform eqsat
bin/sqllogictest --optimized -- <full suite>    # NO --rewrite
```
Expected: eqsat all green (13 repointed + 20 survivors), slt green. Permanent sorry count stays 11 (no Lean files changed — confirm the Lean sources and `ci/test/lean-mir-rewrite.sh` `expected_permanent` are untouched by this slice; do not run the Docker lake build unless a Lean file changed, which it should not).

- [ ] **Step 8: `bin/lint` + `bin/fmt` + commit B.**

```
bin/lint
bin/fmt
git add -u src/transform/src/eqsat/ src/transform/src/eqsat/scalar.rs
git add src/transform/src/literal_constraints.rs src/transform/src/fusion/filter.rs   # only if doc refs fixed
git status   # confirm the 3 deleted files are staged as deletions, only your files staged
git commit -m "$(cat <<'EOF'
eqsat SP2b slice7: delete standalone ScalarEGraph, repoint parity tests

The combined engine is production (slice7 commit A). Delete the now-dead
standalone driver (scalar/egraph.rs, rules.rs, raise.rs) and strip scalar.rs to
the canonicalize_predicates entry, keeping the shared substrate (SNode,
ClassAnalysis, lower_into, ScalarLang) the combined engine uses. Repoint the 13
differential-parity tests from the deleted oracle to frozen snapshots so the
adversarial scalar-rule coverage survives. Completes SP2b: one CombinedLang
engine, all 20 rules ported.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01QNZHc5J4bdG29HzPbaCu7H
EOF
)"
```

Confirm exactly two commits on top of BASE `065aa871bc` (A then B), A not squashed into B.

---

## Self-Review

- **Spec coverage:** Task 1 = commit A (reroute + neutrality gate + 3 housekeeping edits). Task 2 = commit B (repoint 13 + delete driver + strip substrate + gates). Both spec commits covered.
- **Keep-vs-delete:** plan keeps `canonicalize_predicates`, `scalar_all()`/`SCALAR_COMPILED_RULES`, `ScalarLang`/`SNode`/`ClassAnalysis`/`lower_into`/`snode_of`/`lang.rs`; deletes standalone `canonicalize`, `egraph.rs`/`rules.rs`/`raise.rs`, `rules()`, `lower()`. Matches spec.
- **Ordering:** repoint (Task 2 Step 1-2) precedes deletion (Step 3-5) so tests compile at every check; deletion gated by `--tests`.
- **Type consistency:** `Id` re-sourced from `crate::eqsat::core::Id` in node/lower/analysis after `egraph.rs` (which re-exported it) is deleted — flagged in Steps 5-6.
- **Permanent 11 / no Lean change:** asserted in Step 7 and Global Constraints.

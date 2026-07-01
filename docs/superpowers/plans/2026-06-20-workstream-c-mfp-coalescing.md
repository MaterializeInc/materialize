# Workstream C: MFP coalescing at raise time implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make eqsat raise emit Map/Filter/Project runs in `MapFilterProject`-canonical form, reusing the production `CanonicalizeMfp` machinery, so the pipeline's `CanonicalizeMfp` becomes subsumable.

**Architecture:** After the eqsat engine extracts a plan and `raise` reconstructs a `MirRelationExpr`, adjacent Map/Filter/Project operators can sit in arbitrary order (the e-graph fusion rules reduce but do not canonicalize the run into Map-then-Filter-then-Project order). This workstream adds a bottom-up coalescing pass over the raised expression that extracts each maximal Map/Filter/Project run into a `MapFilterProject`, optimizes it, and re-emits it in canonical order, reusing `MapFilterProject::extract_non_errors_from_expr_mut`, `MapFilterProject::optimize`, and `CanonicalizeMfp::rebuild_mfp`.

**Tech Stack:** Rust, `mz_transform::eqsat`, `mz_expr::MapFilterProject`, `mz_transform::canonicalize_mfp::CanonicalizeMfp`.

## Global Constraints

* No `as`/as_conversions; use `mz_ore::cast`.
* No `unsafe` without `SAFETY`.
* No em-dashes or structuring semicolons in comments.
* No vendor names in user-facing surfaces. Never drop existing comments.
* Reuse the production MFP machinery; do NOT reimplement MFP extraction or canonical re-emission.
* `cargo fmt`; `bin/lint`; `cargo clippy -p mz-transform` before commit (ignore `buf`/`trufflehog` env failures).
* `doc/developer/generated/` is read-only.
* `enable_eqsat_optimizer` is default ON, so this changes plans across the SLT corpus. Do NOT mass-rewrite SLT; verify targeted files and let CI surface churn. Rewrite the eqsat `test_transforms` datadriven expectations (those are the workstream's own tests).
* Commit footer:
  ```
  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_016CwwG1wCgqf3v8g6uhf1Nx
  ```

## Reuse path (from the exploration, all public)

* `MapFilterProject::extract_non_errors_from_expr_mut(relation: &mut MirRelationExpr) -> MapFilterProject` (`src/expr/src/linear.rs:374`) peels the maximal error-free Map/Filter/Project run from the root, leaving `relation` at the non-MFP base.
* `MapFilterProject::optimize(&mut self)` (`linear.rs:977`) canonicalizes: CSE/memoize, sort+dedup predicates, inline singletons, prune unreferenced.
* `CanonicalizeMfp::rebuild_mfp(mfp: MapFilterProject, relation: &mut MirRelationExpr)` (`src/transform/src/canonicalize_mfp.rs:80`) re-emits the optimized MFP in canonical Map-then-Filter-then-Project order on top of `relation`, skipping no-op layers and calling `fusion::filter::Filter::action` on the filter node.

The exact per-node sequence (mirrors `CanonicalizeMfp::action`): `let mfp = MapFilterProject::extract_non_errors_from_expr_mut(node); /* optimize */ mfp.optimize(); CanonicalizeMfp::rebuild_mfp(mfp, node);`. For a non-Map/Filter/Project root the extracted MFP is the identity and `rebuild_mfp` is a no-op.

## Soundness / scope notes

* The coalescing operates only on Map/Filter/Project; it never touches Join implementations, so it cannot violate the logical-phase "joins Unimplemented" contract or disturb a committed `DeltaQuery` (those are Join nodes; the MFP run stops at them).
* `extract_non_errors_from_expr_mut` stops at any Map/Filter containing a literal error, so error semantics are preserved.
* Run the coalescing bottom-up over the final raised expression so every maximal run is canonicalized.

## File structure

* `src/transform/src/eqsat/raise.rs` — add the bottom-up coalescing pass and apply it to the raise output (Task 1).
* `src/transform/tests/test_transforms/eqsat.spec` — rewritten expectations (Task 1).
* `docs/superpowers/specs/2026-06-19-mir-egraph-status.md` — coverage matrix (Task 2).

---

### Task 1: Coalesce Map/Filter/Project at raise time

**Files:**
- Modify: `src/transform/src/eqsat/raise.rs` (add `coalesce_mfp`; apply it at the top-level raise entry)
- Test: `src/transform/src/eqsat/raise.rs` (tests module)
- Test: `src/transform/tests/test_transforms/eqsat.spec`

**Interfaces:**
- Produces: a bottom-up `fn coalesce_mfp(expr: &mut MirRelationExpr)` applied to the result of the existing `raise`. The public `raise(rel, commit_wcoj)` signature is unchanged; coalescing is applied to its output.

- [ ] **Step 1: Write the failing test**

In the `raise.rs` tests module, add a test that builds a `Rel` raising to a non-canonical run (e.g. a `Filter` above a `Map` above a `Filter`, or a `Map` above a `Filter` where the canonical form is Map-then-Filter-then-Project) and asserts the raised+coalesced output is in canonical order (Project on top, then Filter, then Map, then base), with adjacent same-type operators fused. Concretely, assert that raising a plan equivalent to `filter(p1, map(s, filter(p2, base)))` yields the canonical `project? / filter / map / base` shape (a single Filter and single Map, not two Filters). The binding criterion: after `raise` + coalesce, there is at most one contiguous Map, one Filter, one Project per run, in Map-Filter-Project order.

(The implementer constructs the exact `Rel`/`MirRelationExpr` and the precise structural assertion; the criterion above is binding.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p mz-transform --lib eqsat::raise`
Expected: FAIL (raise currently emits the nested non-canonical chain verbatim).

- [ ] **Step 3: Implement `coalesce_mfp`**

Add to `raise.rs` a bottom-up pass that, at each node, recurses into children first, then applies the reuse trio to the node. Reuse the production machinery; do not reimplement MFP logic. Sketch:

```rust
use mz_expr::MapFilterProject;
use crate::canonicalize_mfp::CanonicalizeMfp;

/// Coalesce each maximal Map/Filter/Project run in `expr` into a canonical
/// MapFilterProject, bottom up. Reuses the production extraction, MFP
/// optimization, and canonical re-emission, so eqsat output matches what
/// `CanonicalizeMfp` produces. Runs only on Map/Filter/Project, so it never
/// touches Join implementations.
fn coalesce_mfp(expr: &mut MirRelationExpr) {
    // Recurse first so inner runs are canonical before the outer extraction.
    let mut todo: Vec<&mut MirRelationExpr> = expr.children_mut().collect();
    while let Some(child) = todo.pop() {
        coalesce_mfp(child);
    }
    // Peel the maximal error-free Map/Filter/Project run at this node, optimize
    // it, and re-emit it canonically on top of the bare base.
    let mut mfp = MapFilterProject::extract_non_errors_from_expr_mut(expr);
    mfp.optimize();
    CanonicalizeMfp::rebuild_mfp(mfp, expr);
}
```

Determine the correct child-traversal idiom for `MirRelationExpr` (it has a visitor / `children_mut`-style API; check how other transforms recurse, for example `canonicalize_mfp.rs` itself, and match that idiom exactly rather than the sketch above if `children_mut` is not the real method). If `CanonicalizeMfp::action` is itself `pub` and applies exactly this trio per node within a standard visitor, prefer calling that visitor over hand-rolling the recursion.

Apply `coalesce_mfp` to the raise output at the single top-level entry point (where `EqSatTransform`/`optimize`/`optimize_logical` call `raise`), NOT inside the recursive `raise` arms (so it runs once over the finished tree). If raise has a single public entry, wrap it; if multiple callers, add the call at each (`optimize` and `optimize_logical`).

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p mz-transform --lib eqsat::raise`
Expected: PASS, including existing round-trip tests. NOTE: existing round-trip tests assert `raise(lower(x)) == x`; coalescing may canonicalize `x` if `x` had a non-canonical M/F/P run. Bare-column round-trip tests are unaffected, but if any existing round-trip test uses a multi-operator M/F/P input, update its expectation to the canonical form (the round-trip is now semantics-preserving and MFP-canonicalizing, like the scalar-canonicalizing change in workstream B; update the contract comment accordingly).

- [ ] **Step 5: Verify the logical-phase join contract and WCOJ path**

Run: `cargo test -p mz-transform --test wcoj_decision`
Expected: PASS. Confirm coalescing did not disturb the joins-Unimplemented contract (logical) or the offline DeltaQuery commit (the MFP run stops at joins, so it should not).

- [ ] **Step 6: Regenerate datadriven expectations**

Run: `REWRITE=1 cargo test -p mz-transform --test test_transforms` then `cargo test -p mz-transform --test test_transforms`
Expected: the `eqsat.spec` Map/Filter/Project cases now show canonical MFP output. Review the diff: confirm each change is a canonical re-ordering/fusion (Map-Filter-Project), not a semantic change. Update case comments where the output shape changed.

- [ ] **Step 7: Spot-check an SLT file for correctness**

Run: `bin/sqllogictest --optimized -- test/sqllogictest/arithmetic.slt`
Expected: all queries pass. Do not rewrite. If any query returns a wrong RESULT (not just a different plan), stop and report a regression.

- [ ] **Step 8: Commit**

```bash
cargo fmt
bin/lint
cargo clippy -p mz-transform
git add src/transform/src/eqsat/raise.rs src/transform/tests/test_transforms/eqsat.spec
git commit -m "eqsat: coalesce Map/Filter/Project into canonical MFP at raise

Run a bottom-up MapFilterProject coalescing pass over the raised plan,
reusing extract_non_errors_from_expr_mut, MapFilterProject::optimize, and
CanonicalizeMfp::rebuild_mfp, so eqsat output is MFP-canonical."
```

---

### Task 2: Coverage matrix and measure

**Files:**
- Modify: `docs/superpowers/specs/2026-06-19-mir-egraph-status.md`

**Interfaces:**
- Consumes: Task 1. Documentation + measurement only.

- [ ] **Step 1: Update the coverage matrix**

Move `CanonicalizeMfp` from Irreducible/Missing to **Covered** (mechanism: raise-time MFP coalescing reusing the production extract/optimize/rebuild trio). Note that `LiteralLifting` is now **Partial** (MFP `optimize` performs the literal-lifting that part of `LiteralLifting` does, but cross-operator literal lifting remains out of scope). Keep claims evidence-based.

- [ ] **Step 2: Note the deletion-phase implication**

Add a bullet under the roadmap deletion phases noting that workstream C supplies the `CanonicalizeMfp` capability for deletion phase 2 (the `logical_cleanup_pass` MFP canonicalization can be deleted once SLT parity with the flag on is confirmed).

- [ ] **Step 3: Measure**

Run: `cargo test -p mz-transform --test compare_real -- --nocapture` and record the actual win/loss/tie counts in the STATUS doc. Record the truth; if unchanged from 3w/0l/17t, say so.

- [ ] **Step 4: Commit**

```bash
cargo fmt
bin/lint
git add docs/superpowers/specs/2026-06-19-mir-egraph-status.md
git commit -m "eqsat: document MFP coalescing coverage (CanonicalizeMfp)"
```

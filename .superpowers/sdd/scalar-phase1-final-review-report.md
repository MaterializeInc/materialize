# Phase 1 final whole-branch review (scalar eqsat canonicalizer)

Reviewer: agent final-review-p1 (opus). Scope: BASE 09b1728df2 (end of Phase 0)
.. HEAD 7d21177a7d (last code commit). Package:
.superpowers/sdd/final-review-phase1-scalar.diff.

**Verdict: Ready to merge as a foundation = WITH FIXES. No Critical issues.**

## Strengths (key)
- Core soundness holds end-to-end: every union joins proven-equal terms; raise
  extracts an arbitrary min-cost representative, so extraction soundness reduces
  to per-union soundness. Right architecture for a non-destructive canonicalizer;
  structurally avoids the CLU-137 class.
- could_error is a genuine over-approximation across add/union/rebuild; rebuild
  least-fixpoint (egraph.rs:245-289) cannot under-approximate even with fold
  self-cycles. Verified against the real eval machinery, not just the design doc.
- The deliberate could_error-GATED deviation from reduce (null/err-prop) is sound:
  the eager dispatch tuple try_from_iter (repr/scalar.rs:2134-2166) evaluates all
  operands then surfaces the first operand-order error, confirming error wins over
  null. Gates require every OTHER operand could_error==false.
- propagates_nulls gate is load-bearing: all hand-written LazyVariadicFunc (And,
  Or, Coalesce, ErrorIfNull, Greatest, Least) have propagates_nulls()==false, so
  any func passing the gate uses eager all-operand dispatch. No LazyBinaryFunc, so
  ungated err_prop_binary (matching reduce) is sound.
- De Morgan is cost-neutral (only wins via not_not collapse); re-derived {T,F,N,E}
  table against max-error + short-circuit masking, no divergence.

## Issues

### Critical
None. Actively searched for an unsound rule interaction during saturation (a union
raising a child's error-capability + a later same-pass rule reading stale-low
could_error); does not exist (union ORs the merged root's analysis immediately, and
a parent's stale analysis stays a sound over-approx). No panic path in raise; no
non-termination (MAX_ITERS/MAX_ENODES bound; De Morgan has no reverse rule).

### Important (Should Fix)
1. NO phase-level differential test (canonicalize-vs-eval / vs-reduce over a
   corpus). Per-rule tests cannot catch an emergent bad interaction over arbitrary
   nested expressions. Add a randomized/property differential (bounded random
   MirScalarExpr over a few columns + literals + errors; assert
   eval(canonicalize(e))==eval(e) over a datum cube incl error rows) BEFORE Phase 3
   wiring. Highest-value addition; should not slip past Phase 2.
2. Three hand-inlined copies of the union-find find loop (egraph.rs:137-141,
   269-273, public find ~106). Extract free fn find_in(uf:&[Id], id:Id)->Id to
   collapse all three and shrink the bug surface of a subtle hot routine. (1a
   deferred Minor; with three copies, treat as should-fix.)

### Minor
- merge silently resolves a literal conflict via a.literal.or(b.literal)
  (analysis.rs:111). Add a debug_assert! that if both Some they agree.
- lit_bool / lit_bool_or_null / is_literal_null docs (rules.rs:69,393,498)
  overclaim multi-datum handling: unpack_first never checks for extra datums.
  Harmless (scalar literals single-datum) but the comment overstates.
- call_scalar_type clones each raised child (rules.rs:461); use into_iter (style).
- raise invoked inside saturation via call_scalar_type/if_err_cond
  (rules.rs:461,530). Gated/bounded, but a full extraction DP mid-saturation is a
  latent cost cliff for large predicates in Phase 3. Add NOTE:, revisit on real
  workloads.
- raise.rs cycle guard uses Vec::contains (raise.rs:61), O(depth^2). Trivial for
  scalar depths (P0 deferred Minor).

## Deferred-Minor triage
- P0 raise visiting Vec->HashSet: keep-deferred (perf).
- P0 canonicalize col_types arg: RESOLVED in 1e (egraph.rs:170 with_col_types).
- P0 add/union lazy-rebuild doc gap: keep-deferred (now adequately documented).
- 1a find_in helper: FIX-BEFORE-MERGE (lean) - three copies now.
- 1b class_ids rebuild-precondition doc: keep-deferred.
- 1b seed-fixpoint O(passes*nodes): keep-deferred (corpus small).
- 1e call_scalar_type clones: keep-deferred (style).
- 1e redundant matches! before assert_eq: keep-deferred.
- 1e null_prop_binary double-null comment: resolved-enough.
- 1f test comment c0==5: resolved.
- 1g test_if_non_err_cond_not_affected uses if(c0,c1,c1): FIX-BEFORE-MERGE (lean) -
  add if(c0,c1,c2) distinct-branch case so the "if_err_cond did not fire"
  assertion cannot be satisfied by if_same_branches collapsing the term.
- 1g assert err==DivisionByZero in type-union test: keep-deferred.
- 1g doc cite reduce/if_then.rs path: keep-deferred.
- 1i test_not_demorgan_soundness near-vacuous alone: keep-deferred (firing proven
  by companion test).
- 1i not_demorgan doc "left-to-right eval": keep-deferred but RECOMMENDED - precise
  reason is the order-independent max over operand errors (variadic.rs:89); tighten
  rules.rs doc to prevent a future mis-port.

## Recommendations
1. Before Phase 3 wiring, add the randomized canonicalize-vs-eval differential
   (Important #1) - guards rule-interaction regressions as Phase 2 adds
   undistribute/absorption.
2. Extract find_in(uf,id), dedup the three find loops.
3. Tighten De Morgan doc to the max-error rationale; add the merge literal-conflict
   debug_assert.
4. Re-run the full mz-transform suite on HEAD to confirm 257 pass on the integrated
   branch (reviewer read test sources but did not execute).

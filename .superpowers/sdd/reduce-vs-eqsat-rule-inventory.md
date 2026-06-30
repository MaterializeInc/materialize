# Inventory: MirScalarExpr::reduce rewrites vs eqsat scalar rules

Maps every `reduce` scalar rewrite to its status in the equality-saturation
scalar canonicalizer (`src/transform/src/eqsat/scalar/rules.rs`). Status:
DONE (ported), DEFERRED (intentionally postponed, noted in code/ledger), MISSING
(not yet considered). This predicts where flag-on plans diverge from reduce: a
MISSING simplification means eqsat leaves a form reduce would simplify.

eqsat rules() as of HEAD: const_fold, and_or_dedup, and_or_single, and_or_empty,
and_or_short_circuit, and_or_drop_unit, flatten_assoc, factor_and_or,
absorb_and_or, not_not, not_binary_negate, not_demorgan, if_true,
if_false_or_null, if_same_branches, if_err_cond, null_prop_binary,
null_prop_variadic, err_prop_binary, err_prop_variadic, isnull_fold. Plus
operand ordering at raise (raise.rs sorts And/Or operands).

## Boolean / AND-OR (the CLU-137 core) — fully ported

| reduce rewrite | source | eqsat | notes |
|---|---|---|---|
| constant folding (all-literal eval) | reduce_post per-arm | DONE const_fold | excludes nothing extra; reduce excludes Panic in unary |
| null propagation (propagates_nulls + literal null) | binary.rs/variadic.rs | DONE null_prop_binary/variadic | eqsat could_error-GATED (stricter, exact-eval) |
| error propagation (literal error operand) | binary.rs/variadic.rs | DONE err_prop_binary/variadic | eqsat could_error-GATED |
| AND/OR dedup / single / empty / zero(short-circuit) / drop-unit | scalar.rs reduce_and_canonicalize_and_or | DONE and_or_* | |
| AND/OR operand sort | scalar.rs:805 exprs.sort() | DONE (raise-time) | sort at extraction, not a rule |
| flatten associative AND/OR | scalar.rs flatten_associative | DONE flatten_assoc | |
| Not(Not x) -> x | reduce_pre | DONE not_not | |
| Not(a bf b) -> a negate(bf) b | reduce_pre | DONE not_binary_negate | |
| De Morgan Not(AND/OR) | scalar.rs demorgans | DONE not_demorgan | |
| factoring (a∧b)∨(a∧c) -> a∧(b∨c) | undistribute_and_or | DONE factor_and_or | residual-error gated |
| absorption a∨(a∧c) -> a | undistribute_and_or | DONE absorb_and_or | dropped-operand gated |
| If true/false/null cond | if_then.rs reduce_if | DONE if_true/if_false_or_null | |
| If then==els | if_then.rs | DONE if_same_branches | |
| If err cond | if_then.rs | DONE if_err_cond | |
| IsNull(non-nullable) -> false | reduce_pre | DONE isnull_fold | could_error-gated |

## DEFERRED (intentionally postponed; noted in code/ledger)

| reduce rewrite | source | why deferred |
|---|---|---|
| subset factoring (undistribute from a SUBSET of outer operands) | undistribute_and_or else-branch (indexes_to_undistribute) | full-intersection covers CLU-137; subset adds complexity |
| decompose_is_null (IsNull(a op b) -> IsNull(a) OR IsNull(b)) | reduce_pre IsNull else-arm | needs per-func null decomposition; non-nullable fold was the gap that mattered |
| nullability gate disjuncts (factor/absorb "operand non-nullable") | n/a | needs a nullability e-class analysis; residual-error gates suffice for the repro |

## MISSING (not yet ported) — likely sources of flag-on plan divergence

| reduce rewrite | source | likely corpus impact |
|---|---|---|
| binary identity elimination (x+0->x, x*1->x, etc.: literal operand makes call identity) | reduce/binary.rs:55-64,466-486 | HIGH: very common arithmetic; eqsat leaves x+0 unsimplified |
| Coalesce simplification (drop nulls, dedup, single-arg no-op, all-null->null) | reduce/variadic.rs:232-264 | HIGH: Coalesce common; null_prop does NOT fire (Coalesce propagates_nulls=false) |
| involution / inverse elimination f(g(x))->x (~(~x), reverse(reverse), numeric -(-x), bijective cast roundtrips) | reduce/unary.rs:31-138 | MED: needs preserves_uniqueness + infallible+propagates_nulls metadata gate |
| If with boolean then/els -> AND/OR (4 cases, with IS NULL guards) | reduce/if_then.rs:47-83 | MED: common CASE/boolean patterns; eqsat leaves If form |
| If-distribution: IF c THEN x=y ELSE x=z -> x = IF c THEN y ELSE z | reduce/if_then.rs:90-100 | LOW-MED: function distribution, metadata-inspecting |
| binary per-function precompiles (timezone literal, etc.) | reduce/binary.rs:64-202 | LOW: function-specific, niche |
| record-equality decomposition: Literal([..]) = record_create(..) -> field-wise eq | reduce/binary.rs:315-356 | LOW: enables literal_constraints on record fields; niche but a real consumer hook |
| RecordGet(i)(RecordCreate(args)) -> args[i] | reduce/unary.rs:64-83 | LOW: niche |
| ListIndex(ListCreate, literal) -> element | reduce/variadic.rs:157-164 | LOW: niche |
| regexp_replace null handling precompile | reduce/variadic.rs:105-130 | LOW: niche |

## Reading

- The boolean/AND-OR layer (the CLU-137 target and the soundness-hard part) is
  COMPLETE. The MISSING items are mostly function-specific algebraic identities
  and niche constructors, which the design called the "optional later expansion"
  (boolean+null is where the soundness wins concentrate).
- The HIGH-impact gaps (binary identity elimination, Coalesce simplification) are
  the most likely to show up as flag-on EXPLAIN golden diffs in the corpus run:
  eqsat will leave `x + 0`, `coalesce(x, x)`, etc. in forms reduce simplifies.
  These are candidate next rules if golden churn or plan-quality regressions
  warrant them. None are soundness issues (eqsat just simplifies less); they are
  plan-quality / golden-parity gaps.
- The empty-Filter regression (fixed) was one instance of this class (missing
  IsNull fold). Expect the corpus survey to surface more of the same shape from
  the HIGH/MED MISSING rows.

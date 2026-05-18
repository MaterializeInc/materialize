# Lean 4 semantics skeleton

A mechanized model of Materialize's scalar evaluation semantics.

This directory contains the v1 skeleton accompanying the error-handling design document at `../design/20260517_error_handling_semantics.md`.
The goal of the skeleton is not to mechanize all of `MirScalarExpr`.
The goal is to lock in the boolean truth tables for `AND` and `OR` over the four-valued logic `{TRUE, FALSE, NULL, ERROR}` and provide a place to grow from.

## What is here

* `Mz/Datum.lean`: `Datum` (`.bool`, `.int`, `.null`, `.err`), `EvalError` (`.placeholder`, `.divisionByZero`), and the `Datum.IsErr` predicate.
* `Mz/Expr.lean`: `Expr` inductive — literals, columns, binary `and`/`or`, `not`, `ifThen`, the list-carrying constructors `andN`, `orN`, `coalesce`, the binary integer arithmetic constructors `plus`, `minus`, `times`, `divide`, and the binary comparison constructors `eq`, `lt`.
* `Mz/PrimEval.lean`: primitive evaluators on `Datum` and `List Datum` — `evalAnd`, `evalOr`, `evalNot`, `evalIfThen`, `Env`, `Env.get`, `evalAndN`, `evalOrN`, `evalCoalesce`, the integer arithmetic primitives `evalPlus`, `evalMinus`, `evalTimes`, `evalDivide`, and the comparison primitives `evalEq`, `evalLt`. Split out so the algebraic-law files and the expression-level evaluator can both import them without circular dependencies. Division strict on `.err` and `.null`; a `.int n / .int 0` divisor produces `.err .divisionByZero` — the canonical cell-scoped error. Comparison is err-strict and null-strict; mixed-type operands route to `.null` (the skeleton does not model SQL implicit casts).
* `Mz/Eval.lean`: the big-step `eval : Env → Expr → Datum`. List-carrying constructors evaluate each operand and hand the result list to the matching primitive.
* `Mz/Boolean.lean`: per-cell truth-table proofs for `AND`, `OR`, and `NOT`, plus involutivity of `NOT`.
* `Mz/MightError.lean`: the `Expr.might_error` static analyzer, the `Env.ErrFree` predicate, and the `might_error_sound` theorem. Binary `AND` / `OR` short-circuit on literal-`.bool false` / literal-`.bool true` operands via `Expr.isLitBoolFalse` / `Expr.isLitBoolTrue`: either position being the absorbing literal makes the analyzer return `false` regardless of the other operand. The same short-circuit fires on variadic `andN` / `orN` when any operand is the absorbing literal. `IfThen` likewise short-circuits when the condition is a literal `.bool` — only the picked branch's analyzer result is consulted, so a known-erring branch on the discarded side cannot taint the result.
  `.divide a b` reduces to `a.might_error` when `b` is a literal nonzero int (`Expr.divisorIsSafe`) — the divisor cannot trigger divide-by-zero, so the operator inherits errors only from the dividend. Falls back to `true` when the divisor is unknown or a literal zero. `andN` and `orN` recurse via `Expr.argsMightError` ("any operand might error"); `coalesce` recurses via `Expr.argsAllMightError` ("every operand might error"), special-casing the empty list as safe. Soundness for `coalesce` extracts a statically-safe operand through `Expr.exists_safe_of_not_argsAllMightError` and applies `evalCoalesce_not_err_of_some_safe`, which in turn rests on `Coalesce.go_not_err` — the state-machine lemma that "once one safe operand is in the remaining list, the walk cannot return an error". Companion value-level helpers `evalAnd_{left,right}_false` / `evalOr_{left,right}_true` discharge the short-circuit branches of soundness.
* `Mz/Strict.lean`: strictness predicates — payload-preserving (`ErrStrictUnary`, `ErrStrictBinary`, `NullStrictUnary`) and weaker propagation forms (`ErrPropagatingBinary`, `NullPropagatingBinary`) that match the four-valued lattice's `err > null > int` absorption order. Positive instances for `evalNot` and the condition slot of `evalIfThen`; closure under composition; arithmetic instances (`evalPlus`, `evalMinus`, `evalTimes`, `evalDivide` all err-propagating and null-propagating in both positions); comparison instances (`evalEq`, `evalLt` same); negative results witnessing that `AND` and `OR` are *not* err-strict in either position.
* `Mz/Coalesce.lean`: laws for `evalCoalesce` — error-rescue, null-beats-err tiebreak, first-error stickiness.
* `Mz/Laws.lean`: algebraic laws — two-sided identity (`TRUE` for `AND`, `FALSE` for `OR`), idempotence (unconditional), commutativity (conditional on error-freedom of operands), and `Expr`-level reorder safety as a corollary of soundness.
* `Mz/Variadic.lean`: laws for `evalAndN` and `evalOrN` over `List Datum` — cons recurrence, nil, singleton, binary equivalence with the binary evaluators, and `FALSE`/`TRUE` absorption.
* `Mz/ExprVariadic.lean`: `Expr`-level reduction lemmas connecting `eval env (.andN args)` / `.orN` / `.coalesce` to their primitive counterparts, identity / singleton / binary-equivalence corollaries lifted through `eval`, and variadic-absorption theorems — a single operand evaluating to `FALSE` (resp. `TRUE`) makes the whole `andN` (resp. `orN`) evaluate to `FALSE` (resp. `TRUE`).
* `Mz/Bag.lean`: bag semantics on `List Row`. Defines `filterRel` and `project`, with filter idempotence, filter commutativity, projection length-preservation, and the empty-projection equation. Plain `filterRel` silently drops `err` rows; `Mz/ErrStream.lean` adds the explicit data/error stream pair.
* `Mz/ErrStream.lean`: the dataflow-style `BagStream = (data, errors)` pair.
  `BagStream.filter` routes erroring rows into the error collection instead of dropping them, with idempotence proved at both the data and the error level.
  `BagStream.filter_comm_data` proves the data-side commutativity unconditionally; `BagStream.filter_comm_no_err` strengthens to full stream equality when neither predicate errors on the input data.
  `BagStream.project_filter_pushdown_data` lifts `filterRel_pushdown_project` to the data side of `BagStream`: filtering after projecting agrees with substituting through the projection and filtering before projecting. Holds unconditionally on the data field; the errors collection diverges between the two orderings (filter sees projected vs unprojected rows) and is out of scope for this theorem.
  `BagStream.project` projects each row through a list of scalars; a row stays in the data collection only when every scalar succeeds, otherwise its err payloads (one per erroring scalar) are appended to the error collection.
  `rowErrs_nil_of_all_safe` and `projectErrs_eq_nil_of_all_safe` show that when no projection errs, `BagStream.project` does not extend the error collection.
* `Mz/Pushdown.lean`: substitution (`Expr.subst`) plus the headline `eval_subst` theorem (substituting then evaluating against the original row equals evaluating against the projected row), and the relational predicate-pushdown rewrite `filterRel p (project es rel) = project es (filterRel (p.subst es) rel)`.
* `Mz/ColRefs.lean`: column-reference analyzer and rewriter.
  `Expr.colReferencesBoundedBy n e` returns `true` when every `col i` in `e` has `i < n`. Mutually defined with `Expr.argsColRefBoundedBy` so structural recursion handles the nested-list constructors. Headline theorem `eval_append_left_of_bounded`: when a predicate's column references are bounded by `l.length`, evaluating against `l ++ r` agrees with evaluating against `l` alone — the foundation for pushing a single-side filter through a join's `cross` (where the joined env is `l ++ r`).
  `Expr.colShift k e` adds `k` to every column reference, leaving other constructors structurally intact. Right-side analogue of the bounded analyzer: it realigns a predicate written against the right schema with the joined env `l ++ r` (where the right side starts at index `l.length`). Headline `eval_append_right_shift`: `eval (l ++ r) (e.colShift l.length) = eval r e`.
  `Expr.colReferencesBoundedBy_mono` (mutual with `Expr.argsColRefBoundedBy_mono`) lifts a tight bound to a coarser one — useful when a predicate's natural bound is a single relation's width but the proof site needs the joined-env width. Convenience `eval_append_left_of_bounded_at` removes the requirement that the predicate's bound match `l.length` exactly: any `n ≤ l.length` suffices.
  `Expr.colShift_zero` (identity at `k = 0`) and `Expr.colShift_add` (`(e.colShift k).colShift m = e.colShift (k + m)`) give the shift its monoid laws. Useful for nested joins where each layer adds its own offset to the predicate's column references.
  Supporting environment lemmas: `Env.get_append_left` (read from prefix) and `Env.get_append_right` (read from suffix with index shift).
* `Mz/DiffSemiring.lean`: `DiffWithError α` — the diff-field type extension that encodes global (collection-scoped) errors as an absorbing element. Provides `+`, `*`, `-`, `0`, `1` instances over an arbitrary base diff and proves the absorption / commutativity / associativity / distributivity / negation laws that downstream operators must respect. Negation laws (`neg_error`, `neg_val`, `neg_neg_val`, `val_add_neg_val`) carry the principle that `.error` is unrecoverable — a collection-scoped error cannot be subtracted away. The `_int` specializations (`add_comm_int`, `add_assoc_int`, `mul_assoc_int`, `mul_comm_int`, `mul_add_int`, `neg_neg_int`, `val_add_neg_val_int`) discharge the base hypotheses at `Int` so downstream code in `Mz/Join.lean`, `Mz/UnifiedConsolidate.lean`, and `Mz/SetOps.lean` can cite the named laws directly.
* `Mz/UnifiedStream.lean`: unified diff-aware alternative to `BagStream`. `UnifiedStream := List (UnifiedRow × DiffWithError Int)` pairs a carrier (data row or row-scoped err) with a differential-dataflow diff augmented by the absorbing `error` element. Row-scoped errors flow through the carrier; collection-scoped errors flow through diff multiplication / addition. `ofBag` / `split` conversions assign every bag record a diff of `.val 1`; the round-trip theorem `split (ofBag s) = s` holds. The cross-direction is exact only up to multiset equality on `List EvalError` and is lossy for diffs ≠ `.val 1` (split drops diff information).
  `UnifiedStream.project` lifts `BagStream.project` to the diff-aware carrier. Records with `.error` diff or `.err` carrier pass through unchanged; a `.row r` record with `.val n` diff is evaluated against `es` — if every scalar succeeds, the row is emitted with diff `.val n`; if any scalar errs, one `(.err e, .val n)` is emitted per erroring scalar (each preserving the original multiplicity). Theorems: `project_preserves_error_diff` (an `.error` diff in the input always reaches the output), `project_no_error` (all-`.val` inputs yield all-`.val` outputs), `project_nil_es` (empty projection list collapses every row to width-zero), `project_nil_stream` (empty stream is empty).
* `Mz/Aggregate.lean`: aggregate reductions over `List Datum`. `aggCountNonNull` for `COUNT(expr)`. `aggStrict` for `SUM`/`MIN`/`MAX`-style aggregates that propagate `err` (first one in scan order wins) and skip `NULL`s. `aggTry` for the proposed `try_sum`/`try_min`/`try_max` variants that swallow `err` into `NULL` instead of propagating, defined as a post-pass on `aggStrict`. Theorems: `aggStrict_err` (any `err` input → `err` output), `aggStrict_no_err` (no-err inputs + no-err reducer → no-err output), `aggTry_no_err` (the non-strict variant never errors), and `aggTry_eq_aggStrict_of_no_err` (strict and non-strict agree on error-free inputs).
* `Mz/Consolidate.lean`: per-key diff summation over `List (DiffWithError α)`. The headline `sumAll_eq_error_of_mem` proves that an `error` diff anywhere in the list absorbs the consolidated sum to `error`, which is the property a differential dataflow `compact` operator cites when propagating global errors through consolidation. Companion `sumAll_val_of_all_val` says an all-`val` list sums to `val` of some base value.
* `Mz/TimedConsolidate.lean`: per-`(row, time)` consolidation. `TimedUnifiedStream := List (UnifiedRow × Nat × DiffWithError Int)` carries records with time. `atTime t` projects to one time slice (dropping the time component); `consolidateAtTime t` chains it with `UnifiedStream.consolidate`. Theorems: `consolidateAtTime_preserves_error` (an `.error` diff at time `t` survives both filter and consolidation), `atTime_length_le` and `consolidateAtTime_length_le` (both non-expanding). Decomposes the joint key into "filter by time, then consolidate by row".
* `Mz/UnifiedConsolidate.lean`: row-keyed diff summation on `UnifiedStream`. `UnifiedStream.consolidate` buckets records by carrier (via `DecidableEq UnifiedRow`) and sums per-bucket diffs. Theorems cover three properties:
  *absorption* — `consolidate_preserves_error` proves an `.error` diff anywhere in the input gives an `.error` diff in the consolidated output for that carrier;
  *cardinality* — `consolidate_length_le` bounds the output by the input length (consolidation only merges, never expands);
  *no-error preservation* — `consolidate_no_error` proves that if every input diff is a `.val`, every output diff is a `.val`, so `.error` is the only source of absorption.
* `Mz/Triple.lean`: collection-wide and per-time *flat* consolidation views on `TimedUnifiedStream`. `consolidateAll` sums every diff in the stream; `consolidateAtTimeFlat t` sums every diff at time `t`. Both ignore the carrier — they collapse a time slice (or the whole stream) to one `DiffWithError Int`. Absorption: `consolidateAll_eq_error_of_mem` and `consolidateAtTimeFlat_eq_error_of_mem`. Complementary to `Mz/TimedConsolidate.lean`'s `consolidateAtTime t`, which buckets per `(row, time)` and returns a `UnifiedStream`.
* `Mz/Join.lean`: relational joins on the diff-aware `UnifiedStream`. `cross` is the cartesian product — carriers combine via `combineCarrier` (rows concatenate; err on either side wins, left first), diffs multiply through `DiffWithError`'s `Mul` instance. A `.error` diff on either input therefore absorbs to `.error` on the output via `DiffWithError.error_mul_{left,right}`. `join pred l r` filters the product through a join predicate. Theorems: `cross_length` (`l.length * r.length`), `filter_length_le` (filter is non-expanding), `join_length_le` (corollary). Diff-propagation theorems: `cross_diff_error_{left,right}` (a `.error` diff on either side propagates through every output record), `filter_preserves_error_diff` (a record carrying `.error` diff is never dropped by `filter` — the absorbing marker cannot be filtered away). No-error preservation: `cross_no_error` and `filter_no_error` prove that all-`.val` input diffs yield all-`.val` output diffs, so `.error` is the only source of absorbing diffs in the joint output. Algebraic laws: `combineCarrier_assoc` (carrier combine is associative modulo `List.append_assoc`) and the headline `UnifiedStream.cross_assoc` (`(a × b) × c = a × (b × c)`). The proof rearranges nested `flatMap` / `map` via local list-monad lemmas and closes via `DiffWithError.mul_assoc` plus `combineCarrier_assoc`.
* `Mz/SetOps.lean`: set operations on `UnifiedStream`. `unionAll = (++)` concatenates two streams record-wise; theorems cover length (sum), associativity, nil identities, and error / no-error preservation from each input (`unionAll_preserves_error_diff_left`, `unionAll_preserves_error_diff_right`, `unionAll_no_error`). `union = consolidate ∘ unionAll` derives the set-semantics flavor; theorems lift the consolidation guarantees to `union` (`union_length_le`, `union_preserves_error_diff_left`, `union_preserves_error_diff_right`, `union_no_error`).
  `negate` negates every diff (`.error` absorbs negation, `.val n` becomes `.val (-n)`). Theorems: `negate_length` (length preserved), `negate_negate` (involution), `negate_preserves_error_diff`, `negate_no_error`.
  `exceptAll l r = consolidate (unionAll l (negate r))` realizes the signed-diff `EXCEPT ALL` (output diffs may be negative, encoding "this carrier has `n` fewer copies in the result than in the input"). Theorems: `exceptAll_length_le` (≤ sum of input lengths), `exceptAll_preserves_error_diff_left`/`exceptAll_preserves_error_diff_right` (errors from either side survive — negation absorbs at `.error`), `exceptAll_no_error`.
  `clampPositive` drops records with `.val n` where `n ≤ 0`, keeping `.error` records and records with `.val n > 0`. Theorems: `clampPositive_length_le`, `clampPositive_preserves_error_diff`, `clampPositive_only_positive` (every output `.val` is strictly positive).
  `bagExceptAll = clampPositive ∘ exceptAll` realizes the bag-semantics `EXCEPT ALL` — the signed-diff result is post-processed to drop non-positive multiplicities, producing `max(L - R, 0)` per carrier. Theorems lift the signed flavor: `bagExceptAll_length_le`, `bagExceptAll_preserves_error_diff_left`/`_right`, `bagExceptAll_only_positive`.
  `clampToOne` collapses surviving multiplicities to one: `.val n > 0` becomes `.val 1`, non-positive `.val` is dropped, `.error` survives. Defined by structural recursion on the list. Theorems: `clampToOne_length_le`, `clampToOne_preserves_error_diff`, `clampToOne_only_one_or_error` (every output diff is `.val 1` or `.error`).
  `distinct = clampToOne ∘ consolidate` realizes SQL `DISTINCT`: each distinct carrier appears at most once with multiplicity one (or `.error` if a collection-scoped error existed). Theorems: `distinct_length_le`, `distinct_preserves_error_diff`, `distinct_only_one_or_error`.
  `INTERSECT ALL` requires a per-carrier `min` combinator not yet exposed by `DiffWithError`. Deferred.
* `Mz/GroupBy.lean`: two grouping primitives.
  `groupBy keyExpr rel` partitions a relation by evaluated key using Lean's derived `DecidableEq Datum` — two `Datum.err e` keys with the same payload collapse into one group.
  `groupByErrDistinct keyExpr rel` uses the spec-faithful `Datum.groupKeyEq`, which returns `false` whenever either side is `.err`, so every err key produces its own singleton group.
  Theorem `insertIntoDistinct_err` proves the err-key insertion always appends a fresh group; `groupByErrDistinct_length_of_all_err` derives the consequence — every err-keyed row contributes one output group.
  Headline cardinality `totalRows_groupBy` (and its err-distinct variant `totalRows_groupByErrDistinct`) state that the sum of group sizes equals the input relation's length — no row is lost or duplicated by partitioning.
  Agreement theorem `groupByErrDistinct_eq_groupBy_of_no_err` proves the two variants coincide when no row's key evaluates to `.err`. Supporting invariants `insertInto_preserves_non_err_keys` and `groupBy_keys_non_err` thread the "no err keys in the accumulator" property through the foldr.
  Companion `aggregateBy` / `aggregateByErrDistinct` run `aggStrict` per group, modeling `SELECT keyExpr, AGG(valExpr) ... GROUP BY keyExpr`.

## What is not here

* No bag semantics, joins, aggregates, or relational operators.
* No bridge to the Rust evaluator.
  The model and the runtime are independent; divergences are caught by review, not by tooling.
* No Mathlib dependency.
  The skeleton is pure core Lean 4 to keep build time small and bootstrap simple.

## Build

```
cd doc/developer/semantics
lake build
```

Toolchain is pinned in `lean-toolchain`.
CI uses the local `Dockerfile` in this directory, which installs elan and reads the same pin via the `LEAN_TOOLCHAIN` build arg.
The elan toolchain used by local developers and the toolchain baked into the CI image therefore stay in lockstep.
Buildkite runs `ci/test/lean-semantics.sh` on every PR that touches `doc/developer/semantics/` (see the `lean-semantics` step in `ci/test/pipeline.template.yml`).

To reproduce the CI build locally:

```
./ci/test/lean-semantics.sh
```

To run `lake build` directly outside Docker, install elan via the standard instructions at `https://lean-lang.org/` and run `lake build` in this directory.

## Workflow

When a semantic rule changes, the change must land in two places.

1. Update `Mz/Eval.lean` to reflect the new operational behavior.
2. Update the corresponding theorem in `Mz/Boolean.lean` (or add a new one).

A semantic change without a Lean diff is incomplete.
A Lean diff without a corresponding Rust diff in `src/expr/` is a spec change that has not yet shipped.
Reviewers should expect both sides of the change in the same PR.

## Next steps

The diff-semiring extension is in scope: `UnifiedStream` records carry `(UnifiedRow × DiffWithError Int)` and `filter`, `cross`, `join`, `consolidate`, `consolidateAtTime` preserve / multiply / absorb the diff per the semiring laws.

### Blocked

* Full `BagStream.filter` commutativity (no preconditions). The error field requires a notion of multiset equality on `List EvalError`, and even multiset equality fails when both predicates err on the same row — `(p err, q err)` rows record different err payloads depending on filter order. The data side is closed by `filter_comm_data`; the precondition variant by `filter_comm_no_err`.
* Errors-side `BagStream.project` / `BagStream.filter` pushdown. The two orderings collect predicate errors from different row sets; even multiset equality fails when `rowAllSafe` filtering removes rows the predicate would have visited. The data side is closed by `project_filter_pushdown_data`.

### Additive refinements

* Tightening `Expr.might_error` further. Short-circuit detection covers binary / variadic `AND` / `OR` and `IfThen` against literal absorbers. Remaining: ground-truth lookups (literal arithmetic, known-null operands, type-driven), all additive against the current soundness proof.
* Strict cardinality bound for `UnifiedConsolidate`: when a carrier appears `k > 1` times in the input, the output is `k - 1` shorter than the input.
* `UnifiedStream.project` pushdown analogue of `BagStream.project_filter_pushdown_data`. Predicate pushdown across the diff-aware projection is straightforward on the carrier side but the err-split adds asymmetries: filter-after-project sees projected rows; filter-before-project (with substitution) sees originals.
* Lift `ErrPropagatingBinary` / `NullPropagatingBinary` to `Expr` form. Today's predicates work at the `Datum` level; an `Expr.err_propagating` analogue would let the optimizer reason about whole sub-expressions, not just primitives.

### Material expansions

* `INTERSECT ALL` on `UnifiedStream`: requires a per-carrier `min` combinator over `DiffWithError Int`. The combinator is not derivable from `+`, `*`, `-` alone — landing it requires either a new diff primitive or a bucketing operator that materializes per-carrier multiplicities from both inputs.
* `distinct` is in scope; remaining: stronger correctness theorems (idempotence `distinct ∘ distinct = distinct`, agreement with the carrier-set view, no-error preservation on `.val` inputs).
* Cross-link the spec doc (`../design/20260517_error_handling_semantics.md`) to specific theorem names via `[Mz/...:thm]` cross-references.

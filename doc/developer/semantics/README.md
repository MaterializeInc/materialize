# Lean 4 semantics skeleton

A mechanized model of Materialize's scalar evaluation semantics.

This directory contains the v1 skeleton accompanying the error-handling design document at `../design/20260517_error_handling_semantics.md`.
The goal of the skeleton is not to mechanize all of `MirScalarExpr`.
The goal is to lock in the boolean truth tables for `AND` and `OR` over the four-valued logic `{TRUE, FALSE, NULL, ERROR}` and provide a place to grow from.

## What is here

* `Mz/Datum.lean`: `Datum`, `EvalError`, and the `Datum.IsErr` predicate.
* `Mz/Expr.lean`: `Expr` inductive — literals, columns, binary `and`/`or`, `not`, `ifThen`, plus the list-carrying constructors `andN`, `orN`, and `coalesce`.
* `Mz/PrimEval.lean`: primitive evaluators on `Datum` and `List Datum` — `evalAnd`, `evalOr`, `evalNot`, `evalIfThen`, `Env`, `Env.get`, `evalAndN`, `evalOrN`, `evalCoalesce`. Split out so the algebraic-law files and the expression-level evaluator can both import them without circular dependencies.
* `Mz/Eval.lean`: the big-step `eval : Env → Expr → Datum`. List-carrying constructors evaluate each operand and hand the result list to the matching primitive.
* `Mz/Boolean.lean`: per-cell truth-table proofs for `AND`, `OR`, and `NOT`, plus involutivity of `NOT`.
* `Mz/MightError.lean`: the `Expr.might_error` static analyzer, the `Env.ErrFree` predicate, and the `might_error_sound` theorem. `andN` and `orN` recurse via `Expr.argsMightError` ("any operand might error"); `coalesce` recurses via `Expr.argsAllMightError` ("every operand might error"), special-casing the empty list as safe. Soundness for `coalesce` extracts a statically-safe operand through `Expr.exists_safe_of_not_argsAllMightError` and applies `evalCoalesce_not_err_of_some_safe`, which in turn rests on `Coalesce.go_not_err` — the state-machine lemma that "once one safe operand is in the remaining list, the walk cannot return an error".
* `Mz/Strict.lean`: strictness predicates (`ErrStrictUnary`, `ErrStrictBinary`, `NullStrictUnary`), positive instances for `evalNot` and the condition slot of `evalIfThen`, closure under composition, and negative results witnessing that `AND` and `OR` are *not* err-strict in either position.
* `Mz/Coalesce.lean`: laws for `evalCoalesce` — error-rescue, null-beats-err tiebreak, first-error stickiness.
* `Mz/Laws.lean`: algebraic laws — two-sided identity (`TRUE` for `AND`, `FALSE` for `OR`), idempotence (unconditional), commutativity (conditional on error-freedom of operands), and `Expr`-level reorder safety as a corollary of soundness.
* `Mz/Variadic.lean`: laws for `evalAndN` and `evalOrN` over `List Datum` — cons recurrence, nil, singleton, binary equivalence with the binary evaluators, and `FALSE`/`TRUE` absorption.
* `Mz/ExprVariadic.lean`: `Expr`-level reduction lemmas connecting `eval env (.andN args)` / `.orN` / `.coalesce` to their primitive counterparts, identity / singleton / binary-equivalence corollaries lifted through `eval`, and variadic-absorption theorems — a single operand evaluating to `FALSE` (resp. `TRUE`) makes the whole `andN` (resp. `orN`) evaluate to `FALSE` (resp. `TRUE`).
* `Mz/Bag.lean`: bag semantics on `List Row`. Defines `filterRel` and `project`, with filter idempotence, filter commutativity, projection length-preservation, and the empty-projection equation. Plain `filterRel` silently drops `err` rows; `Mz/ErrStream.lean` adds the explicit data/error stream pair.
* `Mz/ErrStream.lean`: the dataflow-style `BagStream = (data, errors)` pair.
  `BagStream.filter` routes erroring rows into the error collection instead of dropping them, with idempotence proved at both the data and the error level.
  `BagStream.project` projects each row through a list of scalars; a row stays in the data collection only when every scalar succeeds, otherwise its err payloads (one per erroring scalar) are appended to the error collection.
  `rowErrs_nil_of_all_safe` and `projectErrs_eq_nil_of_all_safe` show that when no projection errs, `BagStream.project` does not extend the error collection.
* `Mz/Pushdown.lean`: substitution (`Expr.subst`) plus the headline `eval_subst` theorem (substituting then evaluating against the original row equals evaluating against the projected row), and the relational predicate-pushdown rewrite `filterRel p (project es rel) = project es (filterRel (p.subst es) rel)`.
* `Mz/DiffSemiring.lean`: `DiffWithError α` — the diff-field type extension that encodes global (collection-scoped) errors as an absorbing element. Provides `+`, `*`, `0`, `1` instances over an arbitrary base diff and proves the absorption / commutativity / associativity / distributivity laws that downstream operators must respect. Now wired through `UnifiedStream` / `Join` — every operator on the unified stream speaks `DiffWithError Int` directly.
* `Mz/UnifiedStream.lean`: unified diff-aware alternative to `BagStream`. `UnifiedStream := List (UnifiedRow × DiffWithError Int)` pairs a carrier (data row or row-scoped err) with a differential-dataflow diff augmented by the absorbing `error` element. Row-scoped errors flow through the carrier; collection-scoped errors flow through diff multiplication / addition. `ofBag` / `split` conversions assign every bag record a diff of `.val 1`; the round-trip theorem `split (ofBag s) = s` holds. The cross-direction is exact only up to multiset equality on `List EvalError` and is lossy for diffs ≠ `.val 1` (split drops diff information).
* `Mz/Aggregate.lean`: aggregate reductions over `List Datum`. `aggCountNonNull` for `COUNT(expr)`. `aggStrict` for `SUM`/`MIN`/`MAX`-style aggregates that propagate `err` (first one in scan order wins) and skip `NULL`s. `aggTry` for the proposed `try_sum`/`try_min`/`try_max` variants that swallow `err` into `NULL` instead of propagating, defined as a post-pass on `aggStrict`. Theorems: `aggStrict_err` (any `err` input → `err` output), `aggStrict_no_err` (no-err inputs + no-err reducer → no-err output), `aggTry_no_err` (the non-strict variant never errors), and `aggTry_eq_aggStrict_of_no_err` (strict and non-strict agree on error-free inputs).
* `Mz/Consolidate.lean`: per-key diff summation over `List (DiffWithError α)`. The headline `sumAll_eq_error_of_mem` proves that an `error` diff anywhere in the list absorbs the consolidated sum to `error`, which is the property a differential dataflow `compact` operator cites when propagating global errors through consolidation. Companion `sumAll_val_of_all_val` says an all-`val` list sums to `val` of some base value.
* `Mz/UnifiedConsolidate.lean`: row-keyed diff summation on `UnifiedStream`. `UnifiedStream.consolidate` buckets records by carrier (via `DecidableEq UnifiedRow`) and sums per-bucket diffs. Theorems cover three properties:
  *absorption* — `consolidate_preserves_error` proves an `.error` diff anywhere in the input gives an `.error` diff in the consolidated output for that carrier;
  *cardinality* — `consolidate_length_le` bounds the output by the input length (consolidation only merges, never expands);
  *no-error preservation* — `consolidate_no_error` proves that if every input diff is a `.val`, every output diff is a `.val`, so `.error` is the only source of absorption.
* `Mz/Triple.lean`: `TimedRecord = (row, time, diff)` triple stream tying `DiffWithError` to differential dataflow's record format. Defines `consolidateAll` and `consolidateAt t`; lifts `Consolidate`'s absorption to per-time and stream-wide statements. Per-`(row, time)` bucketing reduces to applying `UnifiedConsolidate` inside each time slice.
* `Mz/Join.lean`: relational joins on the diff-aware `UnifiedStream`. `cross` is the cartesian product — carriers combine via `combineCarrier` (rows concatenate; err on either side wins, left first), diffs multiply through `DiffWithError`'s `Mul` instance. A `.error` diff on either input therefore absorbs to `.error` on the output via `DiffWithError.error_mul_{left,right}`. `join pred l r` filters the product through a join predicate. Theorems: `cross_length` (`l.length * r.length`), `filter_length_le` (filter is non-expanding), `join_length_le` (corollary). Diff-propagation theorems: `cross_diff_error_{left,right}` (a `.error` diff on either side propagates through every output record), `filter_preserves_error_diff` (a record carrying `.error` diff is never dropped by `filter` — the absorbing marker cannot be filtered away).
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

The roadmap in priority order:

* `BagStream.filter` commutativity. Data field commutes by `filterRel_comm`; the error field requires a notion of multiset equality on `List EvalError` since list-order differs across permutations.
* `BagStream.project` / `BagStream.filter` commutativity (when the predicate references only un-projected columns). Same multiset-equality caveat on the error collection.
* Tie `DiffWithError` to a concrete dataflow operator: model a `(Row, Time, DiffWithError ℤ)` triple stream and prove that an `error` diff at time `t` propagates to every downstream consolidation.
* Joins on `BagStream` with explicit error propagation.
* Sketch a proof of `cross_assoc` modulo row concatenation associativity. Left-wins error rule is consistent in both nestings; the residual obligation is `(la ++ lb) ++ lc = la ++ (lb ++ lc)` lifted into the row carrier.
* Tightening `Expr.might_error`. The skeleton version is purely structural and ignores type / nullability information; bringing it closer to `MirScalarExpr::might_error` is additive.
* Lift to bag semantics for predicate / projection rewrites.

The diff-semiring extension is now in scope: `UnifiedStream` records carry a `DiffWithError Int` and operators preserve / multiply / absorb it as appropriate.

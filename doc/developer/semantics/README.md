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
* `Mz/MightError.lean`: the `Expr.might_error` static analyzer, the `Env.ErrFree` predicate, and the `might_error_sound` theorem. `andN` and `orN` recurse into their operand list via the mutually-recursive `Expr.argsMightError`; soundness for those cases extracts a per-operand non-erroring witness through `Expr.argsMightError_of_mem` and recurses. `coalesce` is still tainted unconditionally and soundness for it is vacuous.
* `Mz/Strict.lean`: strictness predicates (`ErrStrictUnary`, `ErrStrictBinary`, `NullStrictUnary`), positive instances for `evalNot` and the condition slot of `evalIfThen`, closure under composition, and negative results witnessing that `AND` and `OR` are *not* err-strict in either position.
* `Mz/Coalesce.lean`: laws for `evalCoalesce` — error-rescue, null-beats-err tiebreak, first-error stickiness.
* `Mz/Laws.lean`: algebraic laws — two-sided identity (`TRUE` for `AND`, `FALSE` for `OR`), idempotence (unconditional), commutativity (conditional on error-freedom of operands), and `Expr`-level reorder safety as a corollary of soundness.
* `Mz/Variadic.lean`: laws for `evalAndN` and `evalOrN` over `List Datum` — cons recurrence, nil, singleton, binary equivalence with the binary evaluators, and `FALSE`/`TRUE` absorption.
* `Mz/ExprVariadic.lean`: `Expr`-level reduction lemmas connecting `eval env (.andN args)` / `.orN` / `.coalesce` to their primitive counterparts, plus identity / singleton / binary-equivalence corollaries lifted through `eval`.

## What is not here

* No bag semantics, joins, aggregates, or relational operators.
* No diff-semiring extension for global errors (see the design doc).
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

* Tighten `Expr.might_error` for `.coalesce`. The current placeholder taints unconditionally; a precise analyzer would reason about the rescue rule (one statically-safe operand makes the whole coalesce safe). Requires a list-induction analogous to `Expr.argsMightError_of_mem` plus an `evalCoalesce_not_err` helper.
* Variadic absorption at the `Expr` level: `FALSE ∈ args → eval env (.andN args) = .bool false` for a closed term. Mirrors `evalAndN_false_absorbs`, lifted through `eval`.
* Tightening `Expr.might_error`. The skeleton version is purely structural and ignores type / nullability information; bringing it closer to `MirScalarExpr::might_error` is additive.
* Lift to bag semantics for predicate / projection rewrites.

The diff-semiring extension for global errors is a separate v2 effort.

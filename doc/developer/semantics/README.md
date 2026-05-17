# Lean 4 semantics skeleton

A mechanized model of Materialize's scalar evaluation semantics.

This directory contains the v1 skeleton accompanying the error-handling design document at `../design/20260517_error_handling_semantics.md`.
The goal of the skeleton is not to mechanize all of `MirScalarExpr`.
The goal is to lock in the boolean truth tables for `AND` and `OR` over the four-valued logic `{TRUE, FALSE, NULL, ERROR}` and provide a place to grow from.

## What is here

* `Mz/Datum.lean`: `Datum`, `EvalError`, and the `Datum.IsErr` predicate.
* `Mz/Expr.lean`: a minimal `Expr` inductive (literals, columns, binary `and`/`or`, `not`, `ifThen`).
* `Mz/Eval.lean`: `evalAnd`, `evalOr`, `evalNot`, `evalIfThen`, and `eval` matching the runtime in `src/expr/src/scalar/func/variadic.rs`. `Env.get` is defined by primitive recursion to keep inductive proofs simple.
* `Mz/Boolean.lean`: per-cell truth-table proofs for `AND`, `OR`, and `NOT`, plus involutivity of `NOT`.
* `Mz/MightError.lean`: the conservative `Expr.might_error` static analyzer, the `Env.ErrFree` predicate, and the `might_error_sound` theorem that the optimizer needs in order to trust the analyzer's verdict.
* `Mz/Laws.lean`: algebraic laws — idempotence (unconditional), commutativity (conditional on error-freedom of operands), and `Expr`-level reorder safety as a corollary of soundness.

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

* Strict-propagation theorem: any strict function applied to `err` returns `err`. Requires introducing a `Strict` predicate on functions and proving closure under composition.
* Coalesce error-rescue law: `coalesce(err, x) = x`. Requires an `n`-ary `coalesce` operator on the `Expr` side.
* Variadic `And` and `Or` via fold, with equivalence to the binary form. The binary truth tables and laws transport to the variadic form by induction on the operand list.
* Tightening `Expr.might_error`. The skeleton version is purely structural and ignores type / nullability information; bringing it closer to `MirScalarExpr::might_error` is additive.
* Lift to bag semantics for predicate / projection rewrites.

The diff-semiring extension for global errors is a separate v2 effort.

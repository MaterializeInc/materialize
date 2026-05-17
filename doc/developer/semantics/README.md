# Lean 4 semantics skeleton

A mechanized model of Materialize's scalar evaluation semantics.

This directory contains the v1 skeleton accompanying the error-handling design document at `../design/20260517_error_handling_semantics.md`.
The goal of the skeleton is not to mechanize all of `MirScalarExpr`.
The goal is to lock in the boolean truth tables for `AND` and `OR` over the four-valued logic `{TRUE, FALSE, NULL, ERROR}` and provide a place to grow from.

## What is here

* `Mz/Datum.lean`: `Datum`, `EvalError`, and the `Datum.IsErr` predicate.
* `Mz/Expr.lean`: a minimal `Expr` inductive (literals, columns, binary `and`/`or`, `not`, `ifThen`).
* `Mz/Eval.lean`: `evalAnd`, `evalOr`, `evalNot`, and `eval` matching the runtime in `src/expr/src/scalar/func/variadic.rs`.
* `Mz/Boolean.lean`: per-cell truth-table proofs for `AND`, `OR`, and `NOT`, plus involutivity of `NOT`.
* `Mz/Laws.lean`: algebraic laws — idempotence (unconditional) and commutativity (conditional on error-freedom).

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

* `Expr.might_error` predicate plus soundness: `¬might_error e → ∀ env err-free, ¬(eval env e).IsErr`.
* Reorder-safety conditions on `Expr`: lift `evalAnd_comm_of_no_err` to `eval env (And a b) = eval env (And b a)` when `¬a.might_error ∧ ¬b.might_error`.
* Strict-propagation theorem: any strict function applied to `err` returns `err`.
* Coalesce error-rescue law: `coalesce(err, x) = x`.
* Variadic `And` and `Or` via fold, with equivalence to the binary form.
* Lift to bag semantics for predicate / projection rewrites.

The diff-semiring extension for global errors is a separate v2 effort.

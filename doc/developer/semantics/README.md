# Lean 4 semantics skeleton

A mechanized model of Materialize's scalar evaluation and stream
semantics. Accompanies the error-handling design document at
`../design/20260517_error_handling_semantics.md`.

The skeleton is exploratory. It locks in the four-valued boolean
logic for `AND` and `OR`, mechanizes the design doc's two-diff
stream model, and serves as the place where alternative error
semantics get tried against concrete proof obligations before any
runtime change.

## Reading order

1. `model.md` — layered reference for `Datum`, `Expression`, `Row`,
   `Stream`, and the equivalence relations explored. Start here.
2. `../design/20260517_error_handling_semantics.md` — the discursive
   design doc with the broader options analysis.
3. `transforms.md` — catalog of mechanized equational and inclusion
   laws for optimizer rewrites.
4. The `Mz/*.lean` modules themselves; each carries its own
   docstring.

## Module map

* **Core scalar layer** — `Datum`, `Expr`, `PrimEval`, `Eval`,
  `Boolean`, `MightError`, `Strict`, `Coalesce`, `Laws`, `Variadic`,
  `ExprVariadic`.
* **Substitution** — `Subst` (`Expr.subst`, `eval_subst`).
* **Column-reference analysis** — `ColRefs`
  (`colReferencesBoundedBy`, `colShift`, `colReferencesUnused` and
  the eval-on-append lemmas).
* **Stream layer** — `Stream` (two-diff baseline, untyped row),
  `StreamN` (indexed arity via `List.Vector`).
* **Equivalence relations** — `Equiv` (`eqErrSet`, `refines`,
  `refinesDual`), `EquivBounded` (bounded-arithmetic counterexample).

## Build

`ci/test/lean-semantics.sh` — Docker, Lean v4.29.1, Mathlib v4.29.1
(prebuilt cache baked into the image). For one-shot probes against
the image (Mathlib sources, ad-hoc `lake env` invocations), use
`bin/in-image`.

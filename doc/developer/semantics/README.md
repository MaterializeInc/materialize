# Lean 4 semantics skeleton

A mechanized model of Materialize's scalar evaluation and collection
semantics.
Accompanies the error-handling design document at
`../design/20260517_error_handling_semantics.md`.

The skeleton is exploratory.
It locks in the four-valued boolean logic for `AND` and `OR`,
mechanizes the design doc's two-diff model on a time-stripped
collection (a single collection version in
`../platform/formalism.md`'s sense), and serves as the place where
alternative error semantics get tried against concrete proof
obligations before any runtime change.

## Reading order

1. `model.md` — layered reference for `Datum`, `Expression`, `Row`,
   `Schema`, `Collection`, and the equivalence relations explored.
   Start here.
2. `../design/20260517_error_handling_semantics.md` — the discursive
   design doc with the broader options analysis.
3. `transforms.md` — catalog of transforms we want to model and the
   soundness windows under which each holds.
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
* **Collection layer** — `Collection` (indexed-arity `Collection n`
  with `Update n` carrying `(row, diff, err_diff)`; `filter`,
  `project`, `cross`, `negate`, `unionAll`).
* **Schema** — `Schema` (per-column nullable/errable, collection-
  level `rowErrFree`); `OutputType` (per-`Expr` output schema with
  soundness theorem).
* **Equivalence relations** — `Equiv` (`eqErrSet`, `refines`),
  `EquivBounded` (bounded-arithmetic counterexample).

## Build

`ci/test/lean-semantics.sh` — Docker, Lean v4.29.1, Mathlib v4.29.1
(prebuilt cache baked into the image).
For one-shot probes against the image (Mathlib sources, ad-hoc
`lake env` invocations), use `bin/in-image`.

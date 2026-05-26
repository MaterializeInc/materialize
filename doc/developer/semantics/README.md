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

## Architecture: GADT (indexed) model

The model is built around **schema-indexed types**:

* `ColType { bool | int | top }` — per-column SQL type tag.
* `Schema n` — collection metadata: `cols : Vector ColSchema n`,
  `types : Vector ColType n`, `rowErrFree : Bool`.
* `Datum : ColType → Type` — scalar value indexed by its kind.
  `.bool` inhabits `Datum .bool`, `.int` inhabits `Datum .int`,
  `.null` / `.err _` universal.
* `Expr (sch : Schema n) : ColType → Type` — expression typed by
  input schema and output type. Mutual with `ExprList sch k` for
  variadics.
* `Env sch := (i : Fin n) → Datum (sch.types.get i)` — typed
  environment.
* `Update sch` / `Collection sch` — schema-indexed collection
  layer.

Type discipline is structural — ill-typed expressions are
**unconstructible**. `Expr.not (.lit (.int 5))` fails to
type-check because `Expr.not` requires `Expr sch .bool` and
`.lit (.int 5) : Expr sch .int`. The `WellTyped` predicate that
earlier iterations carried is now subsumed by the GADT; analyzers
and theorems lose the awkward `¬d.IsInt` hypotheses the untyped
model required.

## Reading order

1. `model.md` — layered reference for `Datum`, `Expression`, `Row`,
   `Schema`, `Collection`, and the equivalence relations explored.
   Start here.
2. `../design/20260517_error_handling_semantics.md` — the discursive
   design doc with the broader options analysis.
3. `transforms.md` — catalog of transforms we want to model and the
   soundness windows under which each holds.
4. The `Mz/Indexed/*.lean` modules themselves; each carries its own
   docstring.

## Module map

All modules live under `Mz/Indexed/` (the namespace `Mz.Indexed`).
The non-indexed `Mz/Schema.lean` carries only type-level
definitions (`ColType`, `ColSchema`, `Schema`, `EvalError`) reused
by the indexed model.

* **Datum layer.** `Indexed.Datum` (`Datum : ColType → Type`),
  `Indexed.PrimEval` (`evalAnd`, `evalOr`, `evalNot`, arithmetic,
  comparison, `evalAndN` / `evalOrN` / `evalCoalesce`).
* **Algebraic laws.** `Indexed.Boolean` (truth tables),
  `Indexed.Laws` (identity, idempotence, commutativity),
  `Indexed.Strict` (err- / null-propagation classes),
  `Indexed.Variadic` (variadic absorption), `Indexed.Coalesce`
  (per-kind coalesce equations).
* **Expression layer.** `Indexed.Expr` (mutual `Expr` / `ExprList`
  GADT), `Indexed.Eval` (mutual `eval` / `evalList`),
  `Indexed.Subst` (substitution + soundness),
  `Indexed.MightError` (analyzer + per-primitive error-free
  lemmas), `Indexed.OutputType` (per-`Expr` `ColSchema`
  derivation).
* **Equivalences.** `Indexed.Equiv` (`Datum.eqErrSet`,
  `Datum.refines`, compositionality), `Indexed.EquivBounded`
  (bounded-arithmetic counterexample), `Indexed.Legal`
  (non-deterministic `LegalEval`).
* **Collection layer.** `Indexed.Schema` (`EnvSatisfies`),
  `Indexed.Collection` (`Update sch`, `Collection sch`, `filter`,
  `project`, `cross` [deferred], `negate`, `unionAll`,
  `NoRowErr`).

## Build

`ci/test/lean-semantics.sh` — Docker, Lean v4.29.1, Mathlib v4.29.1
(prebuilt cache baked into the image).
For one-shot probes against the image (Mathlib sources, ad-hoc
`lake env` invocations), use `bin/in-image`.

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

## Architecture

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
4. The `Mz/*.lean` modules themselves; each carries its own
   docstring.

## Module map

All modules live under `Mz/` in the `Mz` namespace. `Mz/Schema.lean`
carries the type-level definitions (`ColType`, `ColSchema`, `Schema`,
`EvalError`); the rest of the modules build the indexed model on
top.

* **Datum layer.** `Mz.Datum` (`Datum : ColType → Type`),
  `Mz.PrimEval` (`evalAnd`, `evalOr`, `evalNot`, arithmetic,
  comparison, `evalAndN` / `evalOrN` / `evalCoalesce`).
* **Algebraic laws.** `Mz.Boolean` (truth tables), `Mz.Laws`
  (identity, idempotence, commutativity), `Mz.Strict` (err- /
  null-propagation classes), `Mz.Variadic` (variadic absorption),
  `Mz.Coalesce` (per-kind coalesce equations).
* **Expression layer.** `Mz.Expr` (mutual `Expr` / `ExprList`
  GADT), `Mz.Eval` (mutual `eval` / `evalList`), `Mz.Subst`
  (substitution + soundness), `Mz.MightError` (analyzer +
  per-primitive error-free lemmas), `Mz.OutputType` (per-`Expr`
  `ColSchema` derivation; `DatumSatisfies` / `RowSatisfies`
  satisfaction predicates; `EnvErrFree_of_RowSatisfies` bridge;
  `coalesce_collapse` schema-rider).
* **Equivalences.** `Mz.Equiv` (`Datum.eqErrSet`, `Datum.refines`,
  compositionality, `evalAnd_err_err_eqErrSet_comm`),
  `Mz.EquivBounded` (bounded-arithmetic counterexample —
  `evalPlusBounded_assoc_max_*`), `Mz.Legal` (non-deterministic
  `LegalEval`, `legal_of_eval`, `legal_plus_err_either`,
  `LegalEquiv` / `LegalSubsume`).
* **Collection layer.** `Mz.Collection` — `Update sch`,
  `Collection sch`, `filter`, `project`, `cross`, `negate`,
  `unionAll`, `Row.refines` / `Update.refines` /
  `Collection.refines` Smyth-style lift, `NoRowErr` precondition,
  `NoRowErr_negate` / `NoRowErr_unionAll` / `NoRowErr_project` /
  `NoRowErr_cross` / `NoRowErr_filter` propagation,
  `Collection.Equiv` (perm + merge + drop_zero) with
  `unionAll_comm_equiv` and `negate_unionAll_self` demonstrators,
  `filter_cross_pushdown_left_unsound` counterexample.

**Arity-cast scaffolding** (Mz.Schema + Mz.Collection):
`Vector.cast_val`, `Vector.cast_eq_append_assoc`,
`Schema.cast_mk`, `Schema.append_assoc_heq`, `Update.cast`,
`Collection.cast`, `cast_rfl`, `crossOne_diff_assoc`,
`crossOne_err_diff_assoc`, `crossOne_diff_eq`,
`crossOne_err_diff_eq`. The bilinear err-rule polynomial
identity closes by `ring`.

**Still open**: `Collection.cross_assoc` — row-component
equality at index `Fin (n + (m + k))` requires Fin-index
manipulation under three nested `▸` casts
(`Schema.types_get_append_left/right` at two nesting levels),
plus the inductive lift over Collection;
`filter_cross_pushdown_left_*` recovery windows (strict via
`NoRowErr`, data via `eraseRowErr`, refinement via `SignOK`) —
blocked on iota reduction of `filterOne`'s match auxiliary, see
inline note in `Mz/Collection.lean`; `eraseRowErr`.

## Build

`ci/test/lean-semantics.sh` — Docker, Lean v4.29.1, Mathlib v4.29.1
(prebuilt cache baked into the image).
For one-shot probes against the image (Mathlib sources, ad-hoc
`lake env` invocations), use `bin/in-image`.

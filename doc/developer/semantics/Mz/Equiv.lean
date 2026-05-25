import Mz.PrimEval

/-!
# Equivalence relations on `Datum` under reorderable SQL evaluation

SQL evaluation order is unspecified outside of `CASE`, `AND` / `OR`
short-circuit, and a few other explicitly-ordered constructs. A
conforming implementation may evaluate `x + 1 + (-1)` as either
`(x + 1) + (-1)` or `x + (1 + (-1))`. On a bounded `Int32` near
`MAX_INT32`, the first form raises overflow and the second does not.

The semantics has to admit this freedom, or *every* interesting
optimizer rewrite (associativity of `+`, predicate pushdown across
`AND`, constant folding under arithmetic) is unsound the moment errors
are reachable. The cost of admitting it is choosing a relation on
`Datum` coarser than strict equality and re-stating the existing laws
against that relation.

This module defines three relations the design doc
(`doc/developer/design/20260517_error_handling_semantics.md`,
§"Evaluation-order equivalence") names as candidates:

* `Datum.eqErrSet a b` — error-set equivalence. Collapses all `.err`
  payloads into a single equivalence class. Symmetric, reflexive,
  transitive.
* `Datum.refines a b` — preorder with errors as the least element.
  Not a partial order: antisymmetry fails because `.err e1 ⊑ .err e2`
  and `.err e2 ⊑ .err e1` both hold for distinct payloads `e1 ≠ e2`.
  The induced equivalence (two-way refinement) is `eqErrSet`. An
  optimizer rewrite `e1 → e2` is sound under the "no spurious
  errors" posture iff `eval e1 ⊑ eval e2` pointwise.
* `Datum.refinesDual a b` — the dual order with errors as the top.
  The "spurious errors permitted" (PostgreSQL) posture is sound under
  this preorder.

Each relation is built from `(=)` and `Datum.IsErr` so the supporting
proofs reduce to disjunction destructuring rather than nested pattern
matching on the relation itself. The current `Mz/Eval.lean` `eval`
function targets strict equality because its codomain is `Datum` and
the boolean / arithmetic primitives pick a specific evaluation order.
The relations here are the layer at which we phrase rewrites whose
strict-equality form is false. The non-trivial laws (associativity of
`plus` under refinement on a bounded `Int`, predicate pushdown across
`cross` under value-only equivalence) are stated as goals or
documented as counterexamples rather than proved here. -/

namespace Mz

/-! ## Error-set equivalence

The minimal upgrade from strict equality: two datums are equivalent
iff they are equal as `Datum` values *or* both are some `.err _`
(regardless of payload). This lets us state laws like "`evalAnd`
commutes on err / err inputs" that fail under strict equality because
the left-bias of the pattern matcher picks the left payload. -/

/-- Error-set equivalence on `Datum`. -/
def Datum.eqErrSet (a b : Datum) : Prop :=
  a = b ∨ (a.IsErr ∧ b.IsErr)

/-- Reflexivity of `eqErrSet`. -/
theorem Datum.eqErrSet_refl (d : Datum) : d.eqErrSet d :=
  Or.inl rfl

/-- Symmetry of `eqErrSet`. -/
theorem Datum.eqErrSet_symm {a b : Datum} (h : a.eqErrSet b) : b.eqErrSet a := by
  cases h with
  | inl h => exact Or.inl h.symm
  | inr h => exact Or.inr ⟨h.2, h.1⟩

/-- Equality implies `eqErrSet`. The forward direction of the
embedding "strict equality ⊆ error-set equivalence". -/
theorem Datum.eqErrSet_of_eq {a b : Datum} (h : a = b) : a.eqErrSet b :=
  Or.inl h

/-- `eqErrSet` is reflexive on the two-error case regardless of
payload mismatch. This is the cell that strict equality cannot
reach. -/
theorem Datum.eqErrSet_err_err (e₁ e₂ : EvalError) :
    Datum.eqErrSet (.err e₁) (.err e₂) :=
  Or.inr ⟨True.intro, True.intro⟩

/-! ## Counterexample: `evalAnd` is not strictly commutative on err / err

`evalAnd` is left-biased on errors: `evalAnd (.err e1) (.err e2)` picks
`e1`. Under strict equality, swapping operands gives a different
`Datum` whenever `e1 ≠ e2`. Under `eqErrSet`, both are some `.err _`
and the relation holds.

The current skeleton has only one `EvalError` variant
(`.divisionByZero`), so the counterexample is moot in the proof tree
today — the only existing error is equal to itself. As soon as a
second variant is added (e.g. an `OverflowError` for bounded
arithmetic), the counterexample fires and `eqErrSet` becomes the
relation that recovers commutativity. The lemmas below illustrate the
shape of the argument so the future extension lands cleanly. -/

/-- `evalAnd` on err / err picks the left payload. -/
theorem evalAnd_err_err (e₁ e₂ : EvalError) :
    evalAnd (.err e₁) (.err e₂) = .err e₁ := rfl

/-- The symmetric statement: with operands swapped, `evalAnd` picks
the (newly) left payload, which was the original right. -/
theorem evalAnd_err_err_swap (e₁ e₂ : EvalError) :
    evalAnd (.err e₂) (.err e₁) = .err e₂ := rfl

/-- Under `eqErrSet`, `evalAnd` commutes on err / err inputs even when
the inner payloads differ. This is the headline rewrite that
error-set equivalence recovers. -/
theorem evalAnd_err_err_eqErrSet_comm (e₁ e₂ : EvalError) :
    (evalAnd (.err e₁) (.err e₂)).eqErrSet (evalAnd (.err e₂) (.err e₁)) := by
  -- evalAnd reduces both sides to .err _; eqErrSet's err / err cell
  -- discharges the goal.
  exact Or.inr ⟨True.intro, True.intro⟩

/-! ## Refinement preorder (errors as bottom)

`a ⊑ b` ("a refines to b" / "b is at least as defined as a") holds iff
`a = b` or `a` is some `.err _`. Errors are the least-defined element.
An optimizer rewrite `e1 → e2` is sound under the "no spurious errors"
posture iff `eval e1 ⊑ eval e2` pointwise. -/

/-- Refinement preorder on `Datum` with errors as bottom. -/
def Datum.refines (a b : Datum) : Prop :=
  a = b ∨ a.IsErr

/-- Reflexivity. -/
theorem Datum.refines_refl (d : Datum) : d.refines d :=
  Or.inl rfl

/-- Equality implies refinement (refines is reflexive and weaker than
equality). -/
theorem Datum.refines_of_eq {a b : Datum} (h : a = b) : a.refines b :=
  Or.inl h

/-- An error refines anything. This is the asymmetric law that
distinguishes refinement from strict equality. -/
theorem Datum.err_refines (e : EvalError) (d : Datum) :
    Datum.refines (.err e) d :=
  Or.inr True.intro

/-- Refinement is transitive. Useful for chaining rewrites. -/
theorem Datum.refines_trans {a b c : Datum}
    (h₁ : a.refines b) (h₂ : b.refines c) : a.refines c := by
  cases h₁ with
  | inl h => subst h; exact h₂
  | inr ha => exact Or.inr ha

/-! ## Dual refinement preorder (errors as top)

`a ⊒ b` iff `a = b` or `b` is some `.err _`. Errors are the
greatest-defined element under this dual order.

Posture sketched as "spurious errors permitted (PostgreSQL)". A
caveat on that label: PG's actual stance is not the unconditional
form "any rewrite that adds an error is admissible." PG permits
errors that surface as side effects of its evaluation strategy
(e.g. evaluation of a sub-expression whose value would not have
been needed), but it does not endorse rewrites that *gratuitously
inject* errors into previously error-free expressions. A pure
`refinesDual` posture is strictly broader than PG. The relation
that matches Materialize's intended stance — "errors that some
legal evaluation order would produce are admissible; errors no
evaluation order would produce are not" — needs a non-deterministic
`eval` that returns a *set* of `Datum`s, which is out of scope for
this preorder layer. -/

/-- Dual refinement preorder on `Datum` with errors as top. -/
def Datum.refinesDual (a b : Datum) : Prop :=
  a = b ∨ b.IsErr

/-- Reflexivity of the dual preorder. -/
theorem Datum.refinesDual_refl (d : Datum) : d.refinesDual d :=
  Or.inl rfl

/-- Anything refines an error under the dual preorder. -/
theorem Datum.refinesDual_err (d : Datum) (e : EvalError) :
    Datum.refinesDual d (.err e) :=
  Or.inr True.intro

/-- Transitivity of the dual preorder. -/
theorem Datum.refinesDual_trans {a b c : Datum}
    (h₁ : a.refinesDual b) (h₂ : b.refinesDual c) : a.refinesDual c := by
  cases h₂ with
  | inl h => subst h; exact h₁
  | inr hc => exact Or.inr hc

/-! ## Connections between the relations

Two-way refinement (under either preorder) collapses to error-set
equivalence. Strict equality is the strongest; the two-way
refinement and `eqErrSet` coincide and form a strict-equality-
respecting equivalence class on errors; each preorder is a one-
directional relaxation that permits asymmetric rewrites. -/

/-- Two-way refinement collapses to error-set equivalence: if `a`
refines `b` and `b` refines `a`, then `a` and `b` are eqErrSet. The
two conjuncts together say "they agree, or both are errs". -/
theorem Datum.eqErrSet_of_refines_both
    {a b : Datum} (h₁ : a.refines b) (h₂ : b.refines a) :
    a.eqErrSet b := by
  cases h₁ with
  | inl h => exact Or.inl h
  | inr ha =>
    cases h₂ with
    | inl h => exact Or.inl h.symm
    | inr hb => exact Or.inr ⟨ha, hb⟩

/-- The dual statement: under the dual preorder, two-way agreement
also collapses to `eqErrSet`. Dual to `eqErrSet_of_refines_both`. -/
theorem Datum.eqErrSet_of_refinesDual_both
    {a b : Datum} (h₁ : a.refinesDual b) (h₂ : b.refinesDual a) :
    a.eqErrSet b := by
  cases h₁ with
  | inl h => exact Or.inl h
  | inr hb =>
    cases h₂ with
    | inl h => exact Or.inl h.symm
    | inr ha => exact Or.inr ⟨ha, hb⟩

/-- `refinesDual` is the reverse of `refines`: `a.refinesDual b ↔
b.refines a`. The two relations are interconvertible; we keep both
to let theorem statements pick the more intuitive direction. -/
theorem Datum.refinesDual_iff_refines_swap (a b : Datum) :
    a.refinesDual b ↔ b.refines a := by
  constructor
  · intro h
    cases h with
    | inl h => exact Or.inl h.symm
    | inr h => exact Or.inr h
  · intro h
    cases h with
    | inl h => exact Or.inl h.symm
    | inr h => exact Or.inr h

/-! ## Counterexamples I tried and could not prove

These are statements where the proof does not go through under the
candidate relations. They are recorded here so a future iteration
knows the terrain.

### Strict equality of `evalAnd` on err / err

Statement: `evalAnd (.err e₁) (.err e₂) = evalAnd (.err e₂) (.err e₁)`.

Counterexample (would apply): the LHS is `.err e₁` by the existing
pattern; the RHS is `.err e₂` by the same pattern with operands
swapped. When `e₁ ≠ e₂`, the two are distinct `Datum` values.

The current skeleton has only `.divisionByZero` so `e₁ = e₂` is forced
and the counterexample is hidden. Add any second `EvalError` variant
and the counterexample fires.

Recovered by: `evalAnd_err_err_eqErrSet_comm` above (under
`eqErrSet`).

### Associativity of `evalPlus` on bounded `Int`

Statement: `evalPlus (evalPlus x (.int 1)) (.int (-1)) =`
`evalPlus x (evalPlus (.int 1) (.int (-1)))`.

The current `evalPlus` uses Lean's unbounded `Int`, so both sides
reduce to `x + 1 + (-1) = x` and the equality holds (in fact provable
by `rfl` for any `x : Datum` after case-splitting on `x`). The
counterexample lives at the bounded-int boundary: with
`x = MAX_INT32` and an `evalPlusBounded`, the LHS errs on overflow and
the RHS is `MAX_INT32`. To exercise this, the skeleton needs an
`evalPlusBounded : Datum → Datum → Datum` variant; that is a natural
next-step extension if bounded arithmetic becomes a forcing function.

Under `eqErrSet`: still fails (one is err, the other is value, not
both errs and not equal).
Under `refines`: LHS ⊑ RHS holds — LHS is `.err _`, so the disjunct
`.err _.IsErr = True` discharges. The reverse, RHS ⊑ LHS, fails:
RHS is `.int MAX_INT32`, not equal to LHS (`.err _`), and `.int _`
is not `IsErr`.
Under `refinesDual`: LHS ⊒ RHS holds by `refines`-dual symmetric
argument; RHS ⊒ LHS does not.

### Predicate pushdown on err side

Statement: `filter p (cross l r) = cross (filter p l) r` on the err
side under any of the relations above.

Counterexample (modeled on the collection model in
`Mz/Collection.lean`): a single-row pair where `eval rL p` errs.
LHS errs once per RHS row (the cross product first, then filter);
the migration moves `diff = 1` to `err_diff = |r|`. RHS errs once
on the left side alone (filter then cross); the cross product
multiplies that single err through `r.length` updates on the right,
giving err multiplicity `|r|`.

The two err multiplicities happen to match (`|r|` in both cases), but
the *carrier rows* differ: LHS has err updates with content
`rL ++ rR_i` for each i; RHS has err updates with content `rL` alone
(filter does not project to a wider carrier). Strict equality on
collections fails on the carrier shape.

Under `eqErrSet` lifted pointwise to collections: still fails on
update-level carrier mismatch.
Under value-only equivalence under non-determinism: holds, because
both sides are valid evaluation orders of the same SQL expression
and their successful-data outputs agree.

Mechanizing this on the err side is the canonical motivation for
moving to non-deterministic semantics. Until that lift lands, the
err-side pushdown statement remains an open obligation; the data-side
form is closed by `Collection.filter` / `Collection.cross` reasoning
that ignores err multiplicities.

### Preservation of `refines` by binary primitives

Statement: `a.refines a' → b.refines b' → (evalPlus a b).refines (evalPlus a' b')`.

This should hold under the err-strict definition of `evalPlus`
(`evalPlus .err _ = .err _`), but the proof obligation expands into
sixteen case checks on `(a, a', b, b')`. The shape: if `a = .err`,
`evalPlus a b = .err`, refines anything. If `a = a'` and `b = b'`,
straight equality. The cross-cases (e.g. `a = .err`, `b' = .err`) all
land on `.err`-refines-anything.

The lift is mechanical and is the analogue of `Mz/Strict.lean`'s
strictness lemmas re-cast against `refines` rather than against the
err-preserving strictness predicate. Out of scope for this module;
listed here so a future extension knows what to prove. -/

end Mz

import Mz.PrimEval
import Mz.Strict

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

This module defines two relations the design doc
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

The dual preorder (errors as top — "spurious errors permitted")
is not separately mechanized: the doc commits to `LegalEval`
(`Mz/Legal.lean`) as the SQL-faithful posture, and the dual
preorder's behavior is recovered from `refines` by argument swap
where needed.

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

/-! ## Compositionality of `refines` under err-strict primitives

The compositionality lemma — `a.refines a' → b.refines b' → (f a
b).refines (f a' b')` — is the gating prerequisite for lifting
`refines` from `Datum` to `Expr`: without it, a rewrite
`evalPlus (.err _) x ⊑ evalPlus 1 x` cannot be composed inside a
larger expression because `evalPlus`-of-refining-arguments has no
known relation to `evalPlus`-of-original-arguments.

The proof is the same for every err-propagating binary primitive:
if either input is `.err _`, the output is `.err _` (so refines
holds via `.err`-refines-anything); otherwise both refinements
must be equalities and the outputs are syntactically equal. The
generic form `refines_cong_binary` captures this; the specific
instances for `evalPlus` / `evalMinus` / `evalTimes` / `evalDivide`
/ `evalEq` / `evalLt` fall out as corollaries of the matching
`ErrPropagatingBinary` instance in `Mz/Strict.lean`. -/

/-- Generic compositionality: any err-propagating binary preserves
refinement. -/
theorem Datum.refines_cong_binary
    {f : Datum → Datum → Datum} (hf : ErrPropagatingBinary f)
    {a a' b b' : Datum}
    (ha : a.refines a') (hb : b.refines b') :
    (f a b).refines (f a' b') := by
  cases ha with
  | inl heq_a =>
    subst heq_a
    cases hb with
    | inl heq_b =>
      subst heq_b
      exact Datum.refines_refl _
    | inr hb_err =>
      exact Or.inr (hf.right a b hb_err)
  | inr ha_err =>
    exact Or.inr (hf.left a b ha_err)

/-- `evalNot` preserves refinement. `evalNot` is strict on `.err`. -/
theorem evalNot_refines_cong
    {a a' : Datum} (h : a.refines a') :
    (evalNot a).refines (evalNot a') := by
  cases h with
  | inl heq => subst heq; exact Datum.refines_refl _
  | inr ha_err =>
    cases a with
    | err _ => exact Or.inr trivial
    | bool _ => cases ha_err
    | int _ => cases ha_err
    | null => cases ha_err

/-- `evalPlus` preserves refinement. -/
theorem evalPlus_refines_cong
    {a a' b b' : Datum} (ha : a.refines a') (hb : b.refines b') :
    (evalPlus a b).refines (evalPlus a' b') :=
  Datum.refines_cong_binary evalPlus_errPropagating ha hb

/-- `evalMinus` preserves refinement. -/
theorem evalMinus_refines_cong
    {a a' b b' : Datum} (ha : a.refines a') (hb : b.refines b') :
    (evalMinus a b).refines (evalMinus a' b') :=
  Datum.refines_cong_binary evalMinus_errPropagating ha hb

/-- `evalTimes` preserves refinement. -/
theorem evalTimes_refines_cong
    {a a' b b' : Datum} (ha : a.refines a') (hb : b.refines b') :
    (evalTimes a b).refines (evalTimes a' b') :=
  Datum.refines_cong_binary evalTimes_errPropagating ha hb

/-- `evalDivide` preserves refinement. -/
theorem evalDivide_refines_cong
    {a a' b b' : Datum} (ha : a.refines a') (hb : b.refines b') :
    (evalDivide a b).refines (evalDivide a' b') :=
  Datum.refines_cong_binary evalDivide_errPropagating ha hb

/-- `evalEq` preserves refinement. -/
theorem evalEq_refines_cong
    {a a' b b' : Datum} (ha : a.refines a') (hb : b.refines b') :
    (evalEq a b).refines (evalEq a' b') :=
  Datum.refines_cong_binary evalEq_errPropagating ha hb

/-- `evalLt` preserves refinement. -/
theorem evalLt_refines_cong
    {a a' b b' : Datum} (ha : a.refines a') (hb : b.refines b') :
    (evalLt a b).refines (evalLt a' b') :=
  Datum.refines_cong_binary evalLt_errPropagating ha hb

/-! ## Connections between the relations

Two-way refinement collapses to error-set equivalence. Strict
equality is the strongest; two-way refinement and `eqErrSet`
coincide and form a strict-equality-respecting equivalence class on
errors; `refines` is a one-directional relaxation that permits
asymmetric rewrites. -/

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

/-! ## Pointers to mechanized counterexamples and lifts

Statements that were once recorded here as open counterexamples are
now mechanized elsewhere:

* `evalAnd` err / err non-commutativity at `=` is recovered under
  `eqErrSet` by `evalAnd_err_err_eqErrSet_comm` above. The strict
  failure is masked while only one `EvalError` variant exists; add
  a second variant and the failure fires.
* Bounded-`Int` associativity failure is mechanized as a live
  theorem in `Mz/EquivBounded.lean` (`evalPlusBounded_assoc_max_*`).
* `refines` compositionality under err-propagating binary primitives
  is mechanized at `Datum.refines_cong_binary` above plus per-op
  corollaries (`evalPlus_refines_cong`, etc.).
* Err-side predicate pushdown over `cross` is mechanized as the
  `filterOne_cross_pushdown_left_unsound` counterexample in
  `Mz/Collection.lean`; the recovery under refines + `SignOK` lives
  in `filter_cross_pushdown_left_refines`. -/

end Mz

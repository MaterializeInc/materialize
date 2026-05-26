import Mz.PrimEval
import Mz.Strict

/-!
# Equivalence relations on indexed `Datum`

`Datum.eqErrSet` and `Datum.refines` lifted to `Datum k`. Indexed
counterpart of `Mz/Equiv.lean`.

Each relation is per-kind: `eqErrSet : Datum k → Datum k → Prop`.
The indexed signatures of evaluators make compositionality
mechanical — operands of mismatched kinds are inexpressible. -/

namespace Mz


/-! ## Error-set equivalence -/

/-- Error-set equivalence on `Datum k`. -/
def Datum.eqErrSet {k : ColType} (a b : Datum k) : Prop :=
  a = b ∨ (a.IsErr ∧ b.IsErr)

theorem Datum.eqErrSet_refl {k : ColType} (d : Datum k) : d.eqErrSet d :=
  Or.inl rfl

theorem Datum.eqErrSet_symm {k : ColType} {a b : Datum k}
    (h : a.eqErrSet b) : b.eqErrSet a := by
  cases h with
  | inl h => exact Or.inl h.symm
  | inr h => exact Or.inr ⟨h.2, h.1⟩

theorem Datum.eqErrSet_of_eq {k : ColType} {a b : Datum k}
    (h : a = b) : a.eqErrSet b := Or.inl h

/-- `eqErrSet` is reflexive on err / err regardless of payload
mismatch. The cell that strict equality cannot reach. -/
theorem Datum.eqErrSet_err_err {k : ColType} (e₁ e₂ : EvalError) :
    Datum.eqErrSet (.err e₁ : Datum k) (.err e₂) :=
  Or.inr ⟨True.intro, True.intro⟩

/-! ## Counterexample: `evalAnd` is not strictly commutative on err / err

`evalAnd` on `Datum .bool` is left-biased on errors. With distinct
err payloads, strict equality fails; `eqErrSet` recovers
commutativity. -/

theorem evalAnd_err_err (e₁ e₂ : EvalError) :
    evalAnd (.err e₁) (.err e₂) = (.err e₁ : Datum .bool) := rfl

theorem evalAnd_err_err_swap (e₁ e₂ : EvalError) :
    evalAnd (.err e₂) (.err e₁) = (.err e₂ : Datum .bool) := rfl

/-- Under `eqErrSet`, `evalAnd` commutes on err / err inputs even
when the inner payloads differ. -/
theorem evalAnd_err_err_eqErrSet_comm (e₁ e₂ : EvalError) :
    (evalAnd (.err e₁) (.err e₂) : Datum .bool).eqErrSet
    (evalAnd (.err e₂) (.err e₁)) :=
  Or.inr ⟨True.intro, True.intro⟩

/-! ## Refinement preorder (errors as bottom) -/

/-- Refinement preorder on `Datum k`: `a ⊑ b` iff `a = b` or `a`
is an `.err _`. Errors are the least-defined element. -/
def Datum.refines {k : ColType} (a b : Datum k) : Prop :=
  a = b ∨ a.IsErr

theorem Datum.refines_refl {k : ColType} (d : Datum k) : d.refines d :=
  Or.inl rfl

theorem Datum.refines_of_eq {k : ColType} {a b : Datum k}
    (h : a = b) : a.refines b := Or.inl h

theorem Datum.err_refines {k : ColType} (e : EvalError) (d : Datum k) :
    Datum.refines (.err e) d := Or.inr True.intro

theorem Datum.refines_trans {k : ColType} {a b c : Datum k}
    (h₁ : a.refines b) (h₂ : b.refines c) : a.refines c := by
  cases h₁ with
  | inl h => subst h; exact h₂
  | inr ha => exact Or.inr ha

/-! ## Compositionality under err-propagating binary primitives -/

/-- Generic compositionality: an err-propagating binary preserves
refinement on its argument kinds. -/
theorem Datum.refines_cong_binary
    {k₁ k₂ k₃ : ColType} {f : Datum k₁ → Datum k₂ → Datum k₃}
    (hf : ErrPropagatingBinary f)
    {a a' : Datum k₁} {b b' : Datum k₂}
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

/-- `evalNot` preserves refinement on `Datum .bool`. -/
theorem evalNot_refines_cong {a a' : Datum .bool} (h : a.refines a') :
    (evalNot a).refines (evalNot a') := by
  cases h with
  | inl heq => subst heq; exact Datum.refines_refl _
  | inr ha_err =>
    cases a with
    | err _ => exact Or.inr trivial
    | bool _ => cases ha_err
    | null => cases ha_err

theorem evalPlus_refines_cong {a a' b b' : Datum .int}
    (ha : a.refines a') (hb : b.refines b') :
    (evalPlus a b).refines (evalPlus a' b') :=
  Datum.refines_cong_binary evalPlus_errPropagating ha hb

theorem evalMinus_refines_cong {a a' b b' : Datum .int}
    (ha : a.refines a') (hb : b.refines b') :
    (evalMinus a b).refines (evalMinus a' b') :=
  Datum.refines_cong_binary evalMinus_errPropagating ha hb

theorem evalTimes_refines_cong {a a' b b' : Datum .int}
    (ha : a.refines a') (hb : b.refines b') :
    (evalTimes a b).refines (evalTimes a' b') :=
  Datum.refines_cong_binary evalTimes_errPropagating ha hb

theorem evalDivide_refines_cong {a a' b b' : Datum .int}
    (ha : a.refines a') (hb : b.refines b') :
    (evalDivide a b).refines (evalDivide a' b') :=
  Datum.refines_cong_binary evalDivide_errPropagating ha hb

theorem evalEq_refines_cong {k : ColType} {a a' b b' : Datum k}
    (ha : a.refines a') (hb : b.refines b') :
    (evalEq a b).refines (evalEq a' b') :=
  Datum.refines_cong_binary evalEq_errPropagating ha hb

theorem evalLt_refines_cong {k : ColType} {a a' b b' : Datum k}
    (ha : a.refines a') (hb : b.refines b') :
    (evalLt a b).refines (evalLt a' b') :=
  Datum.refines_cong_binary evalLt_errPropagating ha hb

/-! ## Connections

Two-way refinement collapses to `eqErrSet`. -/

theorem Datum.eqErrSet_of_refines_both {k : ColType} {a b : Datum k}
    (h₁ : a.refines b) (h₂ : b.refines a) : a.eqErrSet b := by
  cases h₁ with
  | inl h => exact Or.inl h
  | inr ha =>
    cases h₂ with
    | inl h => exact Or.inl h.symm
    | inr hb => exact Or.inr ⟨ha, hb⟩

end Mz

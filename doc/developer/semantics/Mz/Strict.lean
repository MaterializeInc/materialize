import Mz.PrimEval

/-!
# Strict propagation

Err-propagation and null-propagation classes on the primitives.
Abstractions specialize to the indexed signatures:

* Unary classes target `Datum .bool → Datum .bool` (`evalNot`).
* Binary arithmetic classes target `Datum .int → Datum .int →
  Datum .int` (`evalPlus`, `evalMinus`, `evalTimes`, `evalDivide`).
* Binary comparison classes target `Datum k → Datum k → Datum
  .bool` (`evalEq`, `evalLt`), polymorphic in the operand kind.

Kind-fallthrough cases that an untyped model would handle via a
catch-all `.null` route don't exist in the indexed domains, so
proofs collapse to the three-case split (`.bool` for bool ops,
`.int` for arithmetic, `.null` and `.err _` universally). -/

namespace Mz


/-! ## Strictness predicates -/

/-- `f` is err-strict on `Datum .bool`: an `.err _` argument
forces the same `.err _` output. -/
def ErrStrictUnaryBool (f : Datum .bool → Datum .bool) : Prop :=
  ∀ e, f (.err e) = .err e

/-- `f` is err-propagating: an `.err _` in either argument forces
an `.err _` output. Stated kind-generically over operand kinds
`k₁`, `k₂` and result kind `k₃`. -/
structure ErrPropagatingBinary {k₁ k₂ k₃ : ColType}
    (f : Datum k₁ → Datum k₂ → Datum k₃) : Prop where
  left  : ∀ d₁ d₂, d₁.IsErr → (f d₁ d₂).IsErr
  right : ∀ d₁ d₂, d₂.IsErr → (f d₁ d₂).IsErr

/-- `f` is null-propagating in each position, provided the other
argument is not `.err _`. -/
structure NullPropagatingBinary {k₁ k₂ k₃ : ColType}
    (f : Datum k₁ → Datum k₂ → Datum k₃) : Prop where
  left  : ∀ d, ¬d.IsErr → f .null d = .null
  right : ∀ d, ¬d.IsErr → f d .null = .null

/-! ## Boolean fragment -/

theorem evalNot_errStrict : ErrStrictUnaryBool evalNot := fun _ => rfl

theorem evalIfThen_errStrict_condition
    {k : ColType} (e : EvalError) (dt de : Datum k) :
    evalIfThen (.err e) dt de = .err e := rfl

/-! ## Arithmetic instances -/

theorem evalPlus_errPropagating : ErrPropagatingBinary evalPlus where
  left := by
    intro d₁ d₂ h
    match d₁, h with
    | .err e, _ => show (evalPlus (.err e) d₂).IsErr
                   simp [evalPlus, Datum.IsErr]
  right := by
    intro d₁ d₂ h
    match d₂, h with
    | .err e, _ => cases d₁ <;> simp [evalPlus, Datum.IsErr]

theorem evalPlus_nullPropagating : NullPropagatingBinary evalPlus where
  left := by
    intro d h
    cases d with
    | int _ => rfl
    | null  => rfl
    | err e => exact absurd (by simp [Datum.IsErr] : (Datum.err (k := .int) e).IsErr) h
  right := by
    intro d h
    cases d with
    | int _ => rfl
    | null  => rfl
    | err e => exact absurd (by simp [Datum.IsErr] : (Datum.err (k := .int) e).IsErr) h

theorem evalMinus_errPropagating : ErrPropagatingBinary evalMinus where
  left := by
    intro d₁ d₂ h
    match d₁, h with
    | .err e, _ => show (evalMinus (.err e) d₂).IsErr
                   simp [evalMinus, Datum.IsErr]
  right := by
    intro d₁ d₂ h
    match d₂, h with
    | .err e, _ => cases d₁ <;> simp [evalMinus, Datum.IsErr]

theorem evalTimes_errPropagating : ErrPropagatingBinary evalTimes where
  left := by
    intro d₁ d₂ h
    match d₁, h with
    | .err e, _ => show (evalTimes (.err e) d₂).IsErr
                   simp [evalTimes, Datum.IsErr]
  right := by
    intro d₁ d₂ h
    match d₂, h with
    | .err e, _ => cases d₁ <;> simp [evalTimes, Datum.IsErr]

theorem evalDivide_errPropagating : ErrPropagatingBinary evalDivide where
  left := by
    intro d₁ d₂ h
    match d₁, h with
    | .err e, _ => show (evalDivide (.err e) d₂).IsErr
                   simp [evalDivide, Datum.IsErr]
  right := by
    intro d₁ d₂ h
    match d₂, h with
    | .err e, _ => cases d₁ <;> simp [evalDivide, Datum.IsErr]

/-! ## Comparison instances (kind-polymorphic) -/

theorem evalEq_errPropagating {k : ColType} :
    ErrPropagatingBinary (@evalEq k) where
  left := by
    intro d₁ d₂ h
    match d₁, h with
    | .err e, _ => show (evalEq (.err e) d₂).IsErr
                   simp [evalEq, Datum.IsErr]
  right := by
    intro d₁ d₂ h
    match d₂, h with
    | .err e, _ => cases d₁ <;> simp [evalEq, Datum.IsErr]

theorem evalLt_errPropagating {k : ColType} :
    ErrPropagatingBinary (@evalLt k) where
  left := by
    intro d₁ d₂ h
    match d₁, h with
    | .err e, _ => show (evalLt (.err e) d₂).IsErr
                   simp [evalLt, Datum.IsErr]
  right := by
    intro d₁ d₂ h
    match d₂, h with
    | .err e, _ => cases d₁ <;> simp [evalLt, Datum.IsErr]

/-! ## Negative results

`AND` and `OR` are not err-strict in either position; absorption
allows `.bool false`/`.bool true` to override an err operand. -/

theorem evalAnd_not_errStrict_left :
    ¬ ∀ (d : Datum .bool) (e : EvalError), evalAnd (.err e) d = .err e := by
  intro h
  have hh := h (.bool false) .divisionByZero
  simp [evalAnd] at hh

theorem evalAnd_not_errStrict_right :
    ¬ ∀ (d : Datum .bool) (e : EvalError), evalAnd d (.err e) = .err e := by
  intro h
  have hh := h (.bool false) .divisionByZero
  simp [evalAnd] at hh

theorem evalOr_not_errStrict_left :
    ¬ ∀ (d : Datum .bool) (e : EvalError), evalOr (.err e) d = .err e := by
  intro h
  have hh := h (.bool true) .divisionByZero
  simp [evalOr] at hh

theorem evalOr_not_errStrict_right :
    ¬ ∀ (d : Datum .bool) (e : EvalError), evalOr d (.err e) = .err e := by
  intro h
  have hh := h (.bool true) .divisionByZero
  simp [evalOr] at hh

end Mz

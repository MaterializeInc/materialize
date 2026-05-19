import Mz.PrimEval

/-!
# Strict propagation

A scalar function is *err-strict in position k* when supplying `.err`
at that position forces the output to be the same `.err`, regardless
of the other arguments. This is the cell-scoped analogue of
PostgreSQL's `STRICT` qualifier on `NULL`: in the four-valued lattice
of this skeleton, `err` plays the role `NULL` plays in PostgreSQL for
strict functions.

The boolean fragment exposes exactly one fully strict function
(`evalNot`) and one position-strict function (`evalIfThen`, strict
only in its condition). `evalAnd` and `evalOr` are *not* err-strict in
either position because `FALSE`/`TRUE` short-circuit absorbs the other
operand, including `err`. The corresponding negative results are
stated below; they ensure that the spec does not silently regress to
an "errors > everything" model.

`NULL`-strictness is captured by a separate predicate. The two
strictness flavors agree for most arithmetic / comparison operators
but disagree for `AND`, `OR`, `IfThen`, and `COALESCE`, which the
spec singles out for special treatment.
-/

namespace Mz

/-! ## Strictness predicates -/

/-- `f` is err-strict: an `err` argument forces an `err` output with
the same payload. -/
def ErrStrictUnary (f : Datum → Datum) : Prop :=
  ∀ e, f (.err e) = .err e

/-- `f` is err-strict in each argument position. The two positions
have independent witnesses; a function strict only in its left
argument is captured by `.left` alone.

This is the *payload-preserving* form: an `err` argument propagates
the *same* payload to the output. The boolean fragment's `evalNot`
satisfies it. Arithmetic operators do not satisfy `right` literally
because `evalPlus (.err e₁) (.err e₂) = .err e₁`, not `.err e₂`. See
`ErrPropagatingBinary` for the strictly weaker form that arithmetic
satisfies. -/
structure ErrStrictBinary (f : Datum → Datum → Datum) : Prop where
  left  : ∀ d e, f (.err e) d = .err e
  right : ∀ d e, f d (.err e) = .err e

/-- `f` is err-propagating: an `err` in either argument forces an
`err` output, but the output payload may depend on which input came
first. The four-valued lattice's `err`-absorbs-`null`-absorbs-`int`
ordering means arithmetic operators satisfy this weaker form even
though they break the payload-preserving `ErrStrictBinary`. -/
structure ErrPropagatingBinary (f : Datum → Datum → Datum) : Prop where
  left  : ∀ d₁ d₂, d₁.IsErr → (f d₁ d₂).IsErr
  right : ∀ d₁ d₂, d₂.IsErr → (f d₁ d₂).IsErr

/-- `f` is null-strict: a `null` argument forces a `null` output. -/
def NullStrictUnary (f : Datum → Datum) : Prop :=
  f .null = .null

/-- `f` is null-propagating in each position, provided the *other*
argument is not `.err`. The guard is necessary in the four-valued
lattice: `evalPlus .null (.err e) = .err e`, so the unguarded form
"`.null` in either position forces `.null`" would fail. The guard
captures the standard SQL/Materialize rule: in the absence of an
absorbing `err`, a `null` operand makes the result `null`. -/
structure NullPropagatingBinary (f : Datum → Datum → Datum) : Prop where
  left  : ∀ d, ¬d.IsErr → f .null d = .null
  right : ∀ d, ¬d.IsErr → f d .null = .null

/-! ## Concrete instances on the boolean fragment -/

theorem evalNot_errStrict : ErrStrictUnary evalNot := fun _ => rfl

theorem evalNot_nullStrict : NullStrictUnary evalNot := rfl

/-- The condition slot of `IfThen` is err-strict: an `err` condition
forces an `err` output, with the same payload, no matter what the
branches contain. The branch slots are *not* err-strict: when the
condition selects the other branch, the error in the unselected
branch has no effect on the output. -/
theorem evalIfThen_errStrict_condition (e : EvalError) (dt de : Datum) :
    evalIfThen (.err e) dt de = .err e := rfl

/-! ## Closure under composition

If both `f` and `g` are err-strict, so is `f ∘ g`. This is the
property an optimizer uses when it fuses a chain of strict scalar
functions into a single MFP expression: strict-in-strict is strict. -/

theorem ErrStrictUnary.comp {f g : Datum → Datum}
    (hf : ErrStrictUnary f) (hg : ErrStrictUnary g) :
    ErrStrictUnary (f ∘ g) := by
  intro e
  show f (g (.err e)) = .err e
  rw [hg e, hf e]

/-! ## Arithmetic instances

`evalPlus`, `evalMinus`, `evalTimes`, and `evalDivide` propagate `err`
in both positions and `null` in both positions (when the other
operand is not itself `err`). The four-valued lattice is in force:
`err > null > int`. These instances are the cell-scoped analogue of
SQL's "STRICT in NULL" qualifier, lifted to a setting where `err`
takes the dominant role. -/

theorem evalPlus_errPropagating : ErrPropagatingBinary evalPlus where
  left := by
    intro d₁ d₂ h
    match d₁, h with
    | .err e, _ =>
      show (evalPlus (.err e) d₂).IsErr
      simp [evalPlus, Datum.IsErr]
  right := by
    intro d₁ d₂ h
    match d₂, h with
    | .err e, _ => cases d₁ <;> simp [evalPlus, Datum.IsErr]

theorem evalPlus_nullPropagating : NullPropagatingBinary evalPlus where
  left := by
    intro d h
    cases d with
    | bool b => rfl
    | int  n => rfl
    | null   => rfl
    | err  e => exact absurd (by simp [Datum.IsErr] : (Datum.err e).IsErr) h
  right := by
    intro d h
    cases d with
    | bool b => rfl
    | int  n => rfl
    | null   => rfl
    | err  e => exact absurd (by simp [Datum.IsErr] : (Datum.err e).IsErr) h

theorem evalMinus_errPropagating : ErrPropagatingBinary evalMinus where
  left := by
    intro d₁ d₂ h
    match d₁, h with
    | .err e, _ =>
      show (evalMinus (.err e) d₂).IsErr
      simp [evalMinus, Datum.IsErr]
  right := by
    intro d₁ d₂ h
    match d₂, h with
    | .err e, _ => cases d₁ <;> simp [evalMinus, Datum.IsErr]

theorem evalMinus_nullPropagating : NullPropagatingBinary evalMinus where
  left := by
    intro d h
    cases d with
    | bool b => rfl
    | int  n => rfl
    | null   => rfl
    | err  e => exact absurd (by simp [Datum.IsErr] : (Datum.err e).IsErr) h
  right := by
    intro d h
    cases d with
    | bool b => rfl
    | int  n => rfl
    | null   => rfl
    | err  e => exact absurd (by simp [Datum.IsErr] : (Datum.err e).IsErr) h

theorem evalTimes_errPropagating : ErrPropagatingBinary evalTimes where
  left := by
    intro d₁ d₂ h
    match d₁, h with
    | .err e, _ =>
      show (evalTimes (.err e) d₂).IsErr
      simp [evalTimes, Datum.IsErr]
  right := by
    intro d₁ d₂ h
    match d₂, h with
    | .err e, _ => cases d₁ <;> simp [evalTimes, Datum.IsErr]

theorem evalTimes_nullPropagating : NullPropagatingBinary evalTimes where
  left := by
    intro d h
    cases d with
    | bool b => rfl
    | int  n => rfl
    | null   => rfl
    | err  e => exact absurd (by simp [Datum.IsErr] : (Datum.err e).IsErr) h
  right := by
    intro d h
    cases d with
    | bool b => rfl
    | int  n => rfl
    | null   => rfl
    | err  e => exact absurd (by simp [Datum.IsErr] : (Datum.err e).IsErr) h

/-- `evalDivide` propagates `err` from either side. The output may be
`.err .divisionByZero` rather than the input payload when the divisor
is `.int 0`, but the input err always wins when present. -/
theorem evalDivide_errPropagating : ErrPropagatingBinary evalDivide where
  left := by
    intro d₁ d₂ h
    match d₁, h with
    | .err e, _ =>
      show (evalDivide (.err e) d₂).IsErr
      simp [evalDivide, Datum.IsErr]
  right := by
    intro d₁ d₂ h
    match d₂, h with
    | .err e, _ => cases d₁ <;> simp [evalDivide, Datum.IsErr]

theorem evalDivide_nullPropagating : NullPropagatingBinary evalDivide where
  left := by
    intro d h
    cases d with
    | bool b => rfl
    | int  n => rfl
    | null   => rfl
    | err  e => exact absurd (by simp [Datum.IsErr] : (Datum.err e).IsErr) h
  right := by
    intro d h
    cases d with
    | bool b => rfl
    | int  n => rfl
    | null   => rfl
    | err  e => exact absurd (by simp [Datum.IsErr] : (Datum.err e).IsErr) h

/-! ## Comparison instances

`evalEq` and `evalLt` mirror the arithmetic operators in their
propagation behavior: err-strict in both positions, null-strict in
both positions (when the other side is not err). The output is
always `.bool`, `.null`, or `.err`, so the operators chain cleanly
into the boolean-logic fragment. -/

theorem evalEq_errPropagating : ErrPropagatingBinary evalEq where
  left := by
    intro d₁ d₂ h
    match d₁, h with
    | .err e, _ =>
      show (evalEq (.err e) d₂).IsErr
      simp [evalEq, Datum.IsErr]
  right := by
    intro d₁ d₂ h
    match d₂, h with
    | .err e, _ => cases d₁ <;> simp [evalEq, Datum.IsErr]

theorem evalEq_nullPropagating : NullPropagatingBinary evalEq where
  left := by
    intro d h
    cases d with
    | bool b => rfl
    | int  n => rfl
    | null   => rfl
    | err  e => exact absurd (by simp [Datum.IsErr] : (Datum.err e).IsErr) h
  right := by
    intro d h
    cases d with
    | bool b => rfl
    | int  n => rfl
    | null   => rfl
    | err  e => exact absurd (by simp [Datum.IsErr] : (Datum.err e).IsErr) h

theorem evalLt_errPropagating : ErrPropagatingBinary evalLt where
  left := by
    intro d₁ d₂ h
    match d₁, h with
    | .err e, _ =>
      show (evalLt (.err e) d₂).IsErr
      simp [evalLt, Datum.IsErr]
  right := by
    intro d₁ d₂ h
    match d₂, h with
    | .err e, _ => cases d₁ <;> simp [evalLt, Datum.IsErr]

theorem evalLt_nullPropagating : NullPropagatingBinary evalLt where
  left := by
    intro d h
    cases d with
    | bool b => rfl
    | int  n => rfl
    | null   => rfl
    | err  e => exact absurd (by simp [Datum.IsErr] : (Datum.err e).IsErr) h
  right := by
    intro d h
    cases d with
    | bool b => rfl
    | int  n => rfl
    | null   => rfl
    | err  e => exact absurd (by simp [Datum.IsErr] : (Datum.err e).IsErr) h

/-! ## Negative results

`AND` and `OR` are not err-strict in either position. The short
counterexamples below also serve as canonical regression tests: a
future change to `evalAnd` that accidentally promotes
`FALSE AND ERROR` to `ERROR` would break exactly these proofs. -/

theorem evalAnd_not_errStrict_left :
    ¬ ∀ d e, evalAnd (.err e) d = .err e := by
  intro h
  have hh := h (.bool false) .placeholder
  simp [evalAnd] at hh

theorem evalAnd_not_errStrict_right :
    ¬ ∀ d e, evalAnd d (.err e) = .err e := by
  intro h
  have hh := h (.bool false) .placeholder
  simp [evalAnd] at hh

theorem evalOr_not_errStrict_left :
    ¬ ∀ d e, evalOr (.err e) d = .err e := by
  intro h
  have hh := h (.bool true) .placeholder
  simp [evalOr] at hh

theorem evalOr_not_errStrict_right :
    ¬ ∀ d e, evalOr d (.err e) = .err e := by
  intro h
  have hh := h (.bool true) .placeholder
  simp [evalOr] at hh

end Mz

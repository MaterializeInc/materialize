import Mz.Eval

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
argument is captured by `.left` alone. -/
structure ErrStrictBinary (f : Datum → Datum → Datum) : Prop where
  left  : ∀ d e, f (.err e) d = .err e
  right : ∀ d e, f d (.err e) = .err e

/-- `f` is null-strict: a `null` argument forces a `null` output. -/
def NullStrictUnary (f : Datum → Datum) : Prop :=
  f .null = .null

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

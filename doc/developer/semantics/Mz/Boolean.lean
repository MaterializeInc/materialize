import Mz.Eval

/-!
# Boolean truth tables

Cell-by-cell proofs that `evalAnd` and `evalOr` realize the truth
tables stated in
`doc/developer/design/20260517_error_handling_semantics.md`.

Each cell is its own theorem. The redundancy is deliberate: if a
future semantic change touches one cell, exactly one theorem breaks,
making the change reviewable in isolation.

All proofs reduce to `rfl` because `evalAnd` and `evalOr` are defined
by pattern matching and the cases are exhaustive constructor
applications. The `cases d <;> rfl` form is used where a single cell
quantifies over an arbitrary `Datum` (e.g. `false` absorbs everything).
-/

namespace Mz

/-! ## AND -/

theorem and_false_left (d : Datum) : evalAnd (.bool false) d = .bool false := by
  cases d <;> rfl

theorem and_false_right (d : Datum) : evalAnd d (.bool false) = .bool false := by
  cases d with
  | bool b => cases b <;> rfl
  | null   => rfl
  | err _  => rfl

theorem and_true_true : evalAnd (.bool true) (.bool true) = .bool true := rfl
theorem and_true_null : evalAnd (.bool true) .null = .null := rfl
theorem and_null_true : evalAnd .null (.bool true) = .null := rfl
theorem and_null_null : evalAnd .null .null = .null := rfl

theorem and_true_err (e : EvalError) :
    evalAnd (.bool true) (.err e) = .err e := rfl
theorem and_err_true (e : EvalError) :
    evalAnd (.err e) (.bool true) = .err e := rfl
theorem and_null_err (e : EvalError) :
    evalAnd .null (.err e) = .err e := rfl
theorem and_err_null (e : EvalError) :
    evalAnd (.err e) .null = .err e := rfl
theorem and_err_err (e₁ e₂ : EvalError) :
    evalAnd (.err e₁) (.err e₂) = .err e₁ := rfl

/-! ## OR -/

theorem or_true_left (d : Datum) : evalOr (.bool true) d = .bool true := by
  cases d <;> rfl

theorem or_true_right (d : Datum) : evalOr d (.bool true) = .bool true := by
  cases d with
  | bool b => cases b <;> rfl
  | null   => rfl
  | err _  => rfl

theorem or_false_false : evalOr (.bool false) (.bool false) = .bool false := rfl
theorem or_false_null  : evalOr (.bool false) .null = .null := rfl
theorem or_null_false  : evalOr .null (.bool false) = .null := rfl
theorem or_null_null   : evalOr .null .null = .null := rfl

theorem or_false_err (e : EvalError) :
    evalOr (.bool false) (.err e) = .err e := rfl
theorem or_err_false (e : EvalError) :
    evalOr (.err e) (.bool false) = .err e := rfl
theorem or_null_err (e : EvalError) :
    evalOr .null (.err e) = .err e := rfl
theorem or_err_null (e : EvalError) :
    evalOr (.err e) .null = .err e := rfl
theorem or_err_err (e₁ e₂ : EvalError) :
    evalOr (.err e₁) (.err e₂) = .err e₁ := rfl

end Mz

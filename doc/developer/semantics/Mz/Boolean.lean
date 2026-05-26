import Mz.PrimEval

/-!
# Boolean truth tables (indexed)

Cell-by-cell proofs that `evalAnd` / `evalOr` / `evalNot` realize
the truth tables on `Datum .bool`. Indexed counterpart of
`Mz/Boolean.lean`.

All proofs reduce to `rfl`. The codomain of every evaluator is
already closed to `Datum .bool` by the indexing; no `¬IsInt`
hypothesis or catch-all `.null` route to consider. -/

namespace Mz


/-! ## AND -/

theorem and_false_left (d : Datum .bool) : evalAnd (.bool false) d = .bool false := by
  cases d <;> rfl

theorem and_false_right (d : Datum .bool) : evalAnd d (.bool false) = .bool false := by
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

theorem or_true_left (d : Datum .bool) : evalOr (.bool true) d = .bool true := by
  cases d <;> rfl

theorem or_true_right (d : Datum .bool) : evalOr d (.bool true) = .bool true := by
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

/-! ## NOT -/

theorem not_true  : evalNot (.bool true)  = .bool false := rfl
theorem not_false : evalNot (.bool false) = .bool true  := rfl
theorem not_null  : evalNot .null = .null := rfl
theorem not_err (e : EvalError) : evalNot (.err e) = .err e := rfl

/-- `Not` is involutive on `Datum .bool`. Untyped counterpart in
`Mz/Boolean.lean` requires `¬d.IsInt`; here the indexing rules
out the `.int` case at the type level and the hypothesis
disappears. -/
theorem not_not (d : Datum .bool) : evalNot (evalNot d) = d := by
  cases d with
  | bool b => cases b <;> rfl
  | null   => rfl
  | err _  => rfl

end Mz

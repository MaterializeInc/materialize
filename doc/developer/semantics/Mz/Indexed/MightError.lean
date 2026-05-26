import Mz.Indexed.Eval

/-!
# `might_error` analyzer + soundness (indexed)

Conservative analyzer that returns `true` when an `Expr sch k`
might evaluate to `.err _`. Indexed counterpart of
`Mz/MightError.lean`.

The kind discipline of the indexed model removes the
type-mismatch case from every primitive's error sources. What's
left:

* `.lit (.err _)` — literal error.
* `.divide _ _` — divide-by-zero on `.int 0` divisor.
* Variadic short-circuit absorbs err on the "absorbing" side
  (`evalAndN [.bool false, .err _] = .bool false`), so the
  analyzer is conservative — if any operand might err, the
  variadic might err.
* Operator soundness in the err-propagation chain: errs propagate
  through strict primitives. -/

namespace Mz.Indexed

open Mz

/-! ## Per-primitive helper lemmas

Error-free inputs yield error-free outputs. With indexed Datum the
case splits are halved (no `.int` case in `Datum .bool`, etc.). -/

theorem evalNot_not_err {d : Datum .bool} (h : ¬d.IsErr) :
    ¬(evalNot d).IsErr := by
  cases d with
  | bool b => cases b <;> (intro h; cases h)
  | null   => intro h; cases h
  | err _  => exact (h trivial).elim

theorem evalAnd_not_err {d₁ d₂ : Datum .bool}
    (h₁ : ¬d₁.IsErr) (h₂ : ¬d₂.IsErr) :
    ¬(evalAnd d₁ d₂).IsErr := by
  cases d₁ with
  | bool b₁ =>
    cases d₂ with
    | bool b₂ => cases b₁ <;> cases b₂ <;> (intro h; cases h)
    | null    => cases b₁ <;> (intro h; cases h)
    | err _   => exact (h₂ trivial).elim
  | null =>
    cases d₂ with
    | bool b₂ => cases b₂ <;> (intro h; cases h)
    | null    => intro h; cases h
    | err _   => exact (h₂ trivial).elim
  | err _ => exact (h₁ trivial).elim

theorem evalOr_not_err {d₁ d₂ : Datum .bool}
    (h₁ : ¬d₁.IsErr) (h₂ : ¬d₂.IsErr) :
    ¬(evalOr d₁ d₂).IsErr := by
  cases d₁ with
  | bool b₁ =>
    cases d₂ with
    | bool b₂ => cases b₁ <;> cases b₂ <;> (intro h; cases h)
    | null    => cases b₁ <;> (intro h; cases h)
    | err _   => exact (h₂ trivial).elim
  | null =>
    cases d₂ with
    | bool b₂ => cases b₂ <;> (intro h; cases h)
    | null    => intro h; cases h
    | err _   => exact (h₂ trivial).elim
  | err _ => exact (h₁ trivial).elim

theorem evalPlus_not_err {d₁ d₂ : Datum .int}
    (h₁ : ¬d₁.IsErr) (h₂ : ¬d₂.IsErr) :
    ¬(evalPlus d₁ d₂).IsErr := by
  cases d₁ with
  | int _ =>
    cases d₂ with
    | int _ => intro h; cases h
    | null  => intro h; cases h
    | err _ => exact (h₂ trivial).elim
  | null =>
    cases d₂ with
    | int _ => intro h; cases h
    | null  => intro h; cases h
    | err _ => exact (h₂ trivial).elim
  | err _ => exact (h₁ trivial).elim

theorem evalMinus_not_err {d₁ d₂ : Datum .int}
    (h₁ : ¬d₁.IsErr) (h₂ : ¬d₂.IsErr) :
    ¬(evalMinus d₁ d₂).IsErr := by
  cases d₁ with
  | int _ =>
    cases d₂ with
    | int _ => intro h; cases h
    | null  => intro h; cases h
    | err _ => exact (h₂ trivial).elim
  | null =>
    cases d₂ with
    | int _ => intro h; cases h
    | null  => intro h; cases h
    | err _ => exact (h₂ trivial).elim
  | err _ => exact (h₁ trivial).elim

theorem evalTimes_not_err {d₁ d₂ : Datum .int}
    (h₁ : ¬d₁.IsErr) (h₂ : ¬d₂.IsErr) :
    ¬(evalTimes d₁ d₂).IsErr := by
  cases d₁ with
  | int _ =>
    cases d₂ with
    | int _ => intro h; cases h
    | null  => intro h; cases h
    | err _ => exact (h₂ trivial).elim
  | null =>
    cases d₂ with
    | int _ => intro h; cases h
    | null  => intro h; cases h
    | err _ => exact (h₂ trivial).elim
  | err _ => exact (h₁ trivial).elim

theorem evalEq_not_err {k : ColType} {d₁ d₂ : Datum k}
    (h₁ : ¬d₁.IsErr) (h₂ : ¬d₂.IsErr) :
    ¬(evalEq d₁ d₂).IsErr := by
  cases d₁ with
  | bool b₁ =>
    cases d₂ with
    | bool b₂ => intro h; cases h
    | null    => intro h; cases h
    | err _   => exact (h₂ trivial).elim
  | int _ =>
    cases d₂ with
    | int _ => intro h; cases h
    | null  => intro h; cases h
    | err _ => exact (h₂ trivial).elim
  | null =>
    cases d₂ with
    | bool _ => intro h; cases h
    | int _  => intro h; cases h
    | null   => intro h; cases h
    | err _  => exact (h₂ trivial).elim
  | err _ => exact (h₁ trivial).elim

theorem evalLt_not_err {k : ColType} {d₁ d₂ : Datum k}
    (h₁ : ¬d₁.IsErr) (h₂ : ¬d₂.IsErr) :
    ¬(evalLt d₁ d₂).IsErr := by
  cases d₁ with
  | bool b₁ =>
    cases d₂ with
    | bool b₂ => intro h; cases h
    | null    => intro h; cases h
    | err _   => exact (h₂ trivial).elim
  | int _ =>
    cases d₂ with
    | int _ => intro h; cases h
    | null  => intro h; cases h
    | err _ => exact (h₂ trivial).elim
  | null =>
    cases d₂ with
    | bool _ => intro h; cases h
    | int _  => intro h; cases h
    | null   => intro h; cases h
    | err _  => exact (h₂ trivial).elim
  | err _ => exact (h₁ trivial).elim

theorem evalIfThen_not_err {k : ColType}
    {c : Datum .bool} {dt de : Datum k}
    (hc : ¬c.IsErr) (ht : ¬dt.IsErr) (he : ¬de.IsErr) :
    ¬(evalIfThen c dt de).IsErr := by
  cases c with
  | bool b =>
    cases b with
    | false => show ¬(evalIfThen (.bool false) dt de).IsErr
               simp only [evalIfThen]; exact he
    | true  => show ¬(evalIfThen (.bool true) dt de).IsErr
               simp only [evalIfThen]; exact ht
  | null   => intro h; cases h
  | err _  => exact (hc trivial).elim

/-! ## Static analyzer

`might_error : Expr sch k → Bool`. Mutual with `argsMightError`
for variadics. Returns `false` only when the type-system has
proven no err output is possible. Errors come from:

* `.lit (.err _)` — literal.
* `.divide` — always conservatively `true` (divide-by-zero
  depends on the divisor's runtime value).
* Operator propagation — if any operand might err, the operator
  might err.

Conservative: a `false` answer is a soundness guarantee; a `true`
answer is uncertainty. -/

mutual
  def Expr.might_error {n : Nat} {sch : Schema n} :
      {k : ColType} → Expr sch k → Bool
    | _, .lit (.err _) => true
    | _, .lit _        => false
    | _, .col _        => false
    | _, .not a        => Expr.might_error a
    | _, .plus a b     => Expr.might_error a || Expr.might_error b
    | _, .minus a b    => Expr.might_error a || Expr.might_error b
    | _, .times a b    => Expr.might_error a || Expr.might_error b
    | _, .divide _ _   => true
    | _, .eq a b       => Expr.might_error a || Expr.might_error b
    | _, .lt a b       => Expr.might_error a || Expr.might_error b
    | _, .ifThen c t e =>
      Expr.might_error c || Expr.might_error t || Expr.might_error e
    | _, .andN args    => ExprList.argsMightError args
    | _, .orN args     => ExprList.argsMightError args
    | _, .coalesce args => ExprList.argsMightError args

  /-- Conservative `any` over the variadic operands. -/
  def ExprList.argsMightError {n : Nat} {sch : Schema n} :
      {k : ColType} → ExprList sch k → Bool
    | _, .nil       => false
    | _, .cons a as => Expr.might_error a || ExprList.argsMightError as
end

/-! ## Environment error-freeness -/

/-- A typed environment is error-free when every cell is not an
`.err _`. -/
def EnvErrFree {n : Nat} {sch : Schema n} (env : Env sch) : Prop :=
  ∀ i : Fin n, ¬(env i).IsErr

end Mz.Indexed

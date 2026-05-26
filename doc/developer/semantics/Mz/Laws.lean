import Mz.PrimEval

/-!
# Algebraic laws (indexed)

Laws over `evalAnd`, `evalOr`, and `evalNot` constrained to
`Datum .bool`. Indexed counterpart of `Mz/Laws.lean`.

Compared with the untyped model:

* Identity laws (`evalAnd_true_left`, `evalAnd_true_right`,
  `evalOr_false_*`) drop their `¬d.IsInt` hypotheses — the
  indexed `Datum .bool` rules out `.int` at the type level.
* Idempotence (`evalAnd_idem`, `evalOr_idem`) drops `¬d.IsInt`
  for the same reason.
* Conditional commutativity (`evalAnd_comm_of_no_err`,
  `evalOr_comm_of_no_err`) keeps only the genuine `¬IsErr`
  hypotheses (a real cell-content concern, not a
  type-discipline artifact). -/

namespace Mz


/-! ## Identity laws -/

theorem evalAnd_true_left (d : Datum .bool) :
    evalAnd (.bool true) d = d := by
  cases d with
  | bool b => cases b <;> rfl
  | null   => rfl
  | err _  => rfl

theorem evalAnd_true_right (d : Datum .bool) :
    evalAnd d (.bool true) = d := by
  cases d with
  | bool b => cases b <;> rfl
  | null   => rfl
  | err _  => rfl

theorem evalOr_false_left (d : Datum .bool) :
    evalOr (.bool false) d = d := by
  cases d with
  | bool b => cases b <;> rfl
  | null   => rfl
  | err _  => rfl

theorem evalOr_false_right (d : Datum .bool) :
    evalOr d (.bool false) = d := by
  cases d with
  | bool b => cases b <;> rfl
  | null   => rfl
  | err _  => rfl

/-! ## Idempotence -/

theorem evalAnd_idem (d : Datum .bool) : evalAnd d d = d := by
  cases d with
  | bool b => cases b <;> rfl
  | null   => rfl
  | err _  => rfl

theorem evalOr_idem (d : Datum .bool) : evalOr d d = d := by
  cases d with
  | bool b => cases b <;> rfl
  | null   => rfl
  | err _  => rfl

/-! ## Conditional commutativity -/

theorem evalAnd_comm_of_no_err
    {d₁ d₂ : Datum .bool} (h₁ : ¬d₁.IsErr) (h₂ : ¬d₂.IsErr) :
    evalAnd d₁ d₂ = evalAnd d₂ d₁ := by
  cases d₁ with
  | bool b₁ =>
    cases d₂ with
    | bool b₂ => cases b₁ <;> cases b₂ <;> rfl
    | null    => cases b₁ <;> rfl
    | err _   => exact (h₂ trivial).elim
  | null =>
    cases d₂ with
    | bool b₂ => cases b₂ <;> rfl
    | null    => rfl
    | err _   => exact (h₂ trivial).elim
  | err _ => exact (h₁ trivial).elim

theorem evalOr_comm_of_no_err
    {d₁ d₂ : Datum .bool} (h₁ : ¬d₁.IsErr) (h₂ : ¬d₂.IsErr) :
    evalOr d₁ d₂ = evalOr d₂ d₁ := by
  cases d₁ with
  | bool b₁ =>
    cases d₂ with
    | bool b₂ => cases b₁ <;> cases b₂ <;> rfl
    | null    => cases b₁ <;> rfl
    | err _   => exact (h₂ trivial).elim
  | null =>
    cases d₂ with
    | bool b₂ => cases b₂ <;> rfl
    | null    => rfl
    | err _   => exact (h₂ trivial).elim
  | err _ => exact (h₁ trivial).elim

/-! ## Arithmetic commutativity (with err-payload guard)

`evalPlus` on `Datum .int × Datum .int → Datum .int` commutes
modulo err-payload disagreement. Same shape as the boolean
fragment's `_comm_of_no_err` — the err-payload concern is the
only obstruction. -/

theorem evalPlus_comm_of_no_err
    {a b : Datum .int} (h₁ : ¬a.IsErr) (h₂ : ¬b.IsErr) :
    evalPlus a b = evalPlus b a := by
  cases a with
  | int n =>
    cases b with
    | int m => simp only [evalPlus]; rw [Int.add_comm]
    | null  => rfl
    | err _ => exact (h₂ trivial).elim
  | null =>
    cases b with
    | int _ => rfl
    | null  => rfl
    | err _ => exact (h₂ trivial).elim
  | err _ => exact (h₁ trivial).elim

theorem evalTimes_comm_of_no_err
    {a b : Datum .int} (h₁ : ¬a.IsErr) (h₂ : ¬b.IsErr) :
    evalTimes a b = evalTimes b a := by
  cases a with
  | int n =>
    cases b with
    | int m => simp only [evalTimes]; rw [Int.mul_comm]
    | null  => rfl
    | err _ => exact (h₂ trivial).elim
  | null =>
    cases b with
    | int _ => rfl
    | null  => rfl
    | err _ => exact (h₂ trivial).elim
  | err _ => exact (h₁ trivial).elim

end Mz

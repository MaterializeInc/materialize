import Mz.Eval
import Mz.MightError

/-!
# Algebraic laws

Laws over `evalAnd` and `evalOr` that constrain optimizer rewrites.

The laws here are deliberately weaker than the unconditional laws of
two-valued boolean algebra. The current Materialize runtime
(`src/expr/src/scalar/func/variadic.rs`) lets `FALSE` absorb `ERR` in
`AND` and `TRUE` absorb `ERR` in `OR`, but does not let either of
`NULL` or another `ERR` absorb `ERR`. Consequently:

* **Idempotence** holds unconditionally for every cell of the value
  lattice, including `err`. The result is *the same* error, not an
  arbitrary one — this matters for rewrites that fold `x AND x` to
  `x`.
* **Commutativity** holds unless both operands are errors with
  distinct payloads, because `evalAnd (.err e₁) (.err e₂)` selects
  `e₁` while the swapped form selects `e₂`. The conditional form
  guards on `¬IsErr` for at least one operand, which is what an
  optimizer that has run `might_error` analysis can prove.

Associativity is not stated here. It fails over the four-valued
lattice in the presence of distinct errors and would require a
more delicate hypothesis; it is left for a later iteration that
introduces a partial-order on errors.
-/

namespace Mz

/-! ## Identity laws

`TRUE` is the two-sided identity for `evalAnd`; `FALSE` is the
two-sided identity for `evalOr`. The proofs verify each cell of the
non-identity argument. Identities are the seed values used by the
variadic fold in `Mz/Variadic.lean`. -/

theorem evalAnd_true_left (d : Datum) : evalAnd (.bool true) d = d := by
  cases d with
  | bool b => cases b <;> rfl
  | int  _ => rfl
  | null   => rfl
  | err _  => rfl

theorem evalAnd_true_right (d : Datum) : evalAnd d (.bool true) = d := by
  cases d with
  | bool b => cases b <;> rfl
  | int  _ => rfl
  | null   => rfl
  | err _  => rfl

theorem evalOr_false_left (d : Datum) : evalOr (.bool false) d = d := by
  cases d with
  | bool b => cases b <;> rfl
  | int  _ => rfl
  | null   => rfl
  | err _  => rfl

theorem evalOr_false_right (d : Datum) : evalOr d (.bool false) = d := by
  cases d with
  | bool b => cases b <;> rfl
  | int  _ => rfl
  | null   => rfl
  | err _  => rfl

/-! ## Idempotence -/

theorem evalAnd_idem (d : Datum) : evalAnd d d = d := by
  cases d with
  | bool b => cases b <;> rfl
  | int  n => show (if n = n then Datum.int n else Datum.null) = .int n
              rw [if_pos rfl]
  | null   => rfl
  | err _  => rfl

theorem evalOr_idem (d : Datum) : evalOr d d = d := by
  cases d with
  | bool b => cases b <;> rfl
  | int  n => show (if n = n then Datum.int n else Datum.null) = .int n
              rw [if_pos rfl]
  | null   => rfl
  | err _  => rfl

/-! ## Conditional commutativity

`evalAnd` and `evalOr` commute whenever neither operand is an error.
The premise is stronger than strictly necessary — commutativity also
holds when exactly one operand is an error — but the symmetric form
matches the shape an optimizer typically carries (a `might_error`
flag per operand). A weaker premise can be added later if a transform
demands it. -/

theorem evalAnd_comm_of_no_err
    {d₁ d₂ : Datum} (h₁ : ¬d₁.IsErr) (h₂ : ¬d₂.IsErr) :
    evalAnd d₁ d₂ = evalAnd d₂ d₁ := by
  cases d₁ with
  | bool b₁ =>
    cases d₂ with
    | bool b₂ => cases b₁ <;> cases b₂ <;> rfl
    | int  _  => cases b₁ <;> rfl
    | null    => cases b₁ <;> rfl
    | err _   => exact (h₂ trivial).elim
  | int n =>
    cases d₂ with
    | bool b₂ => cases b₂ <;> rfl
    | int  m  =>
      by_cases h : n = m
      · subst h; rfl
      · have h' : ¬m = n := fun h_eq => h h_eq.symm
        show (if n = m then Datum.int n else Datum.null)
            = (if m = n then Datum.int m else Datum.null)
        rw [if_neg h, if_neg h']
    | null    => rfl
    | err _   => exact (h₂ trivial).elim
  | null =>
    cases d₂ with
    | bool b₂ => cases b₂ <;> rfl
    | int  _  => rfl
    | null    => rfl
    | err _   => exact (h₂ trivial).elim
  | err _ => exact (h₁ trivial).elim

theorem evalOr_comm_of_no_err
    {d₁ d₂ : Datum} (h₁ : ¬d₁.IsErr) (h₂ : ¬d₂.IsErr) :
    evalOr d₁ d₂ = evalOr d₂ d₁ := by
  cases d₁ with
  | bool b₁ =>
    cases d₂ with
    | bool b₂ => cases b₁ <;> cases b₂ <;> rfl
    | int  _  => cases b₁ <;> rfl
    | null    => cases b₁ <;> rfl
    | err _   => exact (h₂ trivial).elim
  | int n =>
    cases d₂ with
    | bool b₂ => cases b₂ <;> rfl
    | int  m  =>
      by_cases h : n = m
      · subst h; rfl
      · have h' : ¬m = n := fun h_eq => h h_eq.symm
        show (if n = m then Datum.int n else Datum.null)
            = (if m = n then Datum.int m else Datum.null)
        rw [if_neg h, if_neg h']
    | null    => rfl
    | err _   => exact (h₂ trivial).elim
  | null =>
    cases d₂ with
    | bool b₂ => cases b₂ <;> rfl
    | int  _  => rfl
    | null    => rfl
    | err _   => exact (h₂ trivial).elim
  | err _ => exact (h₁ trivial).elim

/-! ## Reorder safety on `Expr`

These corollaries lift the conditional commutativity laws above
through `eval`. They are the precondition an optimizer must check
before swapping conjuncts: both operands must be statically proved
error-free by `might_error`, and the surrounding environment must be
error-free. -/

theorem eval_and_comm_of_no_might_error
    {a b : Expr} {env : Env}
    (ha : ¬(a.might_error = true)) (hb : ¬(b.might_error = true))
    (hEnv : env.ErrFree) :
    eval env (.and a b) = eval env (.and b a) := by
  have hae := might_error_sound a env ha hEnv
  have hbe := might_error_sound b env hb hEnv
  show eval env (Expr.andN [a, b]) = eval env (Expr.andN [b, a])
  simp only [eval, List.map_cons, List.map_nil, evalAndN]
  rw [evalAnd_true_right, evalAnd_true_right]
  exact evalAnd_comm_of_no_err hae hbe

theorem eval_or_comm_of_no_might_error
    {a b : Expr} {env : Env}
    (ha : ¬(a.might_error = true)) (hb : ¬(b.might_error = true))
    (hEnv : env.ErrFree) :
    eval env (.or a b) = eval env (.or b a) := by
  have hae := might_error_sound a env ha hEnv
  have hbe := might_error_sound b env hb hEnv
  show eval env (Expr.orN [a, b]) = eval env (Expr.orN [b, a])
  simp only [eval, List.map_cons, List.map_nil, evalOrN]
  rw [evalOr_false_right, evalOr_false_right]
  exact evalOr_comm_of_no_err hae hbe

end Mz

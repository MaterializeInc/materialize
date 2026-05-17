import Mz.Eval

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

/-! ## Idempotence -/

theorem evalAnd_idem (d : Datum) : evalAnd d d = d := by
  cases d with
  | bool b => cases b <;> rfl
  | null   => rfl
  | err _  => rfl

theorem evalOr_idem (d : Datum) : evalOr d d = d := by
  cases d with
  | bool b => cases b <;> rfl
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
    | null    => cases b₁ <;> rfl
    | err _   => exact (h₂ trivial).elim
  | null =>
    cases d₂ with
    | bool b₂ => cases b₂ <;> rfl
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
    | null    => cases b₁ <;> rfl
    | err _   => exact (h₂ trivial).elim
  | null =>
    cases d₂ with
    | bool b₂ => cases b₂ <;> rfl
    | null    => rfl
    | err _   => exact (h₂ trivial).elim
  | err _ => exact (h₁ trivial).elim

end Mz

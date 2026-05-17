import Mz.Eval

/-!
# `might_error` static analyzer and soundness

A conservative analyzer that returns `true` when an `Expr` might
evaluate to an `err`, plus the soundness theorem that justifies its
use by the optimizer: if `might_error e` is `false` and the
surrounding environment carries no errors, then `eval env e` is not
an error.

The analyzer in `src/expr/src/scalar.rs::might_error` is more refined
(it knows about `ErrorIfNull` and literal errors). The skeleton
version is purely structural; tightening it later is additive work
that does not change the soundness statement.
-/

namespace Mz

/-! ## Helper lemmas: error-free inputs yield an error-free output -/

theorem evalAnd_not_err
    {d₁ d₂ : Datum} (h₁ : ¬d₁.IsErr) (h₂ : ¬d₂.IsErr) :
    ¬(evalAnd d₁ d₂).IsErr := by
  cases d₁ with
  | bool b₁ =>
    cases d₂ with
    | bool b₂ => cases b₁ <;> cases b₂ <;> decide
    | null    => cases b₁ <;> decide
    | err _   => exact (h₂ trivial).elim
  | null =>
    cases d₂ with
    | bool b₂ => cases b₂ <;> decide
    | null    => decide
    | err _   => exact (h₂ trivial).elim
  | err _ => exact (h₁ trivial).elim

theorem evalOr_not_err
    {d₁ d₂ : Datum} (h₁ : ¬d₁.IsErr) (h₂ : ¬d₂.IsErr) :
    ¬(evalOr d₁ d₂).IsErr := by
  cases d₁ with
  | bool b₁ =>
    cases d₂ with
    | bool b₂ => cases b₁ <;> cases b₂ <;> decide
    | null    => cases b₁ <;> decide
    | err _   => exact (h₂ trivial).elim
  | null =>
    cases d₂ with
    | bool b₂ => cases b₂ <;> decide
    | null    => decide
    | err _   => exact (h₂ trivial).elim
  | err _ => exact (h₁ trivial).elim

theorem evalNot_not_err
    {d : Datum} (h : ¬d.IsErr) : ¬(evalNot d).IsErr := by
  cases d with
  | bool b => cases b <;> decide
  | null   => decide
  | err _  => exact (h trivial).elim

theorem evalIfThen_not_err
    {dc dt de : Datum}
    (hc : ¬dc.IsErr) (ht : ¬dt.IsErr) (he : ¬de.IsErr) :
    ¬(evalIfThen dc dt de).IsErr := by
  cases dc with
  | bool b =>
    cases b
    · -- false branch: evalIfThen reduces to `de`
      simp only [evalIfThen]; exact he
    · -- true branch: evalIfThen reduces to `dt`
      simp only [evalIfThen]; exact ht
  | null =>
    simp only [evalIfThen]; decide
  | err _ => exact (hc trivial).elim

/-! ## Static analyzer -/

/-- Returns `true` when `e` might evaluate to an `err`. The current
implementation is purely structural and conservative: any literal
`err` taints every ancestor. Columns are assumed not to contain errors
(see `Env.ErrFree`).

For the list-carrying constructors (`andN`, `orN`, `coalesce`) the
skeleton is maximally conservative — they always taint. A future
refinement would recurse into `args` and (for `coalesce`) reason
about the rescue rule, but the present version keeps the soundness
proof trivial for those cases. -/
def Expr.might_error : Expr → Bool
  | .lit (.err _)   => true
  | .lit _          => false
  | .col _          => false
  | .and a b        => a.might_error || b.might_error
  | .or  a b        => a.might_error || b.might_error
  | .not a          => a.might_error
  | .ifThen c t e   => c.might_error || t.might_error || e.might_error
  | .andN _         => true
  | .orN _          => true
  | .coalesce _     => true

/-! ## Error-free environments -/

/-- An environment is error-free when every bound value is not an `err`. -/
def Env.ErrFree (env : Env) : Prop :=
  ∀ d ∈ env, ¬d.IsErr

theorem Env.get_not_err {env : Env} (hErr : env.ErrFree) (i : Nat) :
    ¬(Env.get env i).IsErr := by
  induction env generalizing i with
  | nil =>
    -- `Env.get [] i = .null` by definition; `.null` is not an error.
    intro h
    cases h
  | cons hd tl ih =>
    cases i with
    | zero =>
      -- `Env.get (hd :: tl) 0 = hd`. `hd ∈ hd :: tl`, so `ErrFree` rules out err.
      apply hErr hd
      exact List.Mem.head tl
    | succ n =>
      -- Reduce to the tail and apply the IH.
      apply ih
      intro d hd_mem
      apply hErr d
      exact List.Mem.tail hd hd_mem

/-! ## Soundness -/

/-- If `might_error e` is `false` and the environment carries no
errors, then `eval env e` is not an error.

The proof is structural recursion on `e`. The `induction` tactic
cannot be used on `Expr` because it is a nested inductive type (the
`andN` / `orN` / `coalesce` constructors carry `List Expr`), so the
proof is written as a recursive `theorem` that pattern-matches the
constructor and recurses on subexpressions. For each compound case
the hypothesis `¬e.might_error = true` decomposes into
per-subexpression hypotheses via `Bool` distribution. The matching
helper lemma (`evalAnd_not_err`, etc.) then concludes.

The list-carrying constructors are handled vacuously: `might_error`
returns `true` on them unconditionally, so the soundness premise is
absurd. A future refinement will tighten `might_error` to inspect
operands and add real proofs for those cases. -/
theorem might_error_sound :
    ∀ (e : Expr) (env : Env),
      ¬(e.might_error = true) → env.ErrFree → ¬(eval env e).IsErr
  | .lit d, _, hMe, _ => by
    intro hRes
    simp only [eval] at hRes
    cases d with
    | bool _ => cases hRes
    | null   => cases hRes
    | err _  => exact hMe rfl
  | .col i, env, _, hEnv => by
    intro hRes
    simp only [eval] at hRes
    exact Env.get_not_err hEnv i hRes
  | .and a b, env, hMe, hEnv => by
    intro hRes
    simp only [eval] at hRes
    have ha : ¬(a.might_error = true) := fun h => hMe (by simp [Expr.might_error, h])
    have hb : ¬(b.might_error = true) := fun h => hMe (by simp [Expr.might_error, h])
    exact evalAnd_not_err
      (might_error_sound a env ha hEnv)
      (might_error_sound b env hb hEnv) hRes
  | .or a b, env, hMe, hEnv => by
    intro hRes
    simp only [eval] at hRes
    have ha : ¬(a.might_error = true) := fun h => hMe (by simp [Expr.might_error, h])
    have hb : ¬(b.might_error = true) := fun h => hMe (by simp [Expr.might_error, h])
    exact evalOr_not_err
      (might_error_sound a env ha hEnv)
      (might_error_sound b env hb hEnv) hRes
  | .not a, env, hMe, hEnv => by
    intro hRes
    simp only [eval] at hRes
    have ha : ¬(a.might_error = true) := fun h => hMe (by simp [Expr.might_error, h])
    exact evalNot_not_err (might_error_sound a env ha hEnv) hRes
  | .ifThen c t e, env, hMe, hEnv => by
    intro hRes
    simp only [eval] at hRes
    have hc : ¬(c.might_error = true) := fun h => hMe (by simp [Expr.might_error, h])
    have ht : ¬(t.might_error = true) := fun h => hMe (by simp [Expr.might_error, h])
    have he : ¬(e.might_error = true) := fun h => hMe (by simp [Expr.might_error, h])
    exact evalIfThen_not_err
      (might_error_sound c env hc hEnv)
      (might_error_sound t env ht hEnv)
      (might_error_sound e env he hEnv) hRes
  | .andN _, _, hMe, _ => by intro _; exact hMe rfl
  | .orN _, _, hMe, _ => by intro _; exact hMe rfl
  | .coalesce _, _, hMe, _ => by intro _; exact hMe rfl

end Mz

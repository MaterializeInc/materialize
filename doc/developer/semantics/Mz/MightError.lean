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

/-- List-level analogue of `evalAnd_not_err`: if every operand is
error-free, the variadic AND is error-free. -/
theorem evalAndN_not_err :
    ∀ {ds : List Datum}, (∀ d ∈ ds, ¬d.IsErr) → ¬(evalAndN ds).IsErr
  | [],      _ => by intro hRes; cases hRes
  | hd :: tl, h => by
    show ¬(evalAnd hd (evalAndN tl)).IsErr
    apply evalAnd_not_err
    · exact h hd (List.Mem.head tl)
    · exact evalAndN_not_err (fun d hd_mem => h d (List.Mem.tail hd hd_mem))

/-- Dual: every error-free operand makes the variadic OR error-free. -/
theorem evalOrN_not_err :
    ∀ {ds : List Datum}, (∀ d ∈ ds, ¬d.IsErr) → ¬(evalOrN ds).IsErr
  | [],      _ => by intro hRes; cases hRes
  | hd :: tl, h => by
    show ¬(evalOr hd (evalOrN tl)).IsErr
    apply evalOr_not_err
    · exact h hd (List.Mem.head tl)
    · exact evalOrN_not_err (fun d hd_mem => h d (List.Mem.tail hd hd_mem))

/-! ## Static analyzer

Returns `true` when `e` might evaluate to an `err`. The current
implementation is purely structural and conservative: any literal
`err` taints every ancestor. Columns are assumed not to contain errors
(see `Env.ErrFree`).

For `andN` and `orN`, the analyzer recurses into the operand list via
`Expr.argsMightError` and returns `true` if any operand might error.
The mutual recursion across `Expr.might_error` and
`Expr.argsMightError` keeps Lean's structural-recursion checker
satisfied without an explicit termination measure.

`coalesce` is still tainted unconditionally. A precise analyzer
would reason about the rescue rule (`coalesce(err, x) = x` when `x`
is concrete), which requires tracking which operands are statically
*safe* rather than merely *not erroring*. Tightening it is a separate
follow-up. -/
mutual
def Expr.might_error : Expr → Bool
  | .lit (.err _)   => true
  | .lit _          => false
  | .col _          => false
  | .and a b        => a.might_error || b.might_error
  | .or  a b        => a.might_error || b.might_error
  | .not a          => a.might_error
  | .ifThen c t e   => c.might_error || t.might_error || e.might_error
  | .andN args      => Expr.argsMightError args
  | .orN  args      => Expr.argsMightError args
  | .coalesce _     => true

def Expr.argsMightError : List Expr → Bool
  | []        => false
  | e :: rest => e.might_error || Expr.argsMightError rest
end

/-- Membership-driven introduction for `argsMightError`. If some
operand in the list might error, the whole list folds to `true`. The
contrapositive is what the soundness proof uses to extract a
per-operand non-erroring hypothesis from the analyzer's verdict on
`andN` / `orN`. -/
theorem Expr.argsMightError_of_mem
    {args : List Expr} {e : Expr}
    (h_mem : e ∈ args) (h_err : e.might_error = true) :
    Expr.argsMightError args = true := by
  induction args with
  | nil => cases h_mem
  | cons hd tl ih =>
    show (hd.might_error || Expr.argsMightError tl) = true
    cases h_mem with
    | head _    => simp [h_err]
    | tail _ h' => simp [ih h']

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

The proof is structural recursion on `e`. `induction` cannot be used
on `Expr` because it is a nested inductive type, so the proof is
written as a recursive `theorem` that pattern-matches the constructor
and recurses on subexpressions. For each compound case the hypothesis
`¬e.might_error = true` decomposes into per-subexpression hypotheses,
and the matching helper lemma (`evalAnd_not_err` etc.) concludes.

The `andN` / `orN` cases extract a per-operand non-erroring witness
through `Expr.argsMightError_of_mem` and then recurse via
`might_error_sound` on the individual operand. The `coalesce` case
is currently vacuous — `might_error` always returns `true` for
`.coalesce`, so the soundness premise is absurd. A future refinement
will tighten that case alongside the analyzer. -/
theorem might_error_sound :
    ∀ (e : Expr) (env : Env),
      ¬(e.might_error = true) → env.ErrFree → ¬(eval env e).IsErr
  | .lit d, _, hMe, _ => by
    intro hRes
    simp only [eval] at hRes
    cases d with
    | bool _ => cases hRes
    | null   => cases hRes
    | err _  =>
      apply hMe
      simp only [Expr.might_error]
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
  | .andN args, env, hMe, hEnv => by
    intro hRes
    simp only [eval] at hRes
    apply evalAndN_not_err (ds := args.map (eval env)) ?_ hRes
    intro d hd_mem
    obtain ⟨e, e_mem, h_eq⟩ := List.mem_map.mp hd_mem
    subst h_eq
    have he : ¬(e.might_error = true) := fun h => hMe
      (Expr.argsMightError_of_mem e_mem h)
    exact might_error_sound e env he hEnv
  | .orN args, env, hMe, hEnv => by
    intro hRes
    simp only [eval] at hRes
    apply evalOrN_not_err (ds := args.map (eval env)) ?_ hRes
    intro d hd_mem
    obtain ⟨e, e_mem, h_eq⟩ := List.mem_map.mp hd_mem
    subst h_eq
    have he : ¬(e.might_error = true) := fun h => hMe
      (Expr.argsMightError_of_mem e_mem h)
    exact might_error_sound e env he hEnv
  | .coalesce _, _, hMe, _ => by
    intro _
    apply hMe
    simp only [Expr.might_error]

end Mz

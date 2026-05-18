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
  | .lit (.err _)        => true
  | .lit _               => false
  | .col _               => false
  | .and a b             => a.might_error || b.might_error
  | .or  a b             => a.might_error || b.might_error
  | .not a               => a.might_error
  | .ifThen c t e        => c.might_error || t.might_error || e.might_error
  | .andN args           => Expr.argsMightError args
  | .orN  args           => Expr.argsMightError args
  | .coalesce []         => false
  | .coalesce (a :: rest) => a.might_error && Expr.argsAllMightError rest

/-- Bool fold of `might_error` over a list of operands ("does any
operand might-error"), declared mutually with `might_error` so
structural recursion accepts both sides. -/
def Expr.argsMightError : List Expr → Bool
  | []        => false
  | e :: rest => e.might_error || Expr.argsMightError rest

/-- Companion fold for `coalesce`: "do all operands might-error".
The empty-list base case is `true` so that the cons case
`a.might_error && argsAllMightError rest` gives the right answer
for any non-empty list. The pattern match in `Expr.might_error`'s
`.coalesce` arms handles the empty case separately. -/
def Expr.argsAllMightError : List Expr → Bool
  | []        => true
  | e :: rest => e.might_error && Expr.argsAllMightError rest
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

/-- If `argsAllMightError args` is not `true`, there is at least one
operand whose `might_error` is not `true`. This is the
"some safe operand exists" extraction used by the `coalesce` case of
soundness. -/
theorem Expr.exists_safe_of_not_argsAllMightError
    {args : List Expr} (h : ¬(Expr.argsAllMightError args = true)) :
    ∃ e ∈ args, ¬(e.might_error = true) := by
  induction args with
  | nil =>
    -- argsAllMightError [] = true, contradicts h
    exact (h rfl).elim
  | cons hd tl ih =>
    -- argsAllMightError (hd :: tl) = hd.might_error && argsAllMightError tl
    by_cases hd_me : hd.might_error = true
    · -- hd is not safe; the safe one must be in tl.
      have htl : ¬(Expr.argsAllMightError tl = true) := by
        intro h_tl
        apply h
        show (hd.might_error && Expr.argsAllMightError tl) = true
        simp [hd_me, h_tl]
      obtain ⟨e, e_mem, he⟩ := ih htl
      exact ⟨e, List.Mem.tail hd e_mem, he⟩
    · -- hd is the safe operand.
      exact ⟨hd, List.Mem.head tl, hd_me⟩

/-! ## Coalesce safety

Once `Coalesce.go` is invoked with `seenNull = true`, the result is
never an error: the empty-list base returns `.null`, and every cons
case either short-circuits to a `.bool b`, recurses with
`seenNull = true` unchanged, or updates `firstErr` without ever
flipping `seenNull` back to `false`.

The combined lemma `Coalesce.go_not_err` strengthens that
observation: if the starting state has either `seenNull = true` or
at least one not-err element in the remaining list, the result is
not an error. -/

theorem Coalesce.go_not_err :
    ∀ (seenNull : Bool) (firstErr : Option EvalError) (ds : List Datum),
      seenNull = true ∨ (∃ d ∈ ds, ¬d.IsErr) →
      ¬(Coalesce.go seenNull firstErr ds).IsErr
  | true,  _,  [],          _ => by
    intro hRes
    -- Coalesce.go true _ [] = .null
    show False
    simp only [Coalesce.go, if_true] at hRes
    cases hRes
  | false, _,  [],          h => by
    -- Empty + seenNull=false: only the disjunct ∃ d ∈ [] survives, which is False.
    cases h with
    | inl h_true => cases h_true
    | inr h_ex =>
      obtain ⟨_, hmem, _⟩ := h_ex
      cases hmem
  | _,     _,  .bool b :: _,  _ => by
    intro hRes
    -- Coalesce.go _ _ (.bool b :: _) = .bool b
    show False
    simp only [Coalesce.go] at hRes
    cases hRes
  | _,     firstErr, .null :: rest, _ => by
    -- Recurse with seenNull=true.
    show ¬(Coalesce.go true firstErr rest).IsErr
    exact Coalesce.go_not_err true firstErr rest (Or.inl rfl)
  | seenNull, firstErr, .err e :: rest, h => by
    -- Push the witness from (.err e :: rest) into rest, since .err e cannot be the witness.
    have h_rest : seenNull = true ∨ ∃ d ∈ rest, ¬d.IsErr := by
      cases h with
      | inl h_true => exact Or.inl h_true
      | inr h_ex =>
        obtain ⟨d, hmem, hsafe⟩ := h_ex
        cases hmem with
        | head _      => exact (hsafe trivial).elim
        | tail _ h_tl => exact Or.inr ⟨d, h_tl, hsafe⟩
    -- Two cases on firstErr; the recursion shape is the same modulo argument.
    cases firstErr with
    | some firstErr' =>
      show ¬(Coalesce.go seenNull (some firstErr') rest).IsErr
      exact Coalesce.go_not_err seenNull (some firstErr') rest h_rest
    | none =>
      show ¬(Coalesce.go seenNull (some e) rest).IsErr
      exact Coalesce.go_not_err seenNull (some e) rest h_rest

/-- The headline lemma: an `evalCoalesce` call cannot return an
error when at least one operand evaluates to something that is not
an error. The `null`-beats-`err` tiebreak inside `Coalesce.go` does
the work. -/
theorem evalCoalesce_not_err_of_some_safe
    {ds : List Datum} (h : ∃ d ∈ ds, ¬d.IsErr) :
    ¬(evalCoalesce ds).IsErr := by
  show ¬(Coalesce.go false none ds).IsErr
  exact Coalesce.go_not_err false none ds (Or.inr h)

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
  | .coalesce args, env, hMe, hEnv => by
    intro hRes
    simp only [eval] at hRes
    -- Empty list: `evalCoalesce [] = .null`, immediately not an error.
    match args, hMe, hRes with
    | [], _, hRes' =>
      simp [evalCoalesce, Coalesce.go] at hRes'
      cases hRes'
    | a :: rest, hMe', hRes' =>
      -- Non-empty case. `might_error (.coalesce (a :: rest))` reduces to
      -- `a.might_error && argsAllMightError rest`, which is exactly
      -- `argsAllMightError (a :: rest)`. Negate and extract a safe operand.
      have hAll : ¬(Expr.argsAllMightError (a :: rest) = true) := by
        intro hAll
        apply hMe'
        show (a.might_error && Expr.argsAllMightError rest) = true
        -- `argsAllMightError (a :: rest)` reduces by definition to the
        -- conjunction we need.
        exact hAll
      obtain ⟨e, e_mem, he_safe⟩ :=
        Expr.exists_safe_of_not_argsAllMightError hAll
      have he : ¬(eval env e).IsErr := might_error_sound e env he_safe hEnv
      apply evalCoalesce_not_err_of_some_safe
        (ds := (a :: rest).map (eval env))
        ?_ hRes'
      exact ⟨eval env e, List.mem_map.mpr ⟨e, e_mem, rfl⟩, he⟩

end Mz

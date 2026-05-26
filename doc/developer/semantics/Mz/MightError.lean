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
    | bool b₂ => cases b₁ <;> cases b₂ <;> (intro h; cases h)
    | int  _  => cases b₁ <;> (intro h; cases h)
    | null    => cases b₁ <;> (intro h; cases h)
    | err _   => exact (h₂ trivial).elim
  | int _ =>
    cases d₂ with
    | bool b₂ => cases b₂ <;> (intro h; cases h)
    | int  _  => intro h; cases h
    | null    => intro h; cases h
    | err _   => exact (h₂ trivial).elim
  | null =>
    cases d₂ with
    | bool b₂ => cases b₂ <;> (intro h; cases h)
    | int  _  => intro h; cases h
    | null    => intro h; cases h
    | err _   => exact (h₂ trivial).elim
  | err _ => exact (h₁ trivial).elim

theorem evalOr_not_err
    {d₁ d₂ : Datum} (h₁ : ¬d₁.IsErr) (h₂ : ¬d₂.IsErr) :
    ¬(evalOr d₁ d₂).IsErr := by
  cases d₁ with
  | bool b₁ =>
    cases d₂ with
    | bool b₂ => cases b₁ <;> cases b₂ <;> (intro h; cases h)
    | int  _  => cases b₁ <;> (intro h; cases h)
    | null    => cases b₁ <;> (intro h; cases h)
    | err _   => exact (h₂ trivial).elim
  | int _ =>
    cases d₂ with
    | bool b₂ => cases b₂ <;> (intro h; cases h)
    | int  _  => intro h; cases h
    | null    => intro h; cases h
    | err _   => exact (h₂ trivial).elim
  | null =>
    cases d₂ with
    | bool b₂ => cases b₂ <;> (intro h; cases h)
    | int  _  => intro h; cases h
    | null    => intro h; cases h
    | err _   => exact (h₂ trivial).elim
  | err _ => exact (h₁ trivial).elim

theorem evalNot_not_err
    {d : Datum} (h : ¬d.IsErr) : ¬(evalNot d).IsErr := by
  cases d with
  | bool b => cases b <;> (intro h; cases h)
  | int  _ => intro h; cases h
  | null   => intro h; cases h
  | err _  => exact (h trivial).elim

theorem evalPlus_not_err
    {d₁ d₂ : Datum} (h₁ : ¬d₁.IsErr) (h₂ : ¬d₂.IsErr) :
    ¬(evalPlus d₁ d₂).IsErr := by
  cases d₁ with
  | bool _ =>
    cases d₂ with
    | bool _ => intro h; cases h
    | int  _ => intro h; cases h
    | null   => intro h; cases h
    | err _  => exact (h₂ trivial).elim
  | int _ =>
    cases d₂ with
    | bool _ => intro h; cases h
    | int  _ => intro h; cases h
    | null   => intro h; cases h
    | err _  => exact (h₂ trivial).elim
  | null =>
    cases d₂ with
    | bool _ => intro h; cases h
    | int  _ => intro h; cases h
    | null   => intro h; cases h
    | err _  => exact (h₂ trivial).elim
  | err _ => exact (h₁ trivial).elim

theorem evalMinus_not_err
    {d₁ d₂ : Datum} (h₁ : ¬d₁.IsErr) (h₂ : ¬d₂.IsErr) :
    ¬(evalMinus d₁ d₂).IsErr := by
  cases d₁ with
  | bool _ =>
    cases d₂ with
    | bool _ => intro h; cases h
    | int  _ => intro h; cases h
    | null   => intro h; cases h
    | err _  => exact (h₂ trivial).elim
  | int _ =>
    cases d₂ with
    | bool _ => intro h; cases h
    | int  _ => intro h; cases h
    | null   => intro h; cases h
    | err _  => exact (h₂ trivial).elim
  | null =>
    cases d₂ with
    | bool _ => intro h; cases h
    | int  _ => intro h; cases h
    | null   => intro h; cases h
    | err _  => exact (h₂ trivial).elim
  | err _ => exact (h₁ trivial).elim

theorem evalTimes_not_err
    {d₁ d₂ : Datum} (h₁ : ¬d₁.IsErr) (h₂ : ¬d₂.IsErr) :
    ¬(evalTimes d₁ d₂).IsErr := by
  cases d₁ with
  | bool _ =>
    cases d₂ with
    | bool _ => intro h; cases h
    | int  _ => intro h; cases h
    | null   => intro h; cases h
    | err _  => exact (h₂ trivial).elim
  | int _ =>
    cases d₂ with
    | bool _ => intro h; cases h
    | int  _ => intro h; cases h
    | null   => intro h; cases h
    | err _  => exact (h₂ trivial).elim
  | null =>
    cases d₂ with
    | bool _ => intro h; cases h
    | int  _ => intro h; cases h
    | null   => intro h; cases h
    | err _  => exact (h₂ trivial).elim
  | err _ => exact (h₁ trivial).elim

theorem evalEq_not_err
    {d₁ d₂ : Datum} (h₁ : ¬d₁.IsErr) (h₂ : ¬d₂.IsErr) :
    ¬(evalEq d₁ d₂).IsErr := by
  cases d₁ with
  | bool _ =>
    cases d₂ with
    | bool _ => intro h; cases h
    | int  _ => intro h; cases h
    | null   => intro h; cases h
    | err _  => exact (h₂ trivial).elim
  | int _ =>
    cases d₂ with
    | bool _ => intro h; cases h
    | int  _ => intro h; cases h
    | null   => intro h; cases h
    | err _  => exact (h₂ trivial).elim
  | null =>
    cases d₂ with
    | bool _ => intro h; cases h
    | int  _ => intro h; cases h
    | null   => intro h; cases h
    | err _  => exact (h₂ trivial).elim
  | err _ => exact (h₁ trivial).elim

theorem evalLt_not_err
    {d₁ d₂ : Datum} (h₁ : ¬d₁.IsErr) (h₂ : ¬d₂.IsErr) :
    ¬(evalLt d₁ d₂).IsErr := by
  cases d₁ with
  | bool _ =>
    cases d₂ with
    | bool _ => intro h; cases h
    | int  _ => intro h; cases h
    | null   => intro h; cases h
    | err _  => exact (h₂ trivial).elim
  | int _ =>
    cases d₂ with
    | bool _ => intro h; cases h
    | int  _ => intro h; cases h
    | null   => intro h; cases h
    | err _  => exact (h₂ trivial).elim
  | null =>
    cases d₂ with
    | bool _ => intro h; cases h
    | int  _ => intro h; cases h
    | null   => intro h; cases h
    | err _  => exact (h₂ trivial).elim
  | err _ => exact (h₁ trivial).elim

/-- Division is the canonical erring operation: a right operand
of `.int 0` produces `.err .divisionByZero` even when both
operands are otherwise error-free. So the analyzer's universal
"divide might err" verdict is exactly right; soundness on
`.divide` proceeds via the absurd-premise path. -/
theorem evalDivide_not_err_of_nonzero
    {n m : Int} (hm : m ≠ 0) :
    ¬(evalDivide (.int n) (.int m)).IsErr := by
  show ¬(if m = 0 then Datum.err EvalError.divisionByZero
          else Datum.int (n / m)).IsErr
  rw [if_neg hm]
  intro h; cases h

theorem evalDivide_zero (n : Int) :
    evalDivide (.int n) (.int 0) = .err .divisionByZero := by
  show (if (0 : Int) = 0 then Datum.err EvalError.divisionByZero
          else Datum.int (n / 0))
      = Datum.err .divisionByZero
  rw [if_pos rfl]

/-- Generalization: any non-erring dividend divided by a literal
nonzero int does not err. The dividend can be any `Datum`; only
the divisor is constrained. -/
theorem evalDivide_lit_nonzero
    {d : Datum} {n : Int} (h : ¬d.IsErr) (hn : n ≠ 0) :
    ¬(evalDivide d (.int n)).IsErr := by
  cases d with
  | bool _ => intro hRes; cases hRes
  | int  m =>
    show ¬(if n = 0 then Datum.err EvalError.divisionByZero
            else Datum.int (m / n)).IsErr
    rw [if_neg hn]
    intro hRes; cases hRes
  | null   => intro hRes; cases hRes
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
  | int _ =>
    simp only [evalIfThen]; intro h; cases h
  | null =>
    simp only [evalIfThen]; intro h; cases h
  | err _ => exact (hc trivial).elim

/-! ### Short-circuit absorbers

`AND` absorbs to `.bool false` whenever either operand is
`.bool false`, regardless of what the other operand is. Likewise
`OR` absorbs to `.bool true` on either side. These are the
algebraic facts behind the value-level tightening of
`Expr.might_error`. -/

theorem evalAnd_left_false (d : Datum) : evalAnd (.bool false) d = .bool false := rfl

theorem evalAnd_right_false (d : Datum) : evalAnd d (.bool false) = .bool false := by
  cases d with
  | bool b => cases b <;> rfl
  | int  _ => rfl
  | null   => rfl
  | err _  => rfl

theorem evalOr_left_true (d : Datum) : evalOr (.bool true) d = .bool true := rfl

theorem evalOr_right_true (d : Datum) : evalOr d (.bool true) = .bool true := by
  cases d with
  | bool b => cases b <;> rfl
  | int  _ => rfl
  | null   => rfl
  | err _  => rfl

/-- Variadic `AND` absorbs to `.bool false` as soon as any operand
is `.bool false`. Stated inline to keep `MightError.lean`
self-contained (the same fact reappears in `Mz/Variadic.lean`
under the name `evalAndN_false_absorbs`, but importing that
module creates a cycle with `Mz/Laws.lean`). -/
private theorem evalAndN_false_mem_eq
    {ds : List Datum} (h : .bool false ∈ ds) :
    evalAndN ds = .bool false := by
  induction ds with
  | nil => exact absurd h List.not_mem_nil
  | cons hd tl ih =>
    rcases List.mem_cons.mp h with hHead | hTail
    · subst hHead
      show evalAnd (.bool false) (evalAndN tl) = .bool false
      rfl
    · have hTl := ih hTail
      show evalAnd hd (evalAndN tl) = .bool false
      rw [hTl]
      exact evalAnd_right_false hd

/-- Dual: variadic `OR` absorbs to `.bool true` as soon as any
operand is `.bool true`. -/
private theorem evalOrN_true_mem_eq
    {ds : List Datum} (h : .bool true ∈ ds) :
    evalOrN ds = .bool true := by
  induction ds with
  | nil => exact absurd h List.not_mem_nil
  | cons hd tl ih =>
    rcases List.mem_cons.mp h with hHead | hTail
    · subst hHead
      show evalOr (.bool true) (evalOrN tl) = .bool true
      rfl
    · have hTl := ih hTail
      show evalOr hd (evalOrN tl) = .bool true
      rw [hTl]
      exact evalOr_right_true hd

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
implementation is structural with two pieces of value-level
tightening on binary `AND` and `OR`:

* `.and (.lit (.bool false)) _` and `.and _ (.lit (.bool false))`
  return `false`. The four-valued AND table has `false` as the
  dominant absorber (`false AND error = false` from either side),
  so a literal-false operand statically rules out an error.
* `.or (.lit (.bool true)) _` and `.or _ (.lit (.bool true))`
  return `false` for the dual reason.

Any literal `err` taints every ancestor. Columns are assumed not
to contain errors (see `Env.ErrFree`).

For `andN` and `orN`, the analyzer recurses into the operand list
via `Expr.argsMightError`. For `coalesce`, the analyzer fires only
when *every* operand might error.

The mutual recursion across `Expr.might_error`,
`Expr.argsMightError`, and `Expr.argsAllMightError` keeps Lean's
structural-recursion checker satisfied without an explicit
termination measure. -/
/-- Top-of-expression literal-false detector. Non-recursive on
`Expr`; matches only the head constructor. Used by `might_error`
to identify the `false`-absorber position of binary / variadic
`AND` and the `false` branch of `IfThen`. -/
@[simp] def Expr.isLitBoolFalse : Expr → Bool
  | .lit (.bool false) => true
  | _                  => false

/-- Dual: top-of-expression literal-true detector. -/
@[simp] def Expr.isLitBoolTrue : Expr → Bool
  | .lit (.bool true) => true
  | _                 => false

/-- `isLitBoolFalse e = true` exactly characterizes
`e = .lit (.bool false)`. -/
theorem Expr.eq_of_isLitBoolFalse {e : Expr}
    (h : e.isLitBoolFalse = true) : e = .lit (.bool false) := by
  cases e with
  | lit d =>
    cases d with
    | bool b => cases b with
                  | false => rfl
                  | true  => simp [Expr.isLitBoolFalse] at h
    | _ => simp [Expr.isLitBoolFalse] at h
  | _ => simp [Expr.isLitBoolFalse] at h

/-- Dual characterization. -/
theorem Expr.eq_of_isLitBoolTrue {e : Expr}
    (h : e.isLitBoolTrue = true) : e = .lit (.bool true) := by
  cases e with
  | lit d =>
    cases d with
    | bool b => cases b with
                  | true  => rfl
                  | false => simp [Expr.isLitBoolTrue] at h
    | _ => simp [Expr.isLitBoolTrue] at h
  | _ => simp [Expr.isLitBoolTrue] at h

/-- Head matcher for "expression is a statically-safe divisor":
literal `.int n` with `n ≠ 0`. Used by `might_error` to detect
divide expressions whose divisor cannot trigger divide-by-zero. -/
@[simp] def Expr.divisorIsSafe : Expr → Bool
  | .lit (.int 0) => false
  | .lit (.int _) => true
  | _             => false

/-- Characterization: `divisorIsSafe e = true` iff `e` is a
literal `.int n` with `n ≠ 0`. -/
theorem Expr.lit_nonzero_int_of_divisorIsSafe {e : Expr}
    (h : e.divisorIsSafe = true) :
    ∃ n : Int, e = .lit (.int n) ∧ n ≠ 0 := by
  cases e with
  | lit d =>
    cases d with
    | int n  =>
      by_cases hZ : n = 0
      · subst hZ; simp [Expr.divisorIsSafe] at h
      · exact ⟨n, rfl, hZ⟩
    | _ => simp [Expr.divisorIsSafe] at h
  | _ => simp [Expr.divisorIsSafe] at h

mutual
def Expr.might_error : Expr → Bool
  | .lit (.err _)         => true
  | .lit _                => false
  | .col _                => false
  | .not a                => a.might_error
  | .ifThen c t e         =>
    if c.isLitBoolTrue  then t.might_error
    else if c.isLitBoolFalse then e.might_error
    else c.might_error || t.might_error || e.might_error
  | .andN args            =>
    if args.any (·.isLitBoolFalse) then false
    else Expr.argsMightError args
  | .orN  args            =>
    if args.any (·.isLitBoolTrue)  then false
    else Expr.argsMightError args
  | .coalesce []          => false
  | .coalesce (a :: rest) => a.might_error && Expr.argsAllMightError rest
  | .plus   a b           => a.might_error || b.might_error
  | .minus  a b           => a.might_error || b.might_error
  | .times  a b           => a.might_error || b.might_error
  | .divide a b           =>
    if b.divisorIsSafe then a.might_error
    else true
  | .eq     a b           => a.might_error || b.might_error
  | .lt     a b           => a.might_error || b.might_error

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

`evalCoalesce` is the two-pass form: `Coalesce.firstConcrete` finds
the leftmost `.bool _` or `.int _` operand; `Coalesce.residue` is
the fallback when no concrete exists. The safety claim — *an
`evalCoalesce` call cannot return an error when at least one
operand is not an error* — splits along the dispatch:

* `firstConcrete` returns a `.bool _` / `.int _`, which is never
  err.
* When `firstConcrete = none`, every operand is `.null` or `.err _`.
  The safe witness must then be `.null`, and `residue` returns
  `.null` whenever any operand is `.null` (the `.null > .err`
  tiebreaker). -/

/-- A `firstConcrete` hit is a concrete operand, which is not err. -/
theorem Coalesce.firstConcrete_not_err :
    ∀ (ds : List Datum) (d : Datum),
      Coalesce.firstConcrete ds = some d → ¬d.IsErr
  | [], _, h => by cases h
  | .bool _ :: _, _, h => by
    simp only [Coalesce.firstConcrete, Option.some.injEq] at h
    subst h; intro hErr; cases hErr
  | .int _ :: _, _, h => by
    simp only [Coalesce.firstConcrete, Option.some.injEq] at h
    subst h; intro hErr; cases hErr
  | .null :: rest, d, h => by
    simp only [Coalesce.firstConcrete] at h
    exact Coalesce.firstConcrete_not_err rest d h
  | .err _ :: rest, d, h => by
    simp only [Coalesce.firstConcrete] at h
    exact Coalesce.firstConcrete_not_err rest d h

/-- When `firstConcrete = none`, every operand is `.null` or `.err _`. -/
theorem Coalesce.firstConcrete_none_no_concrete :
    ∀ (ds : List Datum),
      Coalesce.firstConcrete ds = none →
      ∀ d ∈ ds, d.isNullB = true ∨ d.IsErr
  | [], _, d, hmem => by cases hmem
  | .bool _ :: _, h, _, _ => by
    -- firstConcrete (.bool b :: _) = some (.bool b), contradicts h.
    simp only [Coalesce.firstConcrete] at h
    cases h
  | .int _ :: _, h, _, _ => by
    simp only [Coalesce.firstConcrete] at h
    cases h
  | .null :: rest, h, d, hmem => by
    simp only [Coalesce.firstConcrete] at h
    cases hmem with
    | head _ => exact Or.inl rfl
    | tail _ htl => exact Coalesce.firstConcrete_none_no_concrete rest h d htl
  | .err _ :: rest, h, d, hmem => by
    simp only [Coalesce.firstConcrete] at h
    cases hmem with
    | head _ => exact Or.inr True.intro
    | tail _ htl => exact Coalesce.firstConcrete_none_no_concrete rest h d htl

/-- When a `.null` is present, `residue` returns `.null` (never err). -/
theorem Coalesce.residue_eq_null_of_null_mem :
    ∀ (ds : List Datum),
      (∃ d ∈ ds, d.isNullB = true) → Coalesce.residue ds = .null
  | [], h => by obtain ⟨_, hmem, _⟩ := h; cases hmem
  | .null :: _, _ => rfl
  | .bool _ :: rest, h => by
    simp only [Coalesce.residue]
    apply Coalesce.residue_eq_null_of_null_mem rest
    obtain ⟨d, hmem, hd⟩ := h
    cases hmem with
    | head _ => cases hd
    | tail _ htl => exact ⟨d, htl, hd⟩
  | .int _ :: rest, h => by
    simp only [Coalesce.residue]
    apply Coalesce.residue_eq_null_of_null_mem rest
    obtain ⟨d, hmem, hd⟩ := h
    cases hmem with
    | head _ => cases hd
    | tail _ htl => exact ⟨d, htl, hd⟩
  | .err e :: rest, h => by
    simp only [Coalesce.residue]
    have h_any : rest.any Datum.isNullB = true := by
      -- `.err e` is not null, so the null witness must be in `rest`.
      obtain ⟨d, hmem, hd⟩ := h
      cases hmem with
      | head _ => cases hd
      | tail _ htl =>
        rw [List.any_eq_true]
        exact ⟨d, htl, hd⟩
    rw [if_pos h_any]

/-- The headline lemma: an `evalCoalesce` call cannot return an
error when at least one operand evaluates to something that is not
an error. The dispatch on `firstConcrete` splits cleanly: a hit is
already concrete (not err); a miss means every operand is `.null`
or `.err`, the safe witness is therefore `.null`, and `residue`
returns `.null`. -/
theorem evalCoalesce_not_err_of_some_safe
    {ds : List Datum} (h : ∃ d ∈ ds, ¬d.IsErr) :
    ¬(evalCoalesce ds).IsErr := by
  show ¬(match Coalesce.firstConcrete ds with
         | some d => d
         | none => Coalesce.residue ds).IsErr
  cases hfc : Coalesce.firstConcrete ds with
  | some d =>
    exact Coalesce.firstConcrete_not_err ds d hfc
  | none =>
    have h_all := Coalesce.firstConcrete_none_no_concrete ds hfc
    obtain ⟨d, hd_mem, hd_safe⟩ := h
    have hd_cases := h_all d hd_mem
    have h_null_in : ∃ d ∈ ds, d.isNullB = true := by
      cases hd_cases with
      | inl hd_null => exact ⟨d, hd_mem, hd_null⟩
      | inr hd_err => exact absurd hd_err hd_safe
    rw [Coalesce.residue_eq_null_of_null_mem ds h_null_in]
    intro hErr; cases hErr

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
    | int  _ => cases hRes
    | null   => cases hRes
    | err _  =>
      apply hMe
      simp only [Expr.might_error]
  | .col i, env, _, hEnv => by
    intro hRes
    simp only [eval] at hRes
    exact Env.get_not_err hEnv i hRes
  | .not a, env, hMe, hEnv => by
    intro hRes
    simp only [eval] at hRes
    have ha : ¬(a.might_error = true) := fun h => hMe (by simp [Expr.might_error, h])
    exact evalNot_not_err (might_error_sound a env ha hEnv) hRes
  | .ifThen c t e, env, hMe, hEnv => by
    intro hRes
    simp only [eval] at hRes
    cases hCT : c.isLitBoolTrue with
    | true =>
      -- c = .lit (.bool true): evalIfThen .bool true dt de = dt
      have hEq : c = .lit (.bool true) := by
        cases c with
        | lit d =>
          cases d with
          | bool b' => cases b' with
                        | true  => rfl
                        | false => simp [Expr.isLitBoolTrue] at hCT
          | _ => simp [Expr.isLitBoolTrue] at hCT
        | _ => simp [Expr.isLitBoolTrue] at hCT
      rw [hEq] at hRes
      simp only [eval] at hRes
      -- hRes : (evalIfThen .bool true (eval env t) (eval env e)).IsErr
      -- evalIfThen .bool true dt _ = dt by definition
      have h_reduce : evalIfThen (.bool true) (eval env t) (eval env e) = eval env t := rfl
      rw [h_reduce] at hRes
      have ht : ¬(t.might_error = true) := fun h => hMe (by
        rw [hEq]
        show (if (Expr.lit (.bool true)).isLitBoolTrue then t.might_error
                else if (Expr.lit (.bool true)).isLitBoolFalse then e.might_error
                else (Expr.lit (.bool true)).might_error || t.might_error || e.might_error)
            = true
        simp [Expr.isLitBoolTrue, h])
      exact might_error_sound t env ht hEnv hRes
    | false =>
      cases hCF : c.isLitBoolFalse with
      | true =>
        have hEq : c = .lit (.bool false) := by
          cases c with
          | lit d =>
            cases d with
            | bool b' => cases b' with
                          | false => rfl
                          | true  => simp [Expr.isLitBoolFalse] at hCF
            | _ => simp [Expr.isLitBoolFalse] at hCF
          | _ => simp [Expr.isLitBoolFalse] at hCF
        rw [hEq] at hRes
        simp only [eval] at hRes
        have h_reduce : evalIfThen (.bool false) (eval env t) (eval env e) = eval env e := rfl
        rw [h_reduce] at hRes
        have he : ¬(e.might_error = true) := fun h => hMe (by
          rw [hEq]
          show (if (Expr.lit (.bool false)).isLitBoolTrue then t.might_error
                  else if (Expr.lit (.bool false)).isLitBoolFalse then e.might_error
                  else (Expr.lit (.bool false)).might_error || t.might_error || e.might_error)
              = true
          simp [Expr.isLitBoolTrue, Expr.isLitBoolFalse, h])
        exact might_error_sound e env he hEnv hRes
      | false =>
        -- Non-short-circuit branch
        have hMeReduce :
            Expr.might_error (.ifThen c t e)
              = (c.might_error || t.might_error || e.might_error) := by
          show (if c.isLitBoolTrue  then t.might_error
                  else if c.isLitBoolFalse then e.might_error
                  else c.might_error || t.might_error || e.might_error)
              = (c.might_error || t.might_error || e.might_error)
          rw [hCT, hCF]
          rfl
        rw [hMeReduce] at hMe
        have hc : ¬(c.might_error = true) := fun h => hMe (by simp [h])
        have ht : ¬(t.might_error = true) := fun h => hMe (by simp [h])
        have he : ¬(e.might_error = true) := fun h => hMe (by simp [h])
        exact evalIfThen_not_err
          (might_error_sound c env hc hEnv)
          (might_error_sound t env ht hEnv)
          (might_error_sound e env he hEnv) hRes
  | .andN args, env, hMe, hEnv => by
    intro hRes
    cases hAny : args.any (·.isLitBoolFalse) with
    | true =>
      -- Some operand is `.lit (.bool false)`. evalAndN absorbs to `.bool false`.
      obtain ⟨e, he_mem, he_lit⟩ := List.any_eq_true.mp hAny
      have hEq : e = .lit (.bool false) := Expr.eq_of_isLitBoolFalse he_lit
      rw [hEq] at he_mem
      have hEvalLit : eval env (.lit (.bool false)) = .bool false := by
        simp only [eval]
      have hMapMem : (Datum.bool false) ∈ args.map (eval env) :=
        List.mem_map.mpr ⟨.lit (.bool false), he_mem, hEvalLit⟩
      simp only [eval] at hRes
      rw [evalAndN_false_mem_eq hMapMem] at hRes
      cases hRes
    | false =>
      have hMeReduce :
          Expr.might_error (.andN args) = Expr.argsMightError args := by
        show (if args.any (·.isLitBoolFalse) then false
                else Expr.argsMightError args)
            = Expr.argsMightError args
        rw [hAny]; rfl
      rw [hMeReduce] at hMe
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
    cases hAny : args.any (·.isLitBoolTrue) with
    | true =>
      obtain ⟨e, he_mem, he_lit⟩ := List.any_eq_true.mp hAny
      have hEq : e = .lit (.bool true) := Expr.eq_of_isLitBoolTrue he_lit
      rw [hEq] at he_mem
      have hEvalLit : eval env (.lit (.bool true)) = .bool true := by
        simp only [eval]
      have hMapMem : (Datum.bool true) ∈ args.map (eval env) :=
        List.mem_map.mpr ⟨.lit (.bool true), he_mem, hEvalLit⟩
      simp only [eval] at hRes
      rw [evalOrN_true_mem_eq hMapMem] at hRes
      cases hRes
    | false =>
      have hMeReduce :
          Expr.might_error (.orN args) = Expr.argsMightError args := by
        show (if args.any (·.isLitBoolTrue) then false
                else Expr.argsMightError args)
            = Expr.argsMightError args
        rw [hAny]; rfl
      rw [hMeReduce] at hMe
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
      simp [evalCoalesce, Coalesce.firstConcrete, Coalesce.residue] at hRes'
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
  | .plus a b, env, hMe, hEnv => by
    intro hRes
    simp only [eval] at hRes
    have ha : ¬(a.might_error = true) := fun h => hMe (by simp [Expr.might_error, h])
    have hb : ¬(b.might_error = true) := fun h => hMe (by simp [Expr.might_error, h])
    exact evalPlus_not_err
      (might_error_sound a env ha hEnv)
      (might_error_sound b env hb hEnv) hRes
  | .minus a b, env, hMe, hEnv => by
    intro hRes
    simp only [eval] at hRes
    have ha : ¬(a.might_error = true) := fun h => hMe (by simp [Expr.might_error, h])
    have hb : ¬(b.might_error = true) := fun h => hMe (by simp [Expr.might_error, h])
    exact evalMinus_not_err
      (might_error_sound a env ha hEnv)
      (might_error_sound b env hb hEnv) hRes
  | .times a b, env, hMe, hEnv => by
    intro hRes
    simp only [eval] at hRes
    have ha : ¬(a.might_error = true) := fun h => hMe (by simp [Expr.might_error, h])
    have hb : ¬(b.might_error = true) := fun h => hMe (by simp [Expr.might_error, h])
    exact evalTimes_not_err
      (might_error_sound a env ha hEnv)
      (might_error_sound b env hb hEnv) hRes
  | .divide a b, env, hMe, hEnv => by
    intro hRes
    simp only [eval] at hRes
    cases hSafe : b.divisorIsSafe with
    | true =>
      -- b is a literal nonzero int: evalDivide _ (.int n) errs only on div0
      obtain ⟨n, hEqB, hNZ⟩ := Expr.lit_nonzero_int_of_divisorIsSafe hSafe
      have hMeReduce :
          Expr.might_error (.divide a b) = a.might_error := by
        show (if b.divisorIsSafe = true then a.might_error else true)
            = a.might_error
        rw [hSafe]; rfl
      rw [hMeReduce] at hMe
      have hAe := might_error_sound a env hMe hEnv
      rw [hEqB] at hRes
      simp only [eval] at hRes
      exact evalDivide_lit_nonzero hAe hNZ hRes
    | false =>
      -- conservative branch: might_error returns `true`, premise absurd
      have hMeTrue : Expr.might_error (.divide a b) = true := by
        show (if b.divisorIsSafe = true then a.might_error else true) = true
        rw [hSafe]; rfl
      exact hMe hMeTrue
  | .eq a b, env, hMe, hEnv => by
    intro hRes
    simp only [eval] at hRes
    have ha : ¬(a.might_error = true) := fun h => hMe (by simp [Expr.might_error, h])
    have hb : ¬(b.might_error = true) := fun h => hMe (by simp [Expr.might_error, h])
    exact evalEq_not_err
      (might_error_sound a env ha hEnv)
      (might_error_sound b env hb hEnv) hRes
  | .lt a b, env, hMe, hEnv => by
    intro hRes
    simp only [eval] at hRes
    have ha : ¬(a.might_error = true) := fun h => hMe (by simp [Expr.might_error, h])
    have hb : ¬(b.might_error = true) := fun h => hMe (by simp [Expr.might_error, h])
    exact evalLt_not_err
      (might_error_sound a env ha hEnv)
      (might_error_sound b env hb hEnv) hRes

end Mz

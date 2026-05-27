import Mz.Eval

/-!
# `might_error` analyzer + soundness

Conservative analyzer that returns `true` when an `Expr sch k`
might evaluate to `.err _`.

The GADT kind discipline removes the type-mismatch case from
every primitive's error sources. What's left:

* `.lit (.err _)` — literal error.
* `.divide _ _` — divide-by-zero on `.int 0` divisor.
* Variadic short-circuit absorbs err on the "absorbing" side
  (`evalAndN [.bool false, .err _] = .bool false`), so the
  analyzer is conservative — if any operand might err, the
  variadic might err.
* Operator soundness in the err-propagation chain: errs propagate
  through strict primitives. -/

namespace Mz


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

/-! ## Coalesce safety

When at least one operand is not an err, `evalCoalesce` returns a
non-err `Datum`. The dispatch on `Coalesce.firstConcrete` splits
cleanly: a hit is concrete (not err); a miss means every operand
is `.null` or `.err`, the safe witness must be `.null`, and
`residue` returns `.null`. -/

theorem Coalesce.firstConcrete_not_err {k : ColType} :
    ∀ (ds : List (Datum k)) (d : Datum k),
      Coalesce.firstConcrete ds = some d → ¬d.IsErr
  | [], _, h => by simp [Coalesce.firstConcrete] at h
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

theorem Coalesce.firstConcrete_none_no_concrete {k : ColType} :
    ∀ (ds : List (Datum k)),
      Coalesce.firstConcrete ds = none →
      ∀ d ∈ ds, d.isNullB = true ∨ d.IsErr
  | [], _, d, hmem => by cases hmem
  | .bool _ :: _, h, _, _ => by
    simp only [Coalesce.firstConcrete] at h; cases h
  | .int _ :: _, h, _, _ => by
    simp only [Coalesce.firstConcrete] at h; cases h
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

theorem Coalesce.residue_eq_null_of_null_mem {k : ColType} :
    ∀ (ds : List (Datum k)),
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
  | .err _ :: rest, h => by
    simp only [Coalesce.residue]
    have h_any : rest.any Datum.isNullB = true := by
      obtain ⟨d, hmem, hd⟩ := h
      cases hmem with
      | head _ => cases hd
      | tail _ htl => rw [List.any_eq_true]; exact ⟨d, htl, hd⟩
    rw [if_pos h_any]

theorem evalCoalesce_not_err_of_some_safe {k : ColType}
    {ds : List (Datum k)} (h : ∃ d ∈ ds, ¬d.IsErr) :
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

/-! ## Variadic safety

`evalAndN` / `evalOrN` are not err when no operand is err. The
absorption rule (`FALSE` for AND, `TRUE` for OR) means err can be
absorbed away even if present; the no-err premise makes the
conclusion stronger and more usable downstream. -/

theorem evalAndN_not_err_of_all_safe :
    ∀ (ds : List (Datum .bool)), (∀ d ∈ ds, ¬d.IsErr) →
      ¬(evalAndN ds).IsErr
  | [], _ => fun h => by cases h
  | hd :: tl, hall => by
    have hhd : ¬hd.IsErr := hall hd (List.Mem.head _)
    have htl : ∀ d ∈ tl, ¬d.IsErr :=
      fun d hd_mem => hall d (List.Mem.tail _ hd_mem)
    have h_tl_safe : ¬(evalAndN tl).IsErr := evalAndN_not_err_of_all_safe tl htl
    show ¬(evalAnd hd (evalAndN tl)).IsErr
    exact evalAnd_not_err hhd h_tl_safe

theorem evalOrN_not_err_of_all_safe :
    ∀ (ds : List (Datum .bool)), (∀ d ∈ ds, ¬d.IsErr) →
      ¬(evalOrN ds).IsErr
  | [], _ => fun h => by cases h
  | hd :: tl, hall => by
    have hhd : ¬hd.IsErr := hall hd (List.Mem.head _)
    have htl : ∀ d ∈ tl, ¬d.IsErr :=
      fun d hd_mem => hall d (List.Mem.tail _ hd_mem)
    have h_tl_safe : ¬(evalOrN tl).IsErr := evalOrN_not_err_of_all_safe tl htl
    show ¬(evalOr hd (evalOrN tl)).IsErr
    exact evalOr_not_err hhd h_tl_safe

/-! ## Soundness of the `might_error` analyzer

If `might_error e = false` and the environment is `EnvErrFree`,
then `eval env e` is not an error. Mutual with `args_might_error_sound`
for the variadic constructors. -/

mutual
  theorem might_error_sound {n : Nat} {sch : Schema n} (env : Env sch)
      (hEnv : EnvErrFree env) :
      {k : ColType} → (e : Expr sch k) →
        Expr.might_error e = false → ¬(eval env e).IsErr
    | _, .lit d, h => by
      cases d with
      | bool _ => intro hErr; cases hErr
      | int _  => intro hErr; cases hErr
      | null   => intro hErr; cases hErr
      | err _  => simp [Expr.might_error] at h
    | _, .col i, _ => hEnv i
    | _, .not a, h => by
      simp only [Expr.might_error] at h
      have iha := might_error_sound env hEnv a h
      unfold eval
      exact evalNot_not_err iha
    | _, .plus a b, h => by
      simp only [Expr.might_error, Bool.or_eq_false_iff] at h
      have iha := might_error_sound env hEnv a h.1
      have ihb := might_error_sound env hEnv b h.2
      show ¬(evalPlus (eval env a) (eval env b)).IsErr
      exact evalPlus_not_err iha ihb
    | _, .minus a b, h => by
      simp only [Expr.might_error, Bool.or_eq_false_iff] at h
      have iha := might_error_sound env hEnv a h.1
      have ihb := might_error_sound env hEnv b h.2
      show ¬(evalMinus (eval env a) (eval env b)).IsErr
      exact evalMinus_not_err iha ihb
    | _, .times a b, h => by
      simp only [Expr.might_error, Bool.or_eq_false_iff] at h
      have iha := might_error_sound env hEnv a h.1
      have ihb := might_error_sound env hEnv b h.2
      show ¬(evalTimes (eval env a) (eval env b)).IsErr
      exact evalTimes_not_err iha ihb
    | _, .divide _ _, h => by simp [Expr.might_error] at h
    | _, .eq a b, h => by
      simp only [Expr.might_error, Bool.or_eq_false_iff] at h
      have iha := might_error_sound env hEnv a h.1
      have ihb := might_error_sound env hEnv b h.2
      show ¬(evalEq (eval env a) (eval env b)).IsErr
      exact evalEq_not_err iha ihb
    | _, .lt a b, h => by
      simp only [Expr.might_error, Bool.or_eq_false_iff] at h
      have iha := might_error_sound env hEnv a h.1
      have ihb := might_error_sound env hEnv b h.2
      show ¬(evalLt (eval env a) (eval env b)).IsErr
      exact evalLt_not_err iha ihb
    | _, .ifThen c t e, h => by
      simp only [Expr.might_error, Bool.or_eq_false_iff] at h
      have ihc := might_error_sound env hEnv c h.1.1
      have iht := might_error_sound env hEnv t h.1.2
      have ihe := might_error_sound env hEnv e h.2
      unfold eval
      cases hev : eval env c with
      | bool b => cases b
                  · exact ihe
                  · exact iht
      | null => intro hRes; cases hRes
      | err _ => exact absurd (by rw [hev]; trivial) ihc
    | _, .andN args, h => by
      simp only [Expr.might_error] at h
      have ih_args := args_might_error_sound env hEnv args h
      show ¬(evalAndN (evalList env args)).IsErr
      exact evalAndN_not_err_of_all_safe _ ih_args
    | _, .orN args, h => by
      simp only [Expr.might_error] at h
      have ih_args := args_might_error_sound env hEnv args h
      show ¬(evalOrN (evalList env args)).IsErr
      exact evalOrN_not_err_of_all_safe _ ih_args
    | _, .coalesce args, h => by
      simp only [Expr.might_error] at h
      have ih_args := args_might_error_sound env hEnv args h
      show ¬(evalCoalesce (evalList env args)).IsErr
      cases hargs : evalList env args with
      | nil =>
        intro hRes
        simp [evalCoalesce, Coalesce.firstConcrete, Coalesce.residue,
              Datum.IsErr] at hRes
      | cons hd tl =>
        have hd_safe : ¬hd.IsErr := by
          apply ih_args hd
          rw [hargs]; exact List.Mem.head tl
        exact evalCoalesce_not_err_of_some_safe ⟨hd, List.Mem.head tl, hd_safe⟩

  /-- Mutual: every element of `evalList env args` is non-err when
  every arg has `might_error = false`. -/
  theorem args_might_error_sound {n : Nat} {sch : Schema n} (env : Env sch)
      (hEnv : EnvErrFree env) :
      {k : ColType} → (args : ExprList sch k) →
        ExprList.argsMightError args = false →
        ∀ d ∈ evalList env args, ¬d.IsErr
    | _, .nil, _, d, hmem => by cases hmem
    | _, .cons a as, h, d, hmem => by
      simp only [ExprList.argsMightError, Bool.or_eq_false_iff] at h
      have iha := might_error_sound env hEnv a h.1
      have ihas := args_might_error_sound env hEnv as h.2
      show ¬d.IsErr
      cases hmem with
      | head _ => exact iha
      | tail _ htl => exact ihas d htl
end

end Mz

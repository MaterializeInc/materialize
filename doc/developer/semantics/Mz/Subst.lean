import Mz.Eval

/-!
# Substitution on indexed `Expr`

`Expr.subst es e` substitutes each column reference in `e` with
the corresponding entry of `es`. With schema indexing, the
substitution moves between schemas: starting from `e : Expr
sch_src k`, we produce `Expr sch_tgt k`, with `es : (i : Fin n_src)
→ Expr sch_tgt (sch_src.types.get i)` providing the per-column
replacement at the right type.

The substitution function is mutual with `ExprList.subst` to handle
the variadic constructors.

Soundness theorem `eval_subst`: evaluating the substituted expression
against `env_tgt` agrees with evaluating the original expression
against the env built from the substitutions:
`eval env_tgt (Expr.subst es e) = eval (fun i => eval env_tgt (es i)) e`.
-/

namespace Mz


mutual
  /-- Substitute column references in `e` using the per-column
  expressions `es`. -/
  def Expr.subst {n_src n_tgt : Nat}
      {sch_src : Schema n_src} {sch_tgt : Schema n_tgt}
      (es : (i : Fin n_src) → Expr sch_tgt (sch_src.types.get i)) :
      {k : ColType} → Expr sch_src k → Expr sch_tgt k
    | _, .lit d        => .lit d
    | _, .col i        => es i
    | _, .not a        => .not (Expr.subst es a)
    | _, .plus a b     => .plus (Expr.subst es a) (Expr.subst es b)
    | _, .minus a b    => .minus (Expr.subst es a) (Expr.subst es b)
    | _, .times a b    => .times (Expr.subst es a) (Expr.subst es b)
    | _, .divide a b   => .divide (Expr.subst es a) (Expr.subst es b)
    | _, .eq a b       => .eq (Expr.subst es a) (Expr.subst es b)
    | _, .lt a b       => .lt (Expr.subst es a) (Expr.subst es b)
    | _, .andN args    => .andN (ExprList.subst es args)
    | _, .orN args     => .orN (ExprList.subst es args)
    | _, .ifThen c t e => .ifThen (Expr.subst es c) (Expr.subst es t) (Expr.subst es e)
    | _, .coalesce args => .coalesce (ExprList.subst es args)

  /-- Pointwise substitution on `ExprList`. -/
  def ExprList.subst {n_src n_tgt : Nat}
      {sch_src : Schema n_src} {sch_tgt : Schema n_tgt}
      (es : (i : Fin n_src) → Expr sch_tgt (sch_src.types.get i)) :
      {k : ColType} → ExprList sch_src k → ExprList sch_tgt k
    | _, .nil       => .nil
    | _, .cons a as => .cons (Expr.subst es a) (ExprList.subst es as)
end

/-! ## Soundness

`eval_subst` and `evalList_subst` are mutual. The result-env is
built pointwise by evaluating each substitution: `fun i => eval
env_tgt (es i)`. -/

mutual
  theorem eval_subst {n_src n_tgt : Nat}
      {sch_src : Schema n_src} {sch_tgt : Schema n_tgt}
      (es : (i : Fin n_src) → Expr sch_tgt (sch_src.types.get i))
      (env_tgt : Env sch_tgt) :
      {k : ColType} → (e : Expr sch_src k) →
        eval env_tgt (Expr.subst es e) =
        eval (fun i => eval env_tgt (es i)) e
    | _, .lit d        => rfl
    | _, .col i        => rfl
    | _, .not a        => by simp [Expr.subst, eval, eval_subst es env_tgt a]
    | _, .plus a b     => by simp [Expr.subst, eval, eval_subst es env_tgt a, eval_subst es env_tgt b]
    | _, .minus a b    => by simp [Expr.subst, eval, eval_subst es env_tgt a, eval_subst es env_tgt b]
    | _, .times a b    => by simp [Expr.subst, eval, eval_subst es env_tgt a, eval_subst es env_tgt b]
    | _, .divide a b   => by simp [Expr.subst, eval, eval_subst es env_tgt a, eval_subst es env_tgt b]
    | _, .eq a b       => by simp [Expr.subst, eval, eval_subst es env_tgt a, eval_subst es env_tgt b]
    | _, .lt a b       => by simp [Expr.subst, eval, eval_subst es env_tgt a, eval_subst es env_tgt b]
    | _, .andN args    => by simp [Expr.subst, eval, evalList_subst es env_tgt args]
    | _, .orN args     => by simp [Expr.subst, eval, evalList_subst es env_tgt args]
    | _, .ifThen c t e => by
      simp [Expr.subst, eval,
            eval_subst es env_tgt c, eval_subst es env_tgt t, eval_subst es env_tgt e]
    | _, .coalesce args => by simp [Expr.subst, eval, evalList_subst es env_tgt args]

  theorem evalList_subst {n_src n_tgt : Nat}
      {sch_src : Schema n_src} {sch_tgt : Schema n_tgt}
      (es : (i : Fin n_src) → Expr sch_tgt (sch_src.types.get i))
      (env_tgt : Env sch_tgt) :
      {k : ColType} → (xs : ExprList sch_src k) →
        evalList env_tgt (ExprList.subst es xs) =
        evalList (fun i => eval env_tgt (es i)) xs
    | _, .nil       => rfl
    | _, .cons a as => by
      simp [ExprList.subst, evalList,
            eval_subst es env_tgt a, evalList_subst es env_tgt as]
end

end Mz

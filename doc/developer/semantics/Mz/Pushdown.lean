import Mz.Eval
import Mz.Bag

/-!
# Predicate pushdown across projection

The classical relational rewrite:

  `filterRel p (project es rel) = project es (filterRel (p[es]) rel)`

where `p[es]` is the substitution that replaces each `Expr.col i` in
`p` with `es.get i` (the i-th scalar of the projection). The
rewrite is the basis for moving a `WHERE` clause through a `SELECT`
clause whenever the predicate's column references can be expressed
in terms of the projection's source.

This file gives the substitution machinery (`Expr.subst`,
`Expr.substArgs`) and the two theorems an optimizer cites:

* `eval_subst`: substituting and then evaluating against the
  original row equals evaluating against the projected row.
* `filterRel_pushdown_project`: the relational pushdown rewrite.

`Expr.subst` is mutually recursive with `Expr.substArgs` so Lean's
structural-recursion checker handles the nested-list constructors
(`andN`, `orN`, `coalesce`) without an explicit termination measure.
-/

namespace Mz

/-! ## Substitution -/

mutual
/-- Substitute column references in `e` with the i-th scalar of
`es`. Out-of-bounds references are replaced by `.lit .null` so that
the resulting expression evaluates to `.null`, matching `Env.get`'s
fallback. -/
def Expr.subst (es : List Expr) : Expr → Expr
  | .lit d            => .lit d
  | .col i            => es.getD i (.lit .null)
  | .and a b          => .and (a.subst es) (b.subst es)
  | .or  a b          => .or  (a.subst es) (b.subst es)
  | .not a            => .not (a.subst es)
  | .ifThen c t e     => .ifThen (c.subst es) (t.subst es) (e.subst es)
  | .andN args        => .andN (Expr.substArgs es args)
  | .orN  args        => .orN  (Expr.substArgs es args)
  | .coalesce args    => .coalesce (Expr.substArgs es args)
  | .plus   a b       => .plus   (a.subst es) (b.subst es)
  | .minus  a b       => .minus  (a.subst es) (b.subst es)
  | .times  a b       => .times  (a.subst es) (b.subst es)
  | .divide a b       => .divide (a.subst es) (b.subst es)
  | .eq     a b       => .eq     (a.subst es) (b.subst es)
  | .lt     a b       => .lt     (a.subst es) (b.subst es)

/-- Pointwise application of `subst` to a list of operands. -/
def Expr.substArgs (es : List Expr) : List Expr → List Expr
  | []        => []
  | e :: rest => e.subst es :: Expr.substArgs es rest
end

/-! ## Helpers for substitution / map agreement

`substArgs es args` and `args.map (·.subst es)` produce the same
list. The skeleton uses the explicit recursive `substArgs` form in
the definition so structural recursion is accepted; the proofs
benefit from being able to switch to `List.map` when needed. -/

theorem Expr.substArgs_eq_map (es args : List Expr) :
    Expr.substArgs es args = args.map (·.subst es) := by
  induction args with
  | nil => rfl
  | cons hd tl ih => simp [Expr.substArgs, ih]

/-! ## Substitution preserves evaluation -/

/-- Reading column `i` from the projected row equals evaluating the
i-th projection scalar on the original row. The proof case-splits on
whether `i` is in bounds. -/
private theorem Env.get_map_eval (env : Env) (es : List Expr) (i : Nat) :
    Env.get (es.map (eval env)) i = eval env (es.getD i (.lit .null)) := by
  induction es generalizing i with
  | nil =>
    -- both sides reduce to `.null`
    show Env.get [] i = eval env (.lit .null)
    simp [Env.get, eval]
  | cons hd tl ih =>
    cases i with
    | zero =>
      -- LHS: Env.get (eval env hd :: tl.map (eval env)) 0 = eval env hd
      -- RHS: eval env ((hd :: tl).getD 0 (.lit .null)) = eval env hd
      show Env.get ((eval env hd) :: tl.map (eval env)) 0 = eval env hd
      rfl
    | succ n =>
      -- recurse on tl
      show Env.get (eval env hd :: tl.map (eval env)) (n + 1)
           = eval env ((hd :: tl).getD (n + 1) (.lit .null))
      show Env.get (tl.map (eval env)) n = eval env (tl.getD n (.lit .null))
      exact ih n

/-- The headline theorem: substituting into `e` and evaluating
against the original row equals evaluating the original `e` against
the projected row.

The proof is structural recursion on `e`, mirroring the structure of
`Expr.subst`. The nested-list constructors recurse through
`Expr.substArgs` and are handled by `eval_substArgs` below. -/
theorem eval_subst :
    ∀ (env : Env) (es : List Expr) (e : Expr),
      eval env (e.subst es) = eval (es.map (eval env)) e
  | env, es, .lit d => by
    simp only [Expr.subst, eval]
  | env, es, .col i => by
    simp only [Expr.subst, eval]
    exact (Env.get_map_eval env es i).symm
  | env, es, .and a b => by
    simp only [Expr.subst, eval]
    rw [eval_subst env es a, eval_subst env es b]
  | env, es, .or a b => by
    simp only [Expr.subst, eval]
    rw [eval_subst env es a, eval_subst env es b]
  | env, es, .not a => by
    simp only [Expr.subst, eval]
    rw [eval_subst env es a]
  | env, es, .ifThen c t e => by
    simp only [Expr.subst, eval]
    rw [eval_subst env es c, eval_subst env es t, eval_subst env es e]
  | env, es, .andN args => by
    simp only [Expr.subst, eval, Expr.substArgs_eq_map]
    rw [List.map_map]
    congr 1
    apply List.map_congr_left
    intro e _
    exact eval_subst env es e
  | env, es, .orN args => by
    simp only [Expr.subst, eval, Expr.substArgs_eq_map]
    rw [List.map_map]
    congr 1
    apply List.map_congr_left
    intro e _
    exact eval_subst env es e
  | env, es, .coalesce args => by
    simp only [Expr.subst, eval, Expr.substArgs_eq_map]
    rw [List.map_map]
    congr 1
    apply List.map_congr_left
    intro e _
    exact eval_subst env es e
  | env, es, .plus a b => by
    simp only [Expr.subst, eval]
    rw [eval_subst env es a, eval_subst env es b]
  | env, es, .minus a b => by
    simp only [Expr.subst, eval]
    rw [eval_subst env es a, eval_subst env es b]
  | env, es, .times a b => by
    simp only [Expr.subst, eval]
    rw [eval_subst env es a, eval_subst env es b]
  | env, es, .divide a b => by
    simp only [Expr.subst, eval]
    rw [eval_subst env es a, eval_subst env es b]
  | env, es, .eq a b => by
    simp only [Expr.subst, eval]
    rw [eval_subst env es a, eval_subst env es b]
  | env, es, .lt a b => by
    simp only [Expr.subst, eval]
    rw [eval_subst env es a, eval_subst env es b]

/-! ## Predicate pushdown -/

/-- The classical predicate-pushdown rewrite. Filtering after
projecting agrees with substituting the projection scalars into the
predicate and filtering before projecting. -/
theorem filterRel_pushdown_project (p : Expr) (es : List Expr) (rel : Relation) :
    filterRel p (project es rel) = project es (filterRel (p.subst es) rel) := by
  unfold filterRel project
  rw [List.filter_map]
  congr 1
  apply List.filter_congr
  intro row _
  -- Goal: (rowPredicate p ∘ (fun row => es.map (eval row))) row = rowPredicate (p.subst es) row
  show rowPredicate p (es.map (eval row)) = rowPredicate (p.subst es) row
  unfold rowPredicate
  rw [eval_subst row es p]

end Mz

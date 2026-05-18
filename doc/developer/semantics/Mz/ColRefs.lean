import Mz.Eval

/-!
# Column-reference analysis

The static analyzer `Expr.colReferencesBoundedBy n` returns `true`
when every `col i` reference in the expression has `i < n`. Used by
the optimizer to decide whether a predicate over a wide schema
mentions only the prefix (or only the suffix) of the row — the
precondition for pushing the predicate to one side of a join.

The headline theorem `eval_append_left_of_bounded` proves the
soundness side: when a predicate's column references are bounded by
`l.length`, evaluating it against the concatenated row `l ++ r`
agrees with evaluating against `l` alone. The right-side analogue
shifts column indices by `l.length`.

Both directions feed the join-pushdown theorems: a filter over a
cross product whose predicate touches only one side commutes with
the cross. The skeleton states the agreement here; the
relational-pushdown variant follows in future iterations of
`Mz/Join.lean`.
-/

namespace Mz

/-! ## Column-bound analyzer -/

mutual
/-- `Expr.colReferencesBoundedBy n e` returns `true` iff every
`col i` in `e` has `i < n`. -/
def Expr.colReferencesBoundedBy (n : Nat) : Expr → Bool
  | .lit _            => true
  | .col i            => decide (i < n)
  | .and a b          => a.colReferencesBoundedBy n && b.colReferencesBoundedBy n
  | .or  a b          => a.colReferencesBoundedBy n && b.colReferencesBoundedBy n
  | .not a            => a.colReferencesBoundedBy n
  | .ifThen c t e     =>
    c.colReferencesBoundedBy n &&
    t.colReferencesBoundedBy n &&
    e.colReferencesBoundedBy n
  | .andN args        => Expr.argsColRefBoundedBy n args
  | .orN  args        => Expr.argsColRefBoundedBy n args
  | .coalesce args    => Expr.argsColRefBoundedBy n args
  | .plus   a b       => a.colReferencesBoundedBy n && b.colReferencesBoundedBy n
  | .minus  a b       => a.colReferencesBoundedBy n && b.colReferencesBoundedBy n
  | .times  a b       => a.colReferencesBoundedBy n && b.colReferencesBoundedBy n
  | .divide a b       => a.colReferencesBoundedBy n && b.colReferencesBoundedBy n
  | .eq     a b       => a.colReferencesBoundedBy n && b.colReferencesBoundedBy n
  | .lt     a b       => a.colReferencesBoundedBy n && b.colReferencesBoundedBy n

/-- Companion fold over operand lists. `argsColRefBoundedBy n args`
returns `true` iff every operand passes the bound. -/
def Expr.argsColRefBoundedBy (n : Nat) : List Expr → Bool
  | []        => true
  | e :: rest => e.colReferencesBoundedBy n && Expr.argsColRefBoundedBy n rest
end

/-! ## Environment-append lemmas -/

/-- Reading a column index below `l.length` from `l ++ r` yields
the same value as reading from `l`. Trivial induction. -/
theorem Env.get_append_left :
    ∀ (l r : Env) (i : Nat), i < l.length →
      Env.get (l ++ r) i = Env.get l i
  | [],          _, _,     h => absurd h (Nat.not_lt_zero _)
  | hd :: _,     _, 0,     _ => rfl
  | _ :: tl,     r, n + 1, h => by
    show Env.get (tl ++ r) n = Env.get tl n
    exact Env.get_append_left tl r n (Nat.lt_of_succ_lt_succ h)

/-! ## Eval agreement under bound

If a predicate's column references are all bounded by `l.length`,
evaluating against `l ++ r` agrees with evaluating against `l`.
Joint structural recursion on `Expr` and the operand list. -/

mutual
theorem eval_append_left_of_bounded :
    ∀ (l r : Env) (e : Expr),
      e.colReferencesBoundedBy l.length = true →
      eval (l ++ r) e = eval l e
  | _, _, .lit _,        _ => by simp [eval]
  | l, r, .col i,        h => by
    have h_lt : i < l.length := of_decide_eq_true h
    simp only [eval]
    exact Env.get_append_left l r i h_lt
  | l, r, .and a b,      h => by
    simp only [Expr.colReferencesBoundedBy, Bool.and_eq_true] at h
    simp only [eval]
    rw [eval_append_left_of_bounded l r a h.1,
        eval_append_left_of_bounded l r b h.2]
  | l, r, .or a b,       h => by
    simp only [Expr.colReferencesBoundedBy, Bool.and_eq_true] at h
    simp only [eval]
    rw [eval_append_left_of_bounded l r a h.1,
        eval_append_left_of_bounded l r b h.2]
  | l, r, .not a,        h => by
    simp only [Expr.colReferencesBoundedBy] at h
    simp only [eval]
    rw [eval_append_left_of_bounded l r a h]
  | l, r, .ifThen c t e, h => by
    simp only [Expr.colReferencesBoundedBy, Bool.and_eq_true] at h
    simp only [eval]
    rw [eval_append_left_of_bounded l r c h.1.1,
        eval_append_left_of_bounded l r t h.1.2,
        eval_append_left_of_bounded l r e h.2]
  | l, r, .andN args, h => by
    simp only [Expr.colReferencesBoundedBy] at h
    simp only [eval]
    rw [eval_append_left_of_bounded_argsMap l r args h]
  | l, r, .orN args, h => by
    simp only [Expr.colReferencesBoundedBy] at h
    simp only [eval]
    rw [eval_append_left_of_bounded_argsMap l r args h]
  | l, r, .coalesce args, h => by
    simp only [Expr.colReferencesBoundedBy] at h
    simp only [eval]
    rw [eval_append_left_of_bounded_argsMap l r args h]
  | l, r, .plus a b, h => by
    simp only [Expr.colReferencesBoundedBy, Bool.and_eq_true] at h
    simp only [eval]
    rw [eval_append_left_of_bounded l r a h.1,
        eval_append_left_of_bounded l r b h.2]
  | l, r, .minus a b, h => by
    simp only [Expr.colReferencesBoundedBy, Bool.and_eq_true] at h
    simp only [eval]
    rw [eval_append_left_of_bounded l r a h.1,
        eval_append_left_of_bounded l r b h.2]
  | l, r, .times a b, h => by
    simp only [Expr.colReferencesBoundedBy, Bool.and_eq_true] at h
    simp only [eval]
    rw [eval_append_left_of_bounded l r a h.1,
        eval_append_left_of_bounded l r b h.2]
  | l, r, .divide a b, h => by
    simp only [Expr.colReferencesBoundedBy, Bool.and_eq_true] at h
    simp only [eval]
    rw [eval_append_left_of_bounded l r a h.1,
        eval_append_left_of_bounded l r b h.2]
  | l, r, .eq a b, h => by
    simp only [Expr.colReferencesBoundedBy, Bool.and_eq_true] at h
    simp only [eval]
    rw [eval_append_left_of_bounded l r a h.1,
        eval_append_left_of_bounded l r b h.2]
  | l, r, .lt a b, h => by
    simp only [Expr.colReferencesBoundedBy, Bool.and_eq_true] at h
    simp only [eval]
    rw [eval_append_left_of_bounded l r a h.1,
        eval_append_left_of_bounded l r b h.2]

/-- Operand-list agreement under bound. Mutually defined with the
`Expr` form so structural recursion accepts both. -/
theorem eval_append_left_of_bounded_argsMap :
    ∀ (l r : Env) (args : List Expr),
      Expr.argsColRefBoundedBy l.length args = true →
      args.map (eval (l ++ r)) = args.map (eval l)
  | _, _, [],         _ => rfl
  | l, r, e :: rest,  h => by
    simp only [Expr.argsColRefBoundedBy, Bool.and_eq_true] at h
    show eval (l ++ r) e :: rest.map (eval (l ++ r))
        = eval l e :: rest.map (eval l)
    rw [eval_append_left_of_bounded l r e h.1,
        eval_append_left_of_bounded_argsMap l r rest h.2]
end

end Mz

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

/-- Reading a column index `l.length + i` from `l ++ r` yields
the i-th value of `r`. The right-side analogue of
`Env.get_append_left`. -/
theorem Env.get_append_right :
    ∀ (l r : Env) (i : Nat),
      Env.get (l ++ r) (l.length + i) = Env.get r i
  | [],      _, _ => by
    show Env.get (([] : Env) ++ _) (0 + _) = _
    rw [List.nil_append, Nat.zero_add]
  | hd :: tl, r, i => by
    show Env.get ((hd :: tl) ++ r) (tl.length + 1 + i) = Env.get r i
    show Env.get (hd :: (tl ++ r)) (tl.length + 1 + i) = Env.get r i
    have h_rewrite : tl.length + 1 + i = (tl.length + i) + 1 := by omega
    rw [h_rewrite]
    show Env.get (tl ++ r) (tl.length + i) = Env.get r i
    exact Env.get_append_right tl r i

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

/-! ## Bound monotonicity

A predicate whose column references are bounded by `n` is also
bounded by any `m ≥ n`. Used to lift a tight per-relation bound
(e.g. `pred is bounded by table-A-width`) to a coarser join-env
bound (`bounded by combined-env-width`). -/

mutual
theorem Expr.colReferencesBoundedBy_mono :
    ∀ {n m : Nat} (e : Expr),
      e.colReferencesBoundedBy n = true → n ≤ m →
      e.colReferencesBoundedBy m = true
  | _, _, .lit _,            _, _ => rfl
  | n, m, .col i,            h, hLe => by
    have h_lt : i < n := of_decide_eq_true h
    show decide (i < m) = true
    exact decide_eq_true (Nat.lt_of_lt_of_le h_lt hLe)
  | _, _, .and a b,          h, hLe => by
    simp only [Expr.colReferencesBoundedBy, Bool.and_eq_true] at h ⊢
    exact ⟨Expr.colReferencesBoundedBy_mono a h.1 hLe,
           Expr.colReferencesBoundedBy_mono b h.2 hLe⟩
  | _, _, .or a b,           h, hLe => by
    simp only [Expr.colReferencesBoundedBy, Bool.and_eq_true] at h ⊢
    exact ⟨Expr.colReferencesBoundedBy_mono a h.1 hLe,
           Expr.colReferencesBoundedBy_mono b h.2 hLe⟩
  | _, _, .not a,            h, hLe => Expr.colReferencesBoundedBy_mono a h hLe
  | _, _, .ifThen c t e,     h, hLe => by
    simp only [Expr.colReferencesBoundedBy, Bool.and_eq_true] at h ⊢
    exact ⟨⟨Expr.colReferencesBoundedBy_mono c h.1.1 hLe,
            Expr.colReferencesBoundedBy_mono t h.1.2 hLe⟩,
           Expr.colReferencesBoundedBy_mono e h.2 hLe⟩
  | _, _, .andN args,        h, hLe => by
    simp only [Expr.colReferencesBoundedBy] at h ⊢
    exact Expr.argsColRefBoundedBy_mono args h hLe
  | _, _, .orN args,         h, hLe => by
    simp only [Expr.colReferencesBoundedBy] at h ⊢
    exact Expr.argsColRefBoundedBy_mono args h hLe
  | _, _, .coalesce args,    h, hLe => by
    simp only [Expr.colReferencesBoundedBy] at h ⊢
    exact Expr.argsColRefBoundedBy_mono args h hLe
  | _, _, .plus a b,         h, hLe => by
    simp only [Expr.colReferencesBoundedBy, Bool.and_eq_true] at h ⊢
    exact ⟨Expr.colReferencesBoundedBy_mono a h.1 hLe,
           Expr.colReferencesBoundedBy_mono b h.2 hLe⟩
  | _, _, .minus a b,        h, hLe => by
    simp only [Expr.colReferencesBoundedBy, Bool.and_eq_true] at h ⊢
    exact ⟨Expr.colReferencesBoundedBy_mono a h.1 hLe,
           Expr.colReferencesBoundedBy_mono b h.2 hLe⟩
  | _, _, .times a b,        h, hLe => by
    simp only [Expr.colReferencesBoundedBy, Bool.and_eq_true] at h ⊢
    exact ⟨Expr.colReferencesBoundedBy_mono a h.1 hLe,
           Expr.colReferencesBoundedBy_mono b h.2 hLe⟩
  | _, _, .divide a b,       h, hLe => by
    simp only [Expr.colReferencesBoundedBy, Bool.and_eq_true] at h ⊢
    exact ⟨Expr.colReferencesBoundedBy_mono a h.1 hLe,
           Expr.colReferencesBoundedBy_mono b h.2 hLe⟩
  | _, _, .eq a b,           h, hLe => by
    simp only [Expr.colReferencesBoundedBy, Bool.and_eq_true] at h ⊢
    exact ⟨Expr.colReferencesBoundedBy_mono a h.1 hLe,
           Expr.colReferencesBoundedBy_mono b h.2 hLe⟩
  | _, _, .lt a b,           h, hLe => by
    simp only [Expr.colReferencesBoundedBy, Bool.and_eq_true] at h ⊢
    exact ⟨Expr.colReferencesBoundedBy_mono a h.1 hLe,
           Expr.colReferencesBoundedBy_mono b h.2 hLe⟩

theorem Expr.argsColRefBoundedBy_mono :
    ∀ {n m : Nat} (args : List Expr),
      Expr.argsColRefBoundedBy n args = true → n ≤ m →
      Expr.argsColRefBoundedBy m args = true
  | _, _, [],         _, _ => rfl
  | _, _, e :: rest,  h, hLe => by
    simp only [Expr.argsColRefBoundedBy, Bool.and_eq_true] at h ⊢
    exact ⟨Expr.colReferencesBoundedBy_mono e h.1 hLe,
           Expr.argsColRefBoundedBy_mono rest h.2 hLe⟩
end

/-- Convenience: when the predicate's bound `n` is at most the
prefix length, `eval (l ++ r) e = eval l e`. Removes the need for
the predicate to know `l.length` exactly. -/
theorem eval_append_left_of_bounded_at
    (l r : Env) (n : Nat) (e : Expr)
    (hP : e.colReferencesBoundedBy n = true) (hLe : n ≤ l.length) :
    eval (l ++ r) e = eval l e :=
  eval_append_left_of_bounded l r e
    (Expr.colReferencesBoundedBy_mono e hP hLe)

/-! ## Column shifting

`Expr.colShift k e` adds `k` to every `col i` reference in `e`,
leaving other constructors structurally intact. Used to align a
predicate originally written against a right-side schema with the
joined env `l ++ r`: in the combined env, the right side starts at
index `l.length`, so shifting the predicate by `l.length` makes
its references land in the right half.

The headline `eval_append_right_shift` states agreement: evaluating
the shifted expression against `l ++ r` equals evaluating the
original against `r`. -/

mutual
def Expr.colShift (k : Nat) : Expr → Expr
  | .lit d            => .lit d
  | .col i            => .col (k + i)
  | .and a b          => .and (a.colShift k) (b.colShift k)
  | .or  a b          => .or  (a.colShift k) (b.colShift k)
  | .not a            => .not (a.colShift k)
  | .ifThen c t e     => .ifThen (c.colShift k) (t.colShift k) (e.colShift k)
  | .andN args        => .andN (Expr.argsColShift k args)
  | .orN  args        => .orN  (Expr.argsColShift k args)
  | .coalesce args    => .coalesce (Expr.argsColShift k args)
  | .plus   a b       => .plus   (a.colShift k) (b.colShift k)
  | .minus  a b       => .minus  (a.colShift k) (b.colShift k)
  | .times  a b       => .times  (a.colShift k) (b.colShift k)
  | .divide a b       => .divide (a.colShift k) (b.colShift k)
  | .eq     a b       => .eq     (a.colShift k) (b.colShift k)
  | .lt     a b       => .lt     (a.colShift k) (b.colShift k)

def Expr.argsColShift (k : Nat) : List Expr → List Expr
  | []        => []
  | e :: rest => e.colShift k :: Expr.argsColShift k rest
end

/-! ## Eval agreement under right-side shift

Evaluating the shifted expression against `l ++ r` agrees with
evaluating the original against `r`. The shift compensates for
the `l.length` offset in the combined env. -/

mutual
theorem eval_append_right_shift :
    ∀ (l r : Env) (e : Expr),
      eval (l ++ r) (e.colShift l.length) = eval r e
  | _, _, .lit _         => by simp [eval, Expr.colShift]
  | l, r, .col i         => by
    show eval (l ++ r) (.col (l.length + i)) = eval r (.col i)
    simp only [eval]
    exact Env.get_append_right l r i
  | l, r, .and a b       => by
    simp only [Expr.colShift, eval]
    rw [eval_append_right_shift l r a, eval_append_right_shift l r b]
  | l, r, .or a b        => by
    simp only [Expr.colShift, eval]
    rw [eval_append_right_shift l r a, eval_append_right_shift l r b]
  | l, r, .not a         => by
    simp only [Expr.colShift, eval]
    rw [eval_append_right_shift l r a]
  | l, r, .ifThen c t e  => by
    simp only [Expr.colShift, eval]
    rw [eval_append_right_shift l r c, eval_append_right_shift l r t,
        eval_append_right_shift l r e]
  | l, r, .andN args     => by
    simp only [Expr.colShift, eval]
    rw [eval_append_right_shift_argsMap l r args]
  | l, r, .orN args      => by
    simp only [Expr.colShift, eval]
    rw [eval_append_right_shift_argsMap l r args]
  | l, r, .coalesce args => by
    simp only [Expr.colShift, eval]
    rw [eval_append_right_shift_argsMap l r args]
  | l, r, .plus a b      => by
    simp only [Expr.colShift, eval]
    rw [eval_append_right_shift l r a, eval_append_right_shift l r b]
  | l, r, .minus a b     => by
    simp only [Expr.colShift, eval]
    rw [eval_append_right_shift l r a, eval_append_right_shift l r b]
  | l, r, .times a b     => by
    simp only [Expr.colShift, eval]
    rw [eval_append_right_shift l r a, eval_append_right_shift l r b]
  | l, r, .divide a b    => by
    simp only [Expr.colShift, eval]
    rw [eval_append_right_shift l r a, eval_append_right_shift l r b]
  | l, r, .eq a b        => by
    simp only [Expr.colShift, eval]
    rw [eval_append_right_shift l r a, eval_append_right_shift l r b]
  | l, r, .lt a b        => by
    simp only [Expr.colShift, eval]
    rw [eval_append_right_shift l r a, eval_append_right_shift l r b]

theorem eval_append_right_shift_argsMap :
    ∀ (l r : Env) (args : List Expr),
      (Expr.argsColShift l.length args).map (eval (l ++ r))
        = args.map (eval r)
  | _, _, [] => rfl
  | l, r, e :: rest => by
    show eval (l ++ r) (e.colShift l.length)
            :: (Expr.argsColShift l.length rest).map (eval (l ++ r))
        = eval r e :: rest.map (eval r)
    rw [eval_append_right_shift l r e,
        eval_append_right_shift_argsMap l r rest]
end

end Mz

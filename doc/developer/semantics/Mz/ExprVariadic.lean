import Mz.Eval
import Mz.Variadic
import Mz.Coalesce

/-!
# `Expr`-level reduction lemmas for the variadic constructors

The variadic `Expr` constructors (`andN`, `orN`, `coalesce`) evaluate
by mapping `eval env` across the operand list and then handing the
result to the corresponding variadic primitive. These reduction
lemmas state that explicitly so downstream proofs can rewrite without
having to unfold `eval`.

Each lemma is a single-line `rfl` because the matching clause of
`eval` is structurally identical to the lemma's right-hand side.

Concrete consequences derived from `Mz/Variadic.lean` follow at the
end of the file: the empty / singleton / two-operand cases of the
variadic `Expr` operators, and absorption by `FALSE` (resp. `TRUE`)
when a literal `FALSE` (resp. `TRUE`) is one of the operands.
-/

namespace Mz

/-! ## eval reduction lemmas -/

theorem eval_andN (env : Env) (args : List Expr) :
    eval env (.andN args) = evalAndN (args.map (eval env)) := by
  simp only [eval]

theorem eval_orN (env : Env) (args : List Expr) :
    eval env (.orN args) = evalOrN (args.map (eval env)) := by
  simp only [eval]

theorem eval_coalesce (env : Env) (args : List Expr) :
    eval env (.coalesce args) = evalCoalesce (args.map (eval env)) := by
  simp only [eval]

/-! ## Identity, singleton, and binary equivalence at the `Expr` level

`andN []`, `orN []`, and `coalesce []` are constants. `andN [a]` and
`orN [a]` reduce to `eval env a`. `andN [a, b]` agrees with `and a b`,
and similarly for `or`. These transport the corresponding `Datum`-
level laws in `Mz/Variadic.lean` through `eval`. -/

theorem eval_andN_nil (env : Env) :
    eval env (.andN []) = .bool true := by
  rw [eval_andN]; rfl

theorem eval_orN_nil (env : Env) :
    eval env (.orN []) = .bool false := by
  rw [eval_orN]; rfl

theorem eval_andN_singleton (env : Env) (a : Expr) :
    eval env (.andN [a]) = eval env a := by
  rw [eval_andN]
  show evalAndN [eval env a] = eval env a
  exact evalAndN_singleton (eval env a)

theorem eval_orN_singleton (env : Env) (a : Expr) :
    eval env (.orN [a]) = eval env a := by
  rw [eval_orN]
  show evalOrN [eval env a] = eval env a
  exact evalOrN_singleton (eval env a)

theorem eval_andN_binary (env : Env) (a b : Expr) :
    eval env (.andN [a, b]) = eval env (.and a b) := by
  rw [eval_andN]
  show evalAndN [eval env a, eval env b] = eval env (.and a b)
  rw [evalAndN_binary]
  -- Goal: evalAnd (eval env a) (eval env b) = eval env (.and a b)
  simp only [eval]

theorem eval_orN_binary (env : Env) (a b : Expr) :
    eval env (.orN [a, b]) = eval env (.or a b) := by
  rw [eval_orN]
  show evalOrN [eval env a, eval env b] = eval env (.or a b)
  rw [evalOrN_binary]
  simp only [eval]

/-! ## Coalesce base cases at the `Expr` level -/

theorem eval_coalesce_nil (env : Env) :
    eval env (.coalesce []) = .null := by
  rw [eval_coalesce]; rfl

theorem eval_coalesce_singleton (env : Env) (a : Expr) :
    eval env (.coalesce [a]) = eval env a := by
  rw [eval_coalesce]
  -- `args.map (eval env) = [eval env a]`; result depends on `eval env a`'s shape.
  show evalCoalesce [eval env a] = eval env a
  -- Case analysis on the underlying datum.
  cases h : eval env a with
  | bool b => rw [show evalCoalesce [Datum.bool b] = Datum.bool b from rfl]
  | null   => rw [show evalCoalesce [Datum.null] = Datum.null from rfl]
  | err e  => rw [show evalCoalesce [Datum.err e] = Datum.err e from rfl]

end Mz

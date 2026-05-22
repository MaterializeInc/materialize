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
    eval env (.andN [a, b]) = eval env (.and a b) := rfl

theorem eval_orN_binary (env : Env) (a b : Expr) :
    eval env (.orN [a, b]) = eval env (.or a b) := rfl

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
  | int  n => rw [show evalCoalesce [Datum.int n] = Datum.int n from rfl]
  | null   => rw [show evalCoalesce [Datum.null] = Datum.null from rfl]
  | err e  => rw [show evalCoalesce [Datum.err e] = Datum.err e from rfl]

/-! ## Variadic absorption at the `Expr` level

A single operand that evaluates to `FALSE` (resp. `TRUE`) makes the
whole variadic `AND` (resp. `OR`) evaluate to `FALSE` (resp. `TRUE`),
regardless of the other operands — including those that produce
`err`. These theorems transport `evalAndN_false_absorbs` /
`evalOrN_true_absorbs` (in `Mz/Variadic.lean`) through `eval`, and
are what an optimizer cites when folding `e₁ AND … AND falseᵢ AND …`
to `false`. The `lit` corollary specializes the semantic premise to
the syntactic case where one of the operands is the literal `.bool
false` / `.bool true`. -/

theorem eval_andN_false_absorbs {env : Env} {args : List Expr}
    (h : ∃ e ∈ args, eval env e = .bool false) :
    eval env (.andN args) = .bool false := by
  rw [eval_andN]
  apply evalAndN_false_absorbs (ds := args.map (eval env))
  obtain ⟨e, he_mem, he_eq⟩ := h
  exact List.mem_map.mpr ⟨e, he_mem, he_eq⟩

theorem eval_orN_true_absorbs {env : Env} {args : List Expr}
    (h : ∃ e ∈ args, eval env e = .bool true) :
    eval env (.orN args) = .bool true := by
  rw [eval_orN]
  apply evalOrN_true_absorbs (ds := args.map (eval env))
  obtain ⟨e, he_mem, he_eq⟩ := h
  exact List.mem_map.mpr ⟨e, he_mem, he_eq⟩

theorem eval_andN_lit_false_absorbs {env : Env} {args : List Expr}
    (h : Expr.lit (.bool false) ∈ args) :
    eval env (.andN args) = .bool false := by
  apply eval_andN_false_absorbs
  refine ⟨Expr.lit (.bool false), h, ?_⟩
  simp only [eval]

theorem eval_orN_lit_true_absorbs {env : Env} {args : List Expr}
    (h : Expr.lit (.bool true) ∈ args) :
    eval env (.orN args) = .bool true := by
  apply eval_orN_true_absorbs
  refine ⟨Expr.lit (.bool true), h, ?_⟩
  simp only [eval]

end Mz

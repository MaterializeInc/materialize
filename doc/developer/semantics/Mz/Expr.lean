import Mz.Datum

/-!
# `Expr`

A minimal subset of `MirScalarExpr` (see `src/expr/src/scalar.rs`).

## Why both binary `and` / `or` and variadic `andN` / `orN`?

Production `MirScalarExpr` carries only the variadic
`VariadicFunc::{And, Or}` form. The skeleton carries both
because each serves a different purpose:

* **Binary `and` / `or`** are the surface on which the four-value
  truth tables (TRUE / FALSE / NULL / ERROR) are stated and proved
  in `Mz/Laws.lean` and `Mz/Strict.lean`. The tables are pairwise
  by construction, so a binary operator is the natural object to
  reason about. Filter-fusion theorems
  (`UnifiedStream.filter_filter_fuse`) likewise quote `Expr.and q p`
  for the pair of predicates being fused.

* **Variadic `andN` / `orN`** match the runtime AST shape. An
  optimizer rewrite that targets `MirScalarExpr` directly cites
  `andN` / `orN`; the skeleton needs them to model that rewrite
  faithfully.

The two forms share semantics: `evalAndN` is the right-fold of
`evalAnd`, so every binary-form law lifts to the variadic form by
induction on the operand list (see `Mz/Variadic.lean` and
`Mz/ExprVariadic.lean`). A future iteration may collapse the two
constructors into one and treat the binary form as sugar
(`Expr.and a b := Expr.andN [a, b]`), at the cost of cascading
through every match site that destructures `.and a b`. The dual
representation is intentional for the current iteration; the cost
is bounded duplication on a small surface (only `eval`, the
column-reference analyses, and the substitution machinery
distinguish the cases).
-/

namespace Mz

/-- Scalar expression syntax.

* `lit d`: literal datum.
* `col i`: reference to column `i` in the surrounding environment.
* `and a b`, `or a b`: binary logical conjunction and disjunction.
* `not a`: logical negation.
* `ifThen c t e`: PostgreSQL-style `CASE` / `If` — the only
  user-controllable short-circuit in `MirScalarExpr`.
* `andN args`, `orN args`: variadic logical conjunction and
  disjunction. These match `MirScalarExpr::VariadicFunc::{And, Or}`
  in the runtime; the binary `and` / `or` constructors are kept
  alongside as the natural surface for the four-value truth tables
  (see the file docstring for why both forms exist).
* `coalesce args`: the error-rescuing variant of `COALESCE` proposed
  in `doc/developer/design/20260517_error_handling_semantics.md`. -/
inductive Expr
  | lit (d : Datum)
  | col (i : Nat)
  | and (a b : Expr)
  | or (a b : Expr)
  | not (a : Expr)
  | ifThen (c t e : Expr)
  | andN (args : List Expr)
  | orN (args : List Expr)
  | coalesce (args : List Expr)
  | plus   (a b : Expr)
  | minus  (a b : Expr)
  | times  (a b : Expr)
  | divide (a b : Expr)
  | eq     (a b : Expr)
  | lt     (a b : Expr)
  deriving Inhabited

end Mz

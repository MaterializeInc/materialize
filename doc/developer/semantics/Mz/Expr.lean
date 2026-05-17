import Mz.Datum

/-!
# `Expr`

A minimal subset of `MirScalarExpr` (see `src/expr/src/scalar.rs`).

The skeleton uses binary `and` and `or` rather than the variadic form
used in production. The boolean truth tables are pairwise, so binary
operators are sufficient to state and prove them. Generalizing to the
variadic form is straightforward via fold and is deferred to a later
iteration.
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
  alongside for proof convenience.
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
  deriving Inhabited

end Mz

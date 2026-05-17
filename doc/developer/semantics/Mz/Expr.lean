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
* `and a b`, `or a b`: logical conjunction and disjunction.
* `ifThen c t e`: PostgreSQL-style `CASE` / `If` — the only
  user-controllable short-circuit in `MirScalarExpr`. -/
inductive Expr
  | lit (d : Datum)
  | col (i : Nat)
  | and (a b : Expr)
  | or (a b : Expr)
  | ifThen (c t e : Expr)
  deriving Inhabited

end Mz

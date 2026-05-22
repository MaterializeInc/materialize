import Mz.Datum

/-!
# `Expr`

A minimal subset of `MirScalarExpr` (see `src/expr/src/scalar.rs`).

Production `MirScalarExpr` carries `VariadicFunc::{And, Or}`; the
skeleton mirrors that with `andN` / `orN` and does not carry a
separate binary `and` / `or`. The binary case is the two-element
variadic, and the four-value truth tables (TRUE / FALSE / NULL /
ERROR) over `evalAnd` / `evalOr` apply at every adjacent pair of
operands through the fold that defines `evalAndN` / `evalOrN`
(see `Mz/PrimEval.lean`).
-/

namespace Mz

/-- Scalar expression syntax.

* `lit d`: literal datum.
* `col i`: reference to column `i` in the surrounding environment.
* `not a`: logical negation.
* `ifThen c t e`: PostgreSQL-style `CASE` / `If` — the only
  user-controllable short-circuit in `MirScalarExpr`.
* `andN args`, `orN args`: variadic logical conjunction and
  disjunction. These match `MirScalarExpr::VariadicFunc::{And, Or}`
  in the runtime. The binary case is `andN [a, b]` / `orN [a, b]`.
* `coalesce args`: the error-rescuing variant of `COALESCE` proposed
  in `doc/developer/design/20260517_error_handling_semantics.md`. -/
inductive Expr
  | lit (d : Datum)
  | col (i : Nat)
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

/-- Sugar: binary AND as the two-element variadic. -/
@[inline] def Expr.and (a b : Expr) : Expr := Expr.andN [a, b]

/-- Sugar: binary OR as the two-element variadic. -/
@[inline] def Expr.or (a b : Expr) : Expr := Expr.orN [a, b]

end Mz

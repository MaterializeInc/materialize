import Mz.Datum
import Mz.Expr

/-!
# `eval`

Operational semantics for `Expr`.

`evalAnd` and `evalOr` are pattern-ordered to match Materialize's
current runtime behavior in `src/expr/src/scalar/func/variadic.rs`
(`And::eval`, `Or::eval`). Specifically:

* `FALSE` in either operand absorbs every other value, including
  `null` and `err`. Symmetric for `TRUE` in `OR`.
* Otherwise, `err` is the result whenever it appears, including when
  the other operand is `null`. This deliberately matches the current
  Mz behavior rather than the "NULL absorbs ERROR" rule discussed in
  `doc/developer/design/20260517_error_handling_semantics.md`.

A change to either rule should produce a corresponding diff in
`Mz/Boolean.lean`. That diff is the spec change.
-/

namespace Mz

/-- AND evaluation table.

The pattern order encodes the absorption hierarchy:
`FALSE > ERROR > NULL > TRUE`. -/
def evalAnd : Datum → Datum → Datum
  | .bool false, _ => .bool false
  | _, .bool false => .bool false
  | .err e, _      => .err e
  | _, .err e      => .err e
  | .null, _       => .null
  | _, .null       => .null
  | .bool true, .bool true => .bool true

/-- OR evaluation table.

Mirror of `evalAnd` with `TRUE` as the dominant absorber:
`TRUE > ERROR > NULL > FALSE`. -/
def evalOr : Datum → Datum → Datum
  | .bool true, _ => .bool true
  | _, .bool true => .bool true
  | .err e, _     => .err e
  | _, .err e     => .err e
  | .null, _      => .null
  | _, .null      => .null
  | .bool false, .bool false => .bool false

/-- Environment: a positional list of bindings for `Expr.col`. -/
abbrev Env := List Datum

/-- Reading an out-of-bounds column yields `NULL`.

This is a modeling choice for the skeleton. The real evaluator
expects callers to provide a well-typed row of the correct width;
the skeleton avoids that obligation by defaulting to `NULL`. -/
def Env.get (env : Env) (i : Nat) : Datum :=
  env.getD i .null

/-- Big-step evaluation. -/
def eval (env : Env) : Expr → Datum
  | .lit d        => d
  | .col i        => Env.get env i
  | .and a b      => evalAnd (eval env a) (eval env b)
  | .or  a b      => evalOr  (eval env a) (eval env b)
  | .ifThen c t e =>
    match eval env c with
    | .bool true  => eval env t
    | .bool false => eval env e
    | .null       => .null
    | .err err    => .err err

end Mz

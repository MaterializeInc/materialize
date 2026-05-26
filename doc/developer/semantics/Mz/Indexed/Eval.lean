import Mz.Indexed.Expr
import Mz.Indexed.PrimEval

/-!
# Big-step evaluator (indexed)

`eval (env : Env sch) (e : Expr sch k) : Datum k`. Schema-indexed.
The schema enforces:

* Column references have correct kinds (`.col i : Expr sch
  (sch.types.get i)`).
* Operators take operands of the kind they expect (no
  type-mismatch routing).

The environment is a typed lookup function:
`Env sch := (i : Fin n) → Datum (sch.types.get i)`. Each cell
returns a `Datum` whose kind agrees with the schema's declared
type for that column.

Mutual `eval` + `evalList` to handle variadic `ExprList`. Same
shape as `Mz/Eval.lean` modulo the indexing.

Modeling note on laziness carries over from the untyped model:
strict surface evaluation realizes the spec's lazy semantics for
absorbing operators (`.andN`, `.orN`, `.coalesce`) and for
`.ifThen`'s inactive arm via the absorption / discard rules in
`PrimEval`. -/

namespace Mz.Indexed

open Mz

/-- A typed environment for evaluating expressions over schema
`sch`. Cell `i` carries a `Datum` of the kind the schema declares
for column `i`. -/
def Env {n : Nat} (sch : Schema n) : Type :=
  (i : Fin n) → Datum (sch.types.get i)

mutual
  /-- Big-step evaluation. Mutual with `evalList`. -/
  def eval {n : Nat} {sch : Schema n} :
      {k : ColType} → Env sch → Expr sch k → Datum k
    | _, _, .lit d        => d
    | _, env, .col i      => env i
    | _, env, .not a      => evalNot (eval env a)
    | _, env, .plus a b   => evalPlus (eval env a) (eval env b)
    | _, env, .minus a b  => evalMinus (eval env a) (eval env b)
    | _, env, .times a b  => evalTimes (eval env a) (eval env b)
    | _, env, .divide a b => evalDivide (eval env a) (eval env b)
    | _, env, .eq a b     => evalEq (eval env a) (eval env b)
    | _, env, .lt a b     => evalLt (eval env a) (eval env b)
    | _, env, .andN args  => evalAndN (evalList env args)
    | _, env, .orN args   => evalOrN (evalList env args)
    | _, env, .ifThen c t e =>
      evalIfThen (eval env c) (eval env t) (eval env e)
    | _, env, .coalesce args => evalCoalesce (evalList env args)

  /-- Evaluate an `ExprList` to a `List (Datum k)`. -/
  def evalList {n : Nat} {sch : Schema n} :
      {k : ColType} → Env sch → ExprList sch k → List (Datum k)
    | _, _, .nil         => []
    | _, env, .cons a as => eval env a :: evalList env as
end

end Mz.Indexed

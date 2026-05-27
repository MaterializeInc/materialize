import Mz.Expr
import Mz.PrimEval

/-!
# Big-step evaluator

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

## Laziness

`.ifThen c t e` dispatches inline on `eval env c` and only
evaluates the selected branch. Eager dispatch through a
`Datum`-level `evalIfThen` primitive would compute both `t` and
`e` regardless — wasteful, and under `Datum.err _` semantics
risks producing an err in the discarded branch.

The variadic constructors `.andN` / `.orN` / `.coalesce`
delegate to `evalList` + the corresponding combinator
(`evalAndN` / `evalOrN` / `evalCoalesce`). This form is eager
(builds the full `List (Datum k)` first) but result-faithful to
the spec:

* `evalAndN` is a right-fold via `evalAnd`; `evalAnd .bool false _`
  is the first pattern (FALSE absorbs every other input including
  `.err _`). So the eager form produces the same value as a
  left-to-right lazy short-circuit on `.bool false` would. Same
  story for `evalOrN` with `.bool true`.
* `evalCoalesce` is a two-pass form (`Coalesce.firstConcrete` +
  `Coalesce.residue`); the spec's `null > err` residue rule
  requires scanning operands after an err head to detect a later
  null, so structural laziness beyond "stop on first concrete"
  has no win.

The eager variadic shape stays for proof scaffolding (`evalAndN`
/ `evalOrN` / `evalCoalesce` on `List (Datum k)` carry the
absorption / residue laws); refactoring to `ExprList`-direct
lazy forms is queued. -/

namespace Mz


/-- A typed environment for evaluating expressions over schema
`sch`. Cell `i` carries a `Datum` of the kind the schema declares
for column `i`. -/
def Env {n : Nat} (sch : Schema n) : Type :=
  (i : Fin n) → Datum (sch.types.get i)

mutual
  /-- Big-step evaluation. Mutual with `evalList` for the variadic
  constructors. `.ifThen` dispatches lazily inline. -/
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
      match eval env c with
      | .bool true  => eval env t
      | .bool false => eval env e
      | .null       => .null
      | .err err    => .err err
    | _, env, .coalesce args => evalCoalesce (evalList env args)

  /-- Evaluate an `ExprList` to a `List (Datum k)`. -/
  def evalList {n : Nat} {sch : Schema n} :
      {k : ColType} → Env sch → ExprList sch k → List (Datum k)
    | _, _, .nil         => []
    | _, env, .cons a as => eval env a :: evalList env as
end

end Mz

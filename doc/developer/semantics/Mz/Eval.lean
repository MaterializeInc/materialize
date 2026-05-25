import Mz.Expr
import Mz.PrimEval

/-!
# `eval`

Big-step evaluator. The primitive operations on `Datum` and
`List Datum` live in `Mz/PrimEval.lean`; this file only defines the
`Expr` → `Datum` translation.

For the variadic constructors `andN`, `orN`, and `coalesce`, the
evaluator first evaluates every operand and then hands the resulting
`List Datum` to the variadic primitive. Operand evaluation uses
`List.attach` so that Lean's structural recursion checker can see
each element of `args` as a subterm of the enclosing `Expr`.

Modeling note on laziness: in PG, the boolean fragment short-
circuits — once a `FALSE` is seen, the rest of the `AND` operands are
not evaluated. In this total skeleton, every `eval env e` is a total
function of its inputs.

The absorption theorems in `Mz/Variadic.lean` guarantee that strict
evaluation produces the same `Datum` as a lazy runtime *whenever the
absorbing operand appears before the erroring one in the operand list*.
The other order produces a different observable: on
`[1/0, .bool false]`, strict eval yields `.err divisionByZero`
because every operand is pre-evaluated; a lazy `AND` reading
left-to-right would error the same way; a *reordered* lazy `AND`
that hits the `.bool false` first would short-circuit and yield
`.bool false`. The strict model here matches Materialize's runtime
(which is also strict at this layer), not PG short-circuit semantics.
A future iteration that introduces effects (resource usage,
partiality, observability) will need to reintroduce the laziness
explicitly.
-/

namespace Mz

/-- Big-step evaluation. -/
def eval (env : Env) : Expr → Datum
  | .lit d        => d
  | .col i        => Env.get env i
  | .not a        => evalNot (eval env a)
  | .ifThen c t e => evalIfThen (eval env c) (eval env t) (eval env e)
  | .andN args     => evalAndN     (args.map (eval env))
  | .orN args      => evalOrN      (args.map (eval env))
  | .coalesce args => evalCoalesce (args.map (eval env))
  | .plus   a b   => evalPlus   (eval env a) (eval env b)
  | .minus  a b   => evalMinus  (eval env a) (eval env b)
  | .times  a b   => evalTimes  (eval env a) (eval env b)
  | .divide a b   => evalDivide (eval env a) (eval env b)
  | .eq     a b   => evalEq     (eval env a) (eval env b)
  | .lt     a b   => evalLt     (eval env a) (eval env b)

end Mz

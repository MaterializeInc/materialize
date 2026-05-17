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

Modeling note on laziness: in the runtime, the boolean fragment short-
circuits — once a `FALSE` is seen, the rest of the `AND` operands are
not evaluated. In this total skeleton, every `eval env e` is a total
function of its inputs, and the absorption theorems in
`Mz/Variadic.lean` guarantee that strict evaluation produces the same
`Datum` as the lazy runtime would. A future iteration that introduces
effects (resource usage, partiality, observability) will need to
reintroduce the laziness explicitly.
-/

namespace Mz

/-- Big-step evaluation. -/
def eval (env : Env) : Expr → Datum
  | .lit d        => d
  | .col i        => Env.get env i
  | .and a b      => evalAnd (eval env a) (eval env b)
  | .or  a b      => evalOr  (eval env a) (eval env b)
  | .not a        => evalNot (eval env a)
  | .ifThen c t e => evalIfThen (eval env c) (eval env t) (eval env e)
  | .andN args     => evalAndN     (args.map (eval env))
  | .orN args      => evalOrN      (args.map (eval env))
  | .coalesce args => evalCoalesce (args.map (eval env))

end Mz

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

Modeling note on laziness: this skeleton implements *lazy semantics
for branching and absorbing operators* via a strict surface
evaluator. Concretely:

* `evalIfThen .bool true dt de = dt` even when `de = .err _`. The
  inactive arm's value is computed (the eval is strict on `Expr`
  arguments) but the result is then *discarded* by `evalIfThen`. The
  effect on the observable `Datum` is the same as if the inactive
  arm were never evaluated.
* `evalCoalesce` past the first concrete operand discards subsequent
  operand values, even if they would have erred.
* `evalAndN` / `evalOrN`: the absorption theorems in
  `Mz/Variadic.lean` guarantee that strict evaluation produces the
  same `Datum` as a lazy runtime *whenever the absorbing operand
  appears before the erroring one in the operand list*. The other
  order produces a different observable: on `[1/0, .bool false]`,
  strict eval yields `.err divisionByZero` because every operand is
  pre-evaluated; a lazy reordered `AND` that hits the `.bool false`
  first would short-circuit and yield `.bool false`. The variadic
  case is the only place strict surface evaluation diverges from
  the implicit lazy semantics.

Net: the observable semantics of `eval` is closer to a lazy
evaluator than the strict surface might suggest. The `refines`
posture in the design doc is framed as "evaluation-order
reorderability under strict semantics"; readers should be aware
that the actual evaluator is closer to lazy and the reorderability
story differs. A future iteration that introduces effects (resource
usage, partiality, observability) will need to reintroduce the
laziness explicitly. The relational `Mz/Legal.lean` form is the
natural next step for SQL-faithful evaluation-order reasoning.
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

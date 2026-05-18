/-!
# `Datum`

The subset of Materialize's `Datum` modeled by the semantics skeleton.

Only the variants required to state the boolean truth tables are
present: booleans, `null`, and the proposed `err` variant carrying an
opaque `EvalError` payload. Numeric, string, and temporal types are
intentionally omitted from this skeleton; adding them later does not
affect the proofs in `Mz/Boolean.lean`.

The Rust counterpart lives in `src/repr/src/row.rs` (`Datum`) and
`src/expr/src/scalar.rs` (`EvalError`).
-/

namespace Mz

/-- Cell-scoped errors raised by `Datum`-level operations. The
skeleton's variants are intentionally small; production
`EvalError` (in `src/expr/src/scalar.rs`) has many more. -/
inductive EvalError
  | placeholder
  | divisionByZero
  deriving DecidableEq, Inhabited

/-- A modeled scalar value.

`bool b` is a boolean literal; `int n` is an integer literal
(skeleton models `Int`, not the full SQL numeric tower); `null`
is the SQL `NULL` value; `err e` is the cell-scoped error
variant whose payload is the `EvalError` raised at the cell. -/
inductive Datum
  | bool (b : Bool)
  | int  (n : Int)
  | null
  | err  (e : EvalError)
  deriving DecidableEq, Inhabited

/-- Propositional predicate "this datum is an error".

Stated as `Prop` rather than `Bool` so it composes with `Not` and is
usable as a hypothesis in proofs without first lifting through
`= true`. The recursor over `Datum` collapses each branch to either
`True` or `False`, so `decide` (and `simp`) handles `IsErr` cleanly. -/
def Datum.IsErr : Datum → Prop
  | .err _ => True
  | _      => False

instance : DecidablePred Datum.IsErr := by
  intro d
  cases d <;> unfold Datum.IsErr <;> infer_instance

end Mz

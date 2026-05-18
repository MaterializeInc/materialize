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

/-- Opaque payload for cell-scoped errors.

The skeleton does not enumerate the variants of the Rust
`EvalError`. A single placeholder constructor keeps the type
inhabited so that proofs that need a concrete value can supply one,
without committing to a wire format. Later refinements will replace
this with the real variant set. -/
inductive EvalError
  | placeholder
  deriving DecidableEq, Inhabited

/-- A modeled scalar value.

`bool b` is a boolean literal; `null` is the SQL `NULL` value; `err e`
is the proposed cell-scoped error variant whose payload is the
`EvalError` raised at the cell. -/
inductive Datum
  | bool (b : Bool)
  | null
  | err (e : EvalError)
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

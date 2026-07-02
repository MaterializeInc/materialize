import Mz.Schema

/-!
# `Datum k` — GADT-indexed datum

`Datum : ColType → Type`. The kind discipline is at the type level:

* `Datum .bool` admits `.bool _`, `.null`, `.err _`.
* `Datum .int`  admits `.int _`,  `.null`, `.err _`.
* `Datum .top`  admits only `.null`, `.err _` — the polymorphic
  case for unknown / universal kinds.

The `.null` and `.err _` constructors are universally quantified
over the kind index. Pattern matching on a `Datum k` for known `k`
prunes the constructors not inhabiting that kind, so evaluators
written against `Datum .bool` see only the boolean fragment and
need no catch-all routing. -/

namespace Mz


/-- A scalar value indexed by its `ColType`. -/
inductive Datum : ColType → Type
  | bool (b : Bool) : Datum .bool
  | int (n : Int)   : Datum .int
  | null            : Datum k
  | err (e : EvalError) : Datum k

namespace Datum

/-- Propositional predicate "this datum is an error". Carried at
the indexed level: an `.err _` of any kind satisfies it. -/
def IsErr {k : ColType} : Datum k → Prop
  | .err _ => True
  | _      => False

instance {k : ColType} : DecidablePred (@IsErr k) := by
  intro d
  cases d <;> unfold IsErr <;> infer_instance

/-- Propositional predicate "this datum is the null value". -/
def IsNull {k : ColType} : Datum k → Prop
  | .null  => True
  | _      => False

instance {k : ColType} : DecidablePred (@IsNull k) := by
  intro d
  cases d <;> unfold IsNull <;> infer_instance

/-- Bool variant of `IsNull`. Sole consumer is `Coalesce.residue`'s
`rest.any Datum.isNullB` check in `Mz/PrimEval.lean` — `List.any`
needs a `Bool` predicate, and the `Prop`-valued `IsNull` doesn't
compose there. -/
def isNullB {k : ColType} : Datum k → Bool
  | .null => true
  | _     => false

end Datum

end Mz

import Mz.Indexed.Eval
import Mz.Indexed.OutputType

/-!
# Indexed schema-satisfaction predicates

The `Schema` / `ColSchema` / `ColType` definitions are re-used from
`Mz/Schema.lean`. This file adds the indexed counterparts of
`RowSatisfies`, `Update.Satisfies`, and `Collection.Satisfies`
that work over the schema-indexed `Env` and the indexed `Datum k`.

Untyped `RowSatisfies` (in `Mz/Schema.lean`) takes a list-vector
row and checks per-cell nullable / errable. The indexed version
takes an `Env sch` (typed lookup) — kind discipline is already
enforced by `Env`'s signature, so the satisfaction predicate only
needs to track the `cols` (nullable, errable) bits and the
collection-level `rowErrFree` flag.

The `cellErrFree` schema-side projection is reused as-is from
`Mz/Schema.lean`. -/

namespace Mz.Indexed

open Mz

/-- A typed environment satisfies a schema iff every cell satisfies
the corresponding `ColSchema`. The kind half is enforced
structurally by the indexing of `Env`. -/
def EnvSatisfies {n : Nat} (sch : Schema n) (env : Env sch) : Prop :=
  ∀ i : Fin n, DatumSatisfies (sch.cols.get i) (env i)

end Mz.Indexed

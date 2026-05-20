import Mz.UnifiedStream2
import Mz.UnifiedConsolidate2
import Mz.DiffWithGlobal

/-!
# Per-`(row, time)` consolidation on `UnifiedStream2`

Mirror of `Mz/TimedConsolidate.lean` over the new diff encoding from
`Mz/UnifiedStream2.lean`. The carrier is just `Row` (no `.err`
constructor) and the diff is `DiffWithGlobal`, so a timed record is
the triple `(Row, Nat, DiffWithGlobal)`. The per-time slice
`atTime t` keeps only records at time `t` and forgets the time
component, producing a plain `UnifiedStream2`. Composing with
`UnifiedStream2.consolidate` gives `consolidateAtTime`.

The frontier operator `advanceFrontier f` lifts each record's time
to at least `f` via `Nat.max`, identical in shape to the old module.

Only the definition and the trivial per-shape reductions ship here;
absorption, cardinality, and retraction laws are left for follow-up
modules in keeping with `Mz/UnifiedConsolidate2.lean`. -/

namespace Mz

/-- A timed record on the new unified stream: carrier, time, diff.
The carrier is the plain `Row` from `Mz/UnifiedStream2.lean`; the
diff is `DiffWithGlobal`, which separates row-scoped errs (inside
`Diff.errs`) from collection-scoped errs (`DiffWithGlobal.global`). -/
abbrev TimedUnifiedRecord2 := Row × Nat × DiffWithGlobal

/-- Differential-dataflow-style stream of timed records over the new
diff encoding. -/
abbrev TimedUnifiedStream2 := List TimedUnifiedRecord2

/-! ## Frontier advance

Records with time strictly before frontier `f` are "advanced" to
`f` (their time is updated to `f`), making the past immutable.
Records at or past `f` are left untouched. The encoding tracks
frontiers as a single `Nat`, sufficient to state the algebraic
laws. -/

/-- Advance every record's time to at least `f`. Records originally
at time `< f` move to `f`; records already at `≥ f` stay. -/
def TimedUnifiedStream2.advanceFrontier (f : Nat) (s : TimedUnifiedStream2) :
    TimedUnifiedStream2 :=
  s.map fun r => (r.1, Nat.max r.2.1 f, r.2.2)

/-! ## Time-slice projection -/

/-- Project a timed stream to the time slice at `t`. Records at
other times are dropped; the time component is forgotten, producing
an ordinary `UnifiedStream2`. -/
def TimedUnifiedStream2.atTime (t : Nat) (s : TimedUnifiedStream2) :
    UnifiedStream2 :=
  s.filterMap fun r =>
    if r.2.1 = t then some (r.1, r.2.2) else none

/-- Bucket records at time `t` by carrier and sum their diffs. -/
def TimedUnifiedStream2.consolidateAtTime (t : Nat) (s : TimedUnifiedStream2) :
    UnifiedStream2 :=
  UnifiedStream2.consolidate (TimedUnifiedStream2.atTime t s)

/-! ## Reduction lemmas

Named per-shape reductions so downstream proofs cite a single
lemma instead of unfolding `map` / `filterMap` inline. -/

theorem TimedUnifiedStream2.advanceFrontier_nil (f : Nat) :
    TimedUnifiedStream2.advanceFrontier f [] = [] := rfl

theorem TimedUnifiedStream2.advanceFrontier_append
    (f : Nat) (a b : TimedUnifiedStream2) :
    TimedUnifiedStream2.advanceFrontier f (a ++ b)
      = TimedUnifiedStream2.advanceFrontier f a
          ++ TimedUnifiedStream2.advanceFrontier f b := by
  show (a ++ b).map _ = a.map _ ++ b.map _
  exact List.map_append

theorem TimedUnifiedStream2.atTime_nil (t : Nat) :
    TimedUnifiedStream2.atTime t [] = [] := rfl

theorem TimedUnifiedStream2.consolidateAtTime_nil (t : Nat) :
    TimedUnifiedStream2.consolidateAtTime t [] = [] := rfl

end Mz

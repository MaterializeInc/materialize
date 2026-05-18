import Mz.Bag
import Mz.DiffSemiring
import Mz.Consolidate

/-!
# Timed records — `(Row, Time, Diff)` triple stream

A first sketch of differential dataflow's record format: a stream
of `(row, time, diff)` triples, where the diff lives in
`DiffWithError α`. The skeleton uses `Nat` for time and parametrizes
over the base diff type.

The operations modeled are consolidation by time (sum diffs across
all rows at a given time) and consolidation across the whole stream
(sum every diff). Both reduce to `DiffWithError.sumAll`, so the
absorption laws from `Mz/Consolidate.lean` transport directly: if
any record in the consolidated range carries an `error` diff, the
consolidated total is `error`.

Per-`(row, time)` bucketing is the next refinement and requires
`DecidableEq` on `Row`; the present file does the simpler "sum
everything in the time slice" version, which is the per-time
collection-global diff.
-/

namespace Mz

/-- A timed record: row, time, and a diff value possibly carrying
the absorbing `error` marker. -/
structure TimedRecord (α : Type) where
  row  : Row
  time : Nat
  diff : DiffWithError α
  deriving Inhabited

/-- A stream of timed records. Order does not matter; the operations
below are insensitive to order whenever the base `Add` on `α` is
commutative. -/
abbrev TimedStream (α : Type) := List (TimedRecord α)

/-- Sum every diff in the stream, ignoring row and time. The
collection-wide diff. -/
def TimedStream.consolidateAll [Zero α] [Add α] (s : TimedStream α) :
    DiffWithError α :=
  DiffWithError.sumAll (s.map (·.diff))

/-- Sum every diff at a given time, ignoring row. The per-time
collection diff. -/
def TimedStream.consolidateAt [Zero α] [Add α] (t : Nat) (s : TimedStream α) :
    DiffWithError α :=
  DiffWithError.sumAll ((s.filter (·.time = t)).map (·.diff))

/-! ## Absorption -/

/-- If any record carries an `error` diff, the all-stream consolidation
is `error`. -/
theorem TimedStream.consolidateAll_eq_error_of_mem [Zero α] [Add α]
    {s : TimedStream α} (r : TimedRecord α)
    (h_mem : r ∈ s) (h_err : r.diff = DiffWithError.error) :
    TimedStream.consolidateAll s = DiffWithError.error := by
  unfold TimedStream.consolidateAll
  apply DiffWithError.sumAll_eq_error_of_mem
  -- Need: error ∈ s.map (·.diff). Since r ∈ s and r.diff = error, by List.mem_map.
  refine List.mem_map.mpr ⟨r, h_mem, ?_⟩
  exact h_err

/-- Same statement restricted to a single time slice: an `error`
record at time `t` forces the per-time consolidation at `t` to
`error`. -/
theorem TimedStream.consolidateAt_eq_error_of_mem [Zero α] [Add α]
    {s : TimedStream α} (t : Nat) (r : TimedRecord α)
    (h_mem : r ∈ s) (h_time : r.time = t)
    (h_err : r.diff = DiffWithError.error) :
    TimedStream.consolidateAt t s = DiffWithError.error := by
  unfold TimedStream.consolidateAt
  apply DiffWithError.sumAll_eq_error_of_mem
  refine List.mem_map.mpr ⟨r, ?_, h_err⟩
  exact List.mem_filter.mpr ⟨h_mem, by simp [h_time]⟩

end Mz

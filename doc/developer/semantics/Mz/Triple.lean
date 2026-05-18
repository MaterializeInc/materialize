import Mz.UnifiedStream
import Mz.TimedConsolidate
import Mz.DiffSemiring
import Mz.Consolidate

/-!
# Collection-wide diff sum on the timed unified stream

Differential dataflow records arrive as `(row, time, diff)` triples.
The carrier here is `TimedUnifiedRecord` from
`Mz/TimedConsolidate.lean`, which pairs a `UnifiedRow` (data row
or row-scoped err) with a `Nat` time and a `DiffWithError Int`
diff. Row-scoped errors flow through the carrier; collection-
scoped errors flow through the diff via the absorbing `.error`
marker.

This file gives two consolidation views that *do not* bucket by
row:

* `consolidateAll`: sum every diff in the stream, ignoring row
  and time. The collection-wide diff.
* `consolidateAt t`: sum every diff at time `t`, ignoring row.
  The per-time collection diff.

Both reduce to `DiffWithError.sumAll`, so the absorption laws
from `Mz/Consolidate.lean` transport directly: an `.error` diff
anywhere in the consolidated range forces the consolidated total
to `.error`.

For per-`(row, time)` bucketing — where the output is itself a
`UnifiedStream`, one record per surviving carrier with the
bucket's summed diff — use `TimedUnifiedStream.consolidateAtTime`
in `Mz/TimedConsolidate.lean`. The two views are complementary:
this file collapses time slices to a single diff value; the
TimedConsolidate view collapses each `(row, time)` bucket
separately.
-/

namespace Mz

/-- Sum every diff in the stream, ignoring row and time. -/
def TimedUnifiedStream.consolidateAll (s : TimedUnifiedStream) : DiffWithError Int :=
  DiffWithError.sumAll (s.map (·.2.2))

/-- Sum every diff at a given time, ignoring row. -/
def TimedUnifiedStream.consolidateAtTimeFlat
    (t : Nat) (s : TimedUnifiedStream) : DiffWithError Int :=
  DiffWithError.sumAll ((s.filter (·.2.1 = t)).map (·.2.2))

/-! ## Absorption -/

/-- An `.error` diff anywhere in the stream forces the
collection-wide consolidation to `.error`. -/
theorem TimedUnifiedStream.consolidateAll_eq_error_of_mem
    {s : TimedUnifiedStream} (r : TimedUnifiedRecord)
    (h_mem : r ∈ s) (h_err : r.2.2 = (DiffWithError.error : DiffWithError Int)) :
    TimedUnifiedStream.consolidateAll s = DiffWithError.error := by
  unfold TimedUnifiedStream.consolidateAll
  apply DiffWithError.sumAll_eq_error_of_mem
  refine List.mem_map.mpr ⟨r, h_mem, ?_⟩
  exact h_err

/-- Restricted to a time slice: an `.error` record at time `t`
forces the per-time flat consolidation at `t` to `.error`. -/
theorem TimedUnifiedStream.consolidateAtTimeFlat_eq_error_of_mem
    {s : TimedUnifiedStream} (t : Nat) (r : TimedUnifiedRecord)
    (h_mem : r ∈ s) (h_time : r.2.1 = t)
    (h_err : r.2.2 = (DiffWithError.error : DiffWithError Int)) :
    TimedUnifiedStream.consolidateAtTimeFlat t s = DiffWithError.error := by
  unfold TimedUnifiedStream.consolidateAtTimeFlat
  apply DiffWithError.sumAll_eq_error_of_mem
  refine List.mem_map.mpr ⟨r, ?_, h_err⟩
  exact List.mem_filter.mpr ⟨h_mem, by simp [h_time]⟩

/-! ## Reverse direction: from `.error` total to `.error` record -/

/-- If the collection-wide consolidation is `.error`, at least
one record in the stream carries an `.error` diff. The converse
of `consolidateAll_eq_error_of_mem`. -/
theorem TimedUnifiedStream.consolidateAll_error_inv
    {s : TimedUnifiedStream}
    (h : TimedUnifiedStream.consolidateAll s = DiffWithError.error) :
    ∃ r ∈ s, r.2.2 = (DiffWithError.error : DiffWithError Int) := by
  unfold TimedUnifiedStream.consolidateAll at h
  obtain ⟨d, hMem, hD⟩ := DiffWithError.sumAll_error_inv h
  obtain ⟨r, hRMem, hRD⟩ := List.mem_map.mp hMem
  exact ⟨r, hRMem, by rw [hRD]; exact hD⟩

/-- Time-slice version: an `.error` total at time `t` witnesses
an `.error` record at time `t`. -/
theorem TimedUnifiedStream.consolidateAtTimeFlat_error_inv
    {s : TimedUnifiedStream} (t : Nat)
    (h : TimedUnifiedStream.consolidateAtTimeFlat t s = DiffWithError.error) :
    ∃ r ∈ s, r.2.1 = t ∧ r.2.2 = (DiffWithError.error : DiffWithError Int) := by
  unfold TimedUnifiedStream.consolidateAtTimeFlat at h
  obtain ⟨d, hMem, hD⟩ := DiffWithError.sumAll_error_inv h
  obtain ⟨r, hRMem, hRD⟩ := List.mem_map.mp hMem
  have hRFilter : r ∈ s.filter (·.2.1 = t) := hRMem
  rw [List.mem_filter] at hRFilter
  refine ⟨r, hRFilter.1, ?_, ?_⟩
  · exact of_decide_eq_true hRFilter.2
  · rw [hRD]; exact hD

end Mz

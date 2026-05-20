import Mz.UnifiedStream2
import Mz.TimedConsolidate2
import Mz.DiffWithGlobal
import Mz.Consolidate2

/-!
# Collection-wide diff sum on the new timed unified stream

Mirror of `Mz/Triple.lean` over the two-layer diff encoding from
`Mz/DiffWithGlobal.lean`. The carrier here is `TimedUnifiedRecord2`
from `Mz/TimedConsolidate2.lean`, which pairs a `Row` (the new
err-free carrier) with a `Nat` time and a `DiffWithGlobal` diff.
Row-scoped errs now live inside `Diff.errs`; collection-scoped errs
live in the diff via the absorbing `.global` marker.

The two consolidation views below do *not* bucket by row:

* `consolidateAll`: sum every diff in the stream, ignoring row and
  time. The collection-wide diff.
* `consolidateAtTimeFlat`: sum every diff at time `t`, ignoring row.
  The per-time collection diff.

Both reduce to `DiffWithGlobal.sumAll`, so the absorption laws from
`Mz/Consolidate2.lean` transport directly: a `.global` diff anywhere
in the consolidated range forces the consolidated total to
`.global`.

For per-`(row, time)` bucketing — where the output is itself a
`UnifiedStream2`, one record per surviving carrier with the bucket's
summed diff — use `TimedUnifiedStream2.consolidateAtTime` in
`Mz/TimedConsolidate2.lean`. The two views are complementary: this
file collapses time slices to a single diff value; the
TimedConsolidate2 view collapses each `(row, time)` bucket
separately.
-/

namespace Mz

/-- Sum every diff in the stream, ignoring row and time. -/
def TimedUnifiedStream2.consolidateAll (s : TimedUnifiedStream2) : DiffWithGlobal :=
  DiffWithGlobal.sumAll (s.map (·.2.2))

/-- Sum every diff at a given time, ignoring row. -/
def TimedUnifiedStream2.consolidateAtTimeFlat
    (t : Nat) (s : TimedUnifiedStream2) : DiffWithGlobal :=
  DiffWithGlobal.sumAll ((s.filter (·.2.1 = t)).map (·.2.2))

/-! ## Absorption -/

/-- A `.global` diff anywhere in the stream forces the
collection-wide consolidation to `.global`. -/
theorem TimedUnifiedStream2.consolidateAll_eq_global_of_mem
    {s : TimedUnifiedStream2} (r : TimedUnifiedRecord2)
    (h_mem : r ∈ s) (h_g : r.2.2 = DiffWithGlobal.global) :
    TimedUnifiedStream2.consolidateAll s = DiffWithGlobal.global := by
  unfold TimedUnifiedStream2.consolidateAll
  apply DiffWithGlobal.sumAll_eq_global_of_mem
  refine List.mem_map.mpr ⟨r, h_mem, ?_⟩
  exact h_g

/-- Restricted to a time slice: a `.global` record at time `t`
forces the per-time flat consolidation at `t` to `.global`. -/
theorem TimedUnifiedStream2.consolidateAtTimeFlat_eq_global_of_mem
    {s : TimedUnifiedStream2} (t : Nat) (r : TimedUnifiedRecord2)
    (h_mem : r ∈ s) (h_time : r.2.1 = t)
    (h_g : r.2.2 = DiffWithGlobal.global) :
    TimedUnifiedStream2.consolidateAtTimeFlat t s = DiffWithGlobal.global := by
  unfold TimedUnifiedStream2.consolidateAtTimeFlat
  apply DiffWithGlobal.sumAll_eq_global_of_mem
  refine List.mem_map.mpr ⟨r, ?_, h_g⟩
  exact List.mem_filter.mpr ⟨h_mem, by simp [h_time]⟩

/-! ## Reverse direction: from `.global` total to `.global` record -/

/-- If the collection-wide consolidation is `.global`, at least one
record in the stream carries a `.global` diff. The converse of
`consolidateAll_eq_global_of_mem`. -/
theorem TimedUnifiedStream2.consolidateAll_global_inv
    {s : TimedUnifiedStream2}
    (h : TimedUnifiedStream2.consolidateAll s = DiffWithGlobal.global) :
    ∃ r ∈ s, r.2.2 = DiffWithGlobal.global := by
  unfold TimedUnifiedStream2.consolidateAll at h
  obtain ⟨d, hMem, hD⟩ := DiffWithGlobal.sumAll_global_inv h
  obtain ⟨r, hRMem, hRD⟩ := List.mem_map.mp hMem
  exact ⟨r, hRMem, by rw [hRD]; exact hD⟩

/-- Time-slice version: a `.global` total at time `t` witnesses a
`.global` record at time `t`. -/
theorem TimedUnifiedStream2.consolidateAtTimeFlat_global_inv
    {s : TimedUnifiedStream2} (t : Nat)
    (h : TimedUnifiedStream2.consolidateAtTimeFlat t s = DiffWithGlobal.global) :
    ∃ r ∈ s, r.2.1 = t ∧ r.2.2 = DiffWithGlobal.global := by
  unfold TimedUnifiedStream2.consolidateAtTimeFlat at h
  obtain ⟨d, hMem, hD⟩ := DiffWithGlobal.sumAll_global_inv h
  obtain ⟨r, hRMem, hRD⟩ := List.mem_map.mp hMem
  have hRFilter : r ∈ s.filter (·.2.1 = t) := hRMem
  rw [List.mem_filter] at hRFilter
  refine ⟨r, hRFilter.1, ?_, ?_⟩
  · exact of_decide_eq_true hRFilter.2
  · rw [hRD]; exact hD

/-! ## Round-trip iff forms

Combine forward absorption with reverse inversion. The flat
consolidations exactly characterize the presence of a `.global` diff
in the stream (per-time slice for the time-aware version). -/

theorem TimedUnifiedStream2.consolidateAll_eq_global_iff
    (s : TimedUnifiedStream2) :
    TimedUnifiedStream2.consolidateAll s = DiffWithGlobal.global
      ↔ ∃ r ∈ s, r.2.2 = DiffWithGlobal.global := by
  constructor
  · exact TimedUnifiedStream2.consolidateAll_global_inv
  · intro ⟨r, hMem, hG⟩
    exact TimedUnifiedStream2.consolidateAll_eq_global_of_mem r hMem hG

theorem TimedUnifiedStream2.consolidateAtTimeFlat_eq_global_iff
    (s : TimedUnifiedStream2) (t : Nat) :
    TimedUnifiedStream2.consolidateAtTimeFlat t s = DiffWithGlobal.global
      ↔ ∃ r ∈ s, r.2.1 = t ∧ r.2.2 = DiffWithGlobal.global := by
  constructor
  · exact TimedUnifiedStream2.consolidateAtTimeFlat_global_inv t
  · intro ⟨r, hMem, hT, hG⟩
    exact TimedUnifiedStream2.consolidateAtTimeFlat_eq_global_of_mem t r hMem hT hG

end Mz

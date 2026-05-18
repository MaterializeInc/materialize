import Mz.UnifiedStream
import Mz.UnifiedConsolidate
import Mz.DiffSemiring

/-!
# Per-`(row, time)` consolidation

`Mz/UnifiedConsolidate.lean` buckets records by carrier and sums
diffs per bucket. Differential dataflow buckets by the joint key
`(row, time)` instead. This file lifts the row-only consolidator
into the timed setting by isolating each time slice with
`atTime t` and running `UnifiedStream.consolidate` on the slice.

A `TimedUnifiedRecord` is a `(UnifiedRow, Nat, DiffWithError Int)`
triple — carrier, time, diff. `TimedUnifiedStream.atTime t` keeps
the records at time `t` and forgets the time component, producing
an ordinary `UnifiedStream`. Composing with `consolidate` gives
`consolidateAtTime`.

The headline theorem `consolidateAtTime_preserves_error` proves
that an `.error` diff at time `t` survives both the time-slice
filter and the per-row consolidation — the absorbing diff marker
propagates through the joint key. Cardinality follows from
`consolidate_length_le` plus the obvious bound on `atTime`.
-/

namespace Mz

/-- A timed record on the unified stream: carrier, time, diff. -/
abbrev TimedUnifiedRecord := UnifiedRow × Nat × DiffWithError Int

/-- Differential-dataflow-style stream of timed unified records. -/
abbrev TimedUnifiedStream := List TimedUnifiedRecord

/-- Project a timed stream to the time slice at `t`. Records at
other times are dropped; the time component is forgotten. -/
def TimedUnifiedStream.atTime (t : Nat) (s : TimedUnifiedStream) : UnifiedStream :=
  s.filterMap fun r =>
    if r.2.1 = t then some (r.1, r.2.2) else none

/-- Bucket records at time `t` by carrier and sum their diffs. -/
def TimedUnifiedStream.consolidateAtTime (t : Nat) (s : TimedUnifiedStream) :
    UnifiedStream :=
  UnifiedStream.consolidate (TimedUnifiedStream.atTime t s)

/-! ## Trivial cases -/

theorem TimedUnifiedStream.atTime_nil (t : Nat) :
    TimedUnifiedStream.atTime t [] = [] := rfl

theorem TimedUnifiedStream.consolidateAtTime_nil (t : Nat) :
    TimedUnifiedStream.consolidateAtTime t [] = [] := rfl

/-! ## Time-slice extraction -/

/-- A record present at time `t` shows up in the time slice
`atTime t` with its carrier and diff. -/
theorem TimedUnifiedStream.mem_atTime_of_mem
    {t : Nat} {s : TimedUnifiedStream}
    {uc : UnifiedRow} {d : DiffWithError Int}
    (h_mem : (uc, t, d) ∈ s) :
    (uc, d) ∈ TimedUnifiedStream.atTime t s := by
  induction s with
  | nil => exact absurd h_mem List.not_mem_nil
  | cons hd tl ih =>
    rcases List.mem_cons.mp h_mem with hHead | hTail
    · subst hHead
      show (uc, d) ∈ ((uc, t, d) :: tl).filterMap fun r =>
            if r.2.1 = t then some (r.1, r.2.2) else none
      simp
    · have ihMem := ih hTail
      show (uc, d) ∈ (hd :: tl).filterMap fun r =>
            if r.2.1 = t then some (r.1, r.2.2) else none
      rw [List.filterMap_cons]
      cases hCond : (if hd.2.1 = t then some (hd.1, hd.2.2) else (none : Option _))
      case none => exact ihMem
      case some hdSlice => exact List.mem_cons_of_mem _ ihMem

/-! ## `.error` absorption -/

/-- An `.error` diff at time `t` survives the per-`(row, time)`
consolidation: the consolidated output at time `t` carries the
carrier with `.error` diff. -/
theorem TimedUnifiedStream.consolidateAtTime_preserves_error
    (t : Nat) (s : TimedUnifiedStream) (uc : UnifiedRow)
    (h_mem : (uc, t, (DiffWithError.error : DiffWithError Int)) ∈ s) :
    (uc, (DiffWithError.error : DiffWithError Int))
      ∈ TimedUnifiedStream.consolidateAtTime t s := by
  have hSlice :
      (uc, (DiffWithError.error : DiffWithError Int))
        ∈ TimedUnifiedStream.atTime t s :=
    TimedUnifiedStream.mem_atTime_of_mem h_mem
  exact UnifiedStream.consolidate_preserves_error _ uc hSlice

/-! ## Cardinality -/

/-- `atTime` is non-expanding: each input record contributes at
most one output record (it is either kept with its time stripped
or dropped). -/
theorem TimedUnifiedStream.atTime_length_le (t : Nat) (s : TimedUnifiedStream) :
    (TimedUnifiedStream.atTime t s).length ≤ s.length := by
  unfold TimedUnifiedStream.atTime
  induction s with
  | nil => exact Nat.le.refl
  | cons hd tl ih =>
    rw [List.filterMap_cons, List.length_cons]
    by_cases hT : hd.2.1 = t
    · rw [if_pos hT, List.length_cons]
      exact Nat.add_le_add_right ih 1
    · rw [if_neg hT]
      exact Nat.le_trans ih (Nat.le_succ _)

/-- Cardinality of the per-time consolidation, chained from
`atTime_length_le` and `consolidate_length_le`. -/
theorem TimedUnifiedStream.consolidateAtTime_length_le
    (t : Nat) (s : TimedUnifiedStream) :
    (TimedUnifiedStream.consolidateAtTime t s).length ≤ s.length := by
  unfold TimedUnifiedStream.consolidateAtTime
  exact Nat.le_trans
    (UnifiedStream.consolidate_length_le _)
    (TimedUnifiedStream.atTime_length_le t s)

end Mz

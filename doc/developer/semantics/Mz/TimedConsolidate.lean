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

/-! ## Frontier advance

Differential dataflow's `advance` operator: records with time
strictly before frontier `f` are "advanced" to `f` (their time
is updated to `f`), making the past immutable. Records at or past
`f` are left untouched.

The skeleton models frontiers as a single `Nat`. The real
framework uses antichains of times for partial-order timestamps;
the scalar form is sufficient to state the algebraic laws. -/

/-- Advance every record's time to at least `f`. Records originally
at time `< f` move to `f`; records already at `≥ f` stay. -/
def TimedUnifiedStream.advanceFrontier (f : Nat) (s : TimedUnifiedStream) :
    TimedUnifiedStream :=
  s.map fun r => (r.1, Nat.max r.2.1 f, r.2.2)

theorem TimedUnifiedStream.advanceFrontier_nil (f : Nat) :
    TimedUnifiedStream.advanceFrontier f [] = [] := rfl

theorem TimedUnifiedStream.advanceFrontier_length
    (f : Nat) (s : TimedUnifiedStream) :
    (TimedUnifiedStream.advanceFrontier f s).length = s.length :=
  List.length_map _

/-- Advancing by `0` is the identity (no times below the frontier). -/
theorem TimedUnifiedStream.advanceFrontier_zero (s : TimedUnifiedStream) :
    TimedUnifiedStream.advanceFrontier 0 s = s := by
  induction s with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨uc, t, d⟩ := hd
    show (uc, Nat.max t 0, d) :: TimedUnifiedStream.advanceFrontier 0 tl
        = (uc, t, d) :: tl
    have hMax : Nat.max t 0 = t := Nat.max_eq_left (Nat.zero_le t)
    rw [hMax, ih]

/-- Idempotence: advancing twice by the same frontier equals
advancing once. After the first pass, every record has time `≥ f`,
so the second `Nat.max _ f` is a no-op. -/
theorem TimedUnifiedStream.advanceFrontier_idem
    (f : Nat) (s : TimedUnifiedStream) :
    TimedUnifiedStream.advanceFrontier f
        (TimedUnifiedStream.advanceFrontier f s)
      = TimedUnifiedStream.advanceFrontier f s := by
  induction s with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨uc, t, d⟩ := hd
    show (uc, Nat.max (Nat.max t f) f, d)
            :: TimedUnifiedStream.advanceFrontier f
                (TimedUnifiedStream.advanceFrontier f tl)
        = (uc, Nat.max t f, d) :: TimedUnifiedStream.advanceFrontier f tl
    have h_max : Nat.max (Nat.max t f) f = Nat.max t f := by
      cases Nat.le_total t f with
      | inl h_le =>
        have h1 : Nat.max t f = f := Nat.max_eq_right h_le
        have h2 : Nat.max f f = f := Nat.max_eq_left (Nat.le_refl _)
        rw [h1, h2]
      | inr h_ge =>
        have h1 : Nat.max t f = t := Nat.max_eq_left h_ge
        rw [h1]; exact h1
    rw [h_max, ih]

/-- Monotone composition: advancing by `f` then `g` equals
advancing by `Nat.max f g`. The max is associative and
commutative on `Nat`, so the final frontier dominates. -/
theorem TimedUnifiedStream.advanceFrontier_advanceFrontier
    (f g : Nat) (s : TimedUnifiedStream) :
    TimedUnifiedStream.advanceFrontier g
        (TimedUnifiedStream.advanceFrontier f s)
      = TimedUnifiedStream.advanceFrontier (Nat.max f g) s := by
  induction s with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨uc, t, d⟩ := hd
    show (uc, Nat.max (Nat.max t f) g, d)
            :: TimedUnifiedStream.advanceFrontier g
                (TimedUnifiedStream.advanceFrontier f tl)
        = (uc, Nat.max t (Nat.max f g), d)
            :: TimedUnifiedStream.advanceFrontier (Nat.max f g) tl
    have hAssoc : Nat.max (Nat.max t f) g = Nat.max t (Nat.max f g) :=
      Nat.max_assoc t f g
    rw [hAssoc, ih]

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

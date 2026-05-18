import Mz.UnifiedStream
import Mz.DiffSemiring

/-!
# Row-keyed consolidation on `UnifiedStream`

`compact` in differential dataflow buckets records that share the
same `(row, time)` key and sums their diffs. The diff-only slice
of that operation already lives in `Mz/Consolidate.lean`; this
file lifts the bucketing to `UnifiedStream` so a consolidated
output is itself a `UnifiedStream` (with one record per distinct
carrier and a summed diff).

The headline property is preservation of the `.error` diff
marker. If any input record carries `.error`, the consolidated
output for that carrier carries `.error`. The semiring's
absorption law (`.error + x = .error`) does the work — the
bucket sum collapses to `.error` the moment one `.error` diff
joins it.

The skeleton skips the time dimension; per-`(row, time)`
bucketing reduces to per-row consolidation inside each time
slice, so adding times later is mechanical.
-/

namespace Mz

/-- Insert `(uc, d)` into a consolidated stream. If a record with
the same carrier already exists, add `d` to its diff. Otherwise
append a new record at the end of the list. -/
private def consolidateInto (uc : UnifiedRow) (d : DiffWithError Int) :
    UnifiedStream → UnifiedStream
  | []                  => [(uc, d)]
  | (uc', d') :: rest =>
    if uc = uc' then (uc', d + d') :: rest
    else (uc', d') :: consolidateInto uc d rest

/-- Sum diffs per carrier across the stream. Order of distinct
carriers is unspecified beyond "encounter order from the right";
order of `consolidate` matters only to the extent that diff
addition is non-commutative on the base, which it is not for
`Int`. -/
def UnifiedStream.consolidate : UnifiedStream → UnifiedStream
  | []              => []
  | (uc, d) :: rest =>
    consolidateInto uc d (UnifiedStream.consolidate rest)

/-! ## Trivial cases -/

theorem UnifiedStream.consolidate_nil :
    UnifiedStream.consolidate [] = [] := rfl

theorem UnifiedStream.consolidate_singleton (uc : UnifiedRow) (d : DiffWithError Int) :
    UnifiedStream.consolidate [(uc, d)] = [(uc, d)] := rfl

/-! ## `.error` absorption -/

/-- Inserting an `.error` diff into any consolidated stream yields
an output containing the carrier with diff `.error`. Either the
carrier was already in the stream (bucket diff becomes
`.error + d_old = .error`) or it was not (fresh bucket appended). -/
private theorem consolidateInto_error_diff
    (uc : UnifiedRow) (us : UnifiedStream) :
    (uc, (DiffWithError.error : DiffWithError Int))
      ∈ consolidateInto uc DiffWithError.error us := by
  induction us with
  | nil => exact List.mem_singleton.mpr rfl
  | cons hd tl ih =>
    obtain ⟨uc', d'⟩ := hd
    by_cases hEq : uc = uc'
    · subst hEq
      show (uc, DiffWithError.error)
        ∈ (if uc = uc then (uc, DiffWithError.error + d') :: tl
            else (uc, d') :: consolidateInto uc DiffWithError.error tl)
      rw [if_pos rfl]
      rw [DiffWithError.error_add_left]
      exact List.mem_cons_self
    · show (uc, DiffWithError.error)
        ∈ (if uc = uc' then (uc', DiffWithError.error + d') :: tl
            else (uc', d') :: consolidateInto uc DiffWithError.error tl)
      rw [if_neg hEq]
      exact List.mem_cons_of_mem _ ih

/-- Inserting any record into a consolidated stream that already
contains `(uc, .error)` leaves the `.error` record in place. If
the inserted key matches `uc`, the bucket diff becomes
`d_new + .error = .error`; otherwise the `.error` record is in
the recursive tail. -/
private theorem consolidateInto_preserves_error_mem
    (uc' : UnifiedRow) (d' : DiffWithError Int) (us : UnifiedStream)
    (uc : UnifiedRow)
    (h_mem : (uc, (DiffWithError.error : DiffWithError Int)) ∈ us) :
    (uc, (DiffWithError.error : DiffWithError Int))
      ∈ consolidateInto uc' d' us := by
  induction us with
  | nil => exact absurd h_mem (List.not_mem_nil)
  | cons hd tl ih =>
    obtain ⟨uc₀, d₀⟩ := hd
    rcases List.mem_cons.mp h_mem with hEq | hTail
    · -- (uc, .error) = (uc₀, d₀)
      have hUc : uc = uc₀ := (Prod.mk.injEq _ _ _ _).mp hEq |>.1
      have hD  : (DiffWithError.error : DiffWithError Int) = d₀ :=
        (Prod.mk.injEq _ _ _ _).mp hEq |>.2
      subst hUc
      subst hD
      by_cases hEq' : uc' = uc
      · show (uc, DiffWithError.error)
          ∈ (if uc' = uc then (uc, d' + DiffWithError.error) :: tl
              else (uc, DiffWithError.error) :: consolidateInto uc' d' tl)
        rw [if_pos hEq']
        rw [DiffWithError.error_add_right]
        exact List.mem_cons_self
      · show (uc, DiffWithError.error)
          ∈ (if uc' = uc then (uc, d' + DiffWithError.error) :: tl
              else (uc, DiffWithError.error) :: consolidateInto uc' d' tl)
        rw [if_neg hEq']
        exact List.mem_cons_self
    · by_cases hEq' : uc' = uc₀
      · show (uc, DiffWithError.error)
          ∈ (if uc' = uc₀ then (uc₀, d' + d₀) :: tl
              else (uc₀, d₀) :: consolidateInto uc' d' tl)
        rw [if_pos hEq']
        exact List.mem_cons_of_mem _ hTail
      · show (uc, DiffWithError.error)
          ∈ (if uc' = uc₀ then (uc₀, d' + d₀) :: tl
              else (uc₀, d₀) :: consolidateInto uc' d' tl)
        rw [if_neg hEq']
        exact List.mem_cons_of_mem _ (ih hTail)

/-- Headline absorption: an `.error` diff anywhere in the input
survives the row-keyed consolidation. -/
theorem UnifiedStream.consolidate_preserves_error
    (us : UnifiedStream) (uc : UnifiedRow)
    (h_mem : (uc, (DiffWithError.error : DiffWithError Int)) ∈ us) :
    (uc, (DiffWithError.error : DiffWithError Int))
      ∈ UnifiedStream.consolidate us := by
  induction us with
  | nil => exact absurd h_mem (List.not_mem_nil)
  | cons hd tl ih =>
    obtain ⟨uc₀, d₀⟩ := hd
    rcases List.mem_cons.mp h_mem with hEq | hTail
    · have hUc : uc = uc₀ := (Prod.mk.injEq _ _ _ _).mp hEq |>.1
      have hD  : (DiffWithError.error : DiffWithError Int) = d₀ :=
        (Prod.mk.injEq _ _ _ _).mp hEq |>.2
      subst hUc
      subst hD
      show (uc, DiffWithError.error)
        ∈ consolidateInto uc DiffWithError.error (UnifiedStream.consolidate tl)
      exact consolidateInto_error_diff uc _
    · show (uc, DiffWithError.error)
        ∈ consolidateInto uc₀ d₀ (UnifiedStream.consolidate tl)
      exact consolidateInto_preserves_error_mem uc₀ d₀ _ uc (ih hTail)

end Mz

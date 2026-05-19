import Mz.DiffSemiring

/-!
# Consolidation

Differential dataflow's `compact` operation sums the diffs of
records that share the same `(row, time)` key. The skeleton models
the diff-only slice of that operation: summing a list of
`DiffWithError α` values. The interesting property is that an
`error` diff in the list absorbs the whole sum to `error`,
regardless of what the other diffs are.

This is the algebraic statement an actual `compact` operator would
cite when arguing that global errors propagate through
consolidation: as soon as any record's diff is `error`, the
consolidated diff for that key is `error`, so downstream operators
treat the key's contribution as invalid.

The full operator would also bucket records by `(row, time)`,
which requires `DecidableEq` on `Row` and a notion of time. That
bucketing is orthogonal to the absorption argument; the skeleton
gives the single-bucket version, which is the per-key inner sum.
-/

namespace Mz

namespace DiffWithError

variable {α : Type}

/-- Sum a list of diffs, starting from `0`. The fold is right-
associative; with `+` being commutative on the base type, the
result is order-independent. -/
def sumAll [Zero α] [Add α] : List (DiffWithError α) → DiffWithError α
  | []        => 0
  | d :: rest => d + sumAll rest

/-! ## Absorption -/

/-- If `error` appears anywhere in the diff list, the consolidated
sum is `error`. The proof walks the list and uses the absorption
laws from `Mz/DiffSemiring.lean` at the matching cons. -/
theorem sumAll_eq_error_of_mem [Zero α] [Add α]
    {ds : List (DiffWithError α)} (h : (error : DiffWithError α) ∈ ds) :
    sumAll ds = error := by
  induction ds with
  | nil => cases h
  | cons hd tl ih =>
    cases h with
    | head _ =>
      -- hd = error; first cons step is `error + sumAll tl`, which is `error`.
      show (error : DiffWithError α) + sumAll tl = error
      exact error_add_left _
    | tail _ h_tl =>
      -- error is in tl. By IH `sumAll tl = error`, then `hd + error = error`.
      show hd + sumAll tl = error
      rw [ih h_tl]
      exact error_add_right hd

/-! ## No-error preservation

If every diff in the list is an honest `val x`, the consolidated
sum is also `val` of *some* `α`. The exact value depends on the
base addition, which the skeleton does not commit to here. -/

theorem sumAll_val_of_all_val [Zero α] [Add α]
    {ds : List (DiffWithError α)}
    (h : ∀ d ∈ ds, ∃ x : α, d = val x) :
    ∃ x : α, sumAll ds = val x := by
  induction ds with
  | nil =>
    show ∃ x : α, (0 : DiffWithError α) = val x
    refine ⟨0, ?_⟩
    rfl
  | cons hd tl ih =>
    obtain ⟨xh, hh_eq⟩ := h hd (List.Mem.head _)
    have htl : ∀ d ∈ tl, ∃ x : α, d = val x :=
      fun d hd_mem => h d (List.Mem.tail _ hd_mem)
    obtain ⟨xt, ht_eq⟩ := ih htl
    refine ⟨xh + xt, ?_⟩
    show hd + sumAll tl = val (xh + xt)
    rw [hh_eq, ht_eq]
    rfl

/-- Reverse direction: a `sumAll` equal to `.error` witnesses an
`.error` summand somewhere in the list. The `.val + .val` arm of
`+` never returns `.error`, so an `.error` total rules in at
least one `.error` input. -/
theorem sumAll_error_inv [Zero α] [Add α]
    {ds : List (DiffWithError α)}
    (h : sumAll ds = error) :
    ∃ d ∈ ds, d = error := by
  induction ds with
  | nil =>
    -- sumAll [] = 0 = val 0 ≠ error
    show ∃ d ∈ ([] : List (DiffWithError α)), d = error
    exfalso
    have h0 : (0 : DiffWithError α) = (error : DiffWithError α) := h
    have : (DiffWithError.val (0 : α) : DiffWithError α)
         = (DiffWithError.error : DiffWithError α) := h0
    cases this
  | cons hd tl ih =>
    show ∃ d ∈ hd :: tl, d = error
    have hSum : hd + sumAll tl = error := h
    rcases add_eq_error_left_or_right hd (sumAll tl) hSum with hHd | hTl
    · exact ⟨hd, List.mem_cons_self, hHd⟩
    · obtain ⟨d, hMem, hD⟩ := ih hTl
      exact ⟨d, List.mem_cons_of_mem _ hMem, hD⟩

end DiffWithError

end Mz

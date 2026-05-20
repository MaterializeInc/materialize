import Mz.DiffWithGlobal

/-!
# Consolidation under the two-layer diff

`Mz/Consolidate.lean` consolidates a list of `DiffWithError α`
values, where a single `error` absorbs the entire sum. That model
conflates row-scoped errors (retractable, kept in `Diff.errs`) with
collection-scoped errors (terminal, encoded as
`DiffWithGlobal.global`). See
`doc/developer/design/20260517_error_handling_semantics.md`, section
"Global-scoped errors: absorbing diff marker".

This file mirrors `Mz/Consolidate.lean` for the new
`DiffWithGlobal` carrier: the absorbing element is now
`DiffWithGlobal.global`, and the row-scoped err counts inside
`Diff` are summed pointwise rather than absorbing. The absorption
proofs cite `DiffWithGlobal.global_add_left/right` and
`add_eq_global_left_or_right` directly, the same way the old file
cited the corresponding `DiffWithError` laws.

The honest sum / converse-absorption pair argues that downstream
operators may treat a `global` consolidated diff as proof that some
input record contributed `global`, and conversely that an all-`val`
input list stays in the `val` branch — i.e. row-scoped err counts
alone never escalate consolidation to `global`.
-/

namespace Mz

namespace DiffWithGlobal

/-- Sum a list of diffs, right-associative fold from `0`. Mirrors
`DiffWithError.sumAll`; with `+` commutative on `DiffWithGlobal`
(via `DiffWithGlobal.add_comm`), the result is order-independent. -/
def sumAll : List DiffWithGlobal → DiffWithGlobal
  | []        => 0
  | d :: rest => d + sumAll rest

/-! ## Absorption -/

/-- If `global` appears anywhere in the diff list, the consolidated
sum is `global`. The proof walks the list and uses the absorption
laws from `Mz/DiffWithGlobal.lean` at the matching cons. -/
theorem sumAll_eq_global_of_mem
    {ds : List DiffWithGlobal} (h : DiffWithGlobal.global ∈ ds) :
    sumAll ds = DiffWithGlobal.global := by
  induction ds with
  | nil => cases h
  | cons hd tl ih =>
    cases h with
    | head _ =>
      -- hd = global; first cons step is `global + sumAll tl`, which is `global`.
      show (global : DiffWithGlobal) + sumAll tl = global
      exact global_add_left _
    | tail _ h_tl =>
      -- global is in tl. By IH `sumAll tl = global`, then `hd + global = global`.
      show hd + sumAll tl = global
      rw [ih h_tl]
      exact global_add_right hd

/-! ## No-global preservation

If every diff in the list is an honest `val d`, the consolidated
sum is also `val` of *some* `Diff`. The exact value depends on
`Diff`'s addition, which this file does not commit to here. -/

theorem sumAll_val_of_all_val
    {ds : List DiffWithGlobal}
    (h : ∀ d ∈ ds, ∃ x : Diff, d = val x) :
    ∃ x : Diff, sumAll ds = val x := by
  induction ds with
  | nil =>
    show ∃ x : Diff, (0 : DiffWithGlobal) = val x
    refine ⟨0, ?_⟩
    rfl
  | cons hd tl ih =>
    obtain ⟨xh, hh_eq⟩ := h hd (List.Mem.head _)
    have htl : ∀ d ∈ tl, ∃ x : Diff, d = val x :=
      fun d hd_mem => h d (List.Mem.tail _ hd_mem)
    obtain ⟨xt, ht_eq⟩ := ih htl
    refine ⟨xh + xt, ?_⟩
    show hd + sumAll tl = val (xh + xt)
    rw [hh_eq, ht_eq]
    rfl

/-- Reverse direction: a `sumAll` equal to `.global` witnesses a
`.global` summand somewhere in the list. The `.val + .val` arm of
`+` never returns `.global`, so a `.global` total rules in at
least one `.global` input. -/
theorem sumAll_global_inv
    {ds : List DiffWithGlobal}
    (h : sumAll ds = DiffWithGlobal.global) :
    ∃ d ∈ ds, d = DiffWithGlobal.global := by
  induction ds with
  | nil =>
    -- sumAll [] = 0 = val 0 ≠ global
    show ∃ d ∈ ([] : List DiffWithGlobal), d = global
    exfalso
    have h0 : (0 : DiffWithGlobal) = (global : DiffWithGlobal) := h
    have : (DiffWithGlobal.val (0 : Diff) : DiffWithGlobal)
         = (DiffWithGlobal.global : DiffWithGlobal) := h0
    cases this
  | cons hd tl ih =>
    show ∃ d ∈ hd :: tl, d = global
    have hSum : hd + sumAll tl = global := h
    rcases add_eq_global_left_or_right hd (sumAll tl) hSum with hHd | hTl
    · exact ⟨hd, List.mem_cons_self, hHd⟩
    · obtain ⟨d, hMem, hD⟩ := ih hTl
      exact ⟨d, List.mem_cons_of_mem _ hMem, hD⟩

end DiffWithGlobal

end Mz

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
append a new record at the end of the list. Exposed (not
`private`) so downstream files can state laws about it. -/
def consolidateInto (uc : UnifiedRow) (d : DiffWithError Int) :
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

/-! ### `consolidateInto` reduction lemmas

Named per-shape reductions so proofs cite a single lemma instead
of unfolding the `if`-then-else by hand. -/

theorem consolidateInto_nil (uc : UnifiedRow) (d : DiffWithError Int) :
    consolidateInto uc d [] = [(uc, d)] := rfl

/-- Inserting `(uc, d)` at the head of a list whose head matches
`uc` folds into the head bucket. -/
theorem consolidateInto_match
    (uc : UnifiedRow) (d d' : DiffWithError Int) (tl : UnifiedStream) :
    consolidateInto uc d ((uc, d') :: tl) = (uc, d + d') :: tl := by
  show (if uc = uc then (uc, d + d') :: tl
          else (uc, d') :: consolidateInto uc d tl)
      = (uc, d + d') :: tl
  rw [if_pos rfl]

/-- Inserting `(uc, d)` at the head of a list whose head does not
match `uc` skips the head and recurses on the tail. -/
theorem consolidateInto_skip
    (uc uc' : UnifiedRow) (d d' : DiffWithError Int) (tl : UnifiedStream)
    (h : uc ≠ uc') :
    consolidateInto uc d ((uc', d') :: tl)
      = (uc', d') :: consolidateInto uc d tl := by
  show (if uc = uc' then (uc', d + d') :: tl
          else (uc', d') :: consolidateInto uc d tl)
      = (uc', d') :: consolidateInto uc d tl
  rw [if_neg h]

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
      rw [consolidateInto_match]
      rw [DiffWithError.error_add_left]
      exact List.mem_cons_self
    · rw [consolidateInto_skip _ _ _ _ _ hEq]
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
      · subst hEq'
        rw [consolidateInto_match]
        rw [DiffWithError.error_add_right]
        exact List.mem_cons_self
      · rw [consolidateInto_skip _ _ _ _ _ hEq']
        exact List.mem_cons_self
    · by_cases hEq' : uc' = uc₀
      · subst hEq'
        rw [consolidateInto_match]
        exact List.mem_cons_of_mem _ hTail
      · rw [consolidateInto_skip _ _ _ _ _ hEq']
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

/-! ## Cardinality

Consolidation is non-expanding. Every input record either lands
in an existing bucket (no length change) or starts a new one
(length grows by one), so the output length is at most the input
length. The strict inequality holds when at least one carrier
appears more than once, which the skeleton does not state
separately. -/

/-- `consolidateInto` adds at most one record to the bucket list:
either it appends a fresh bucket (length + 1) or it lands inside
an existing bucket (length unchanged). -/
private theorem consolidateInto_length_le_succ
    (uc : UnifiedRow) (d : DiffWithError Int) (us : UnifiedStream) :
    (consolidateInto uc d us).length ≤ us.length + 1 := by
  induction us with
  | nil => exact Nat.le.refl
  | cons hd tl ih =>
    obtain ⟨uc', d'⟩ := hd
    by_cases hEq : uc = uc'
    · subst hEq
      rw [consolidateInto_match]
      simp [List.length_cons]
    · rw [consolidateInto_skip _ _ _ _ _ hEq]
      show (consolidateInto uc d tl).length + 1 ≤ tl.length + 1 + 1
      omega

/-- Output of `consolidate` has length at most the input length. -/
theorem UnifiedStream.consolidate_length_le (us : UnifiedStream) :
    (UnifiedStream.consolidate us).length ≤ us.length := by
  induction us with
  | nil => exact Nat.le.refl
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    show (consolidateInto uc d (UnifiedStream.consolidate tl)).length
        ≤ ((uc, d) :: tl).length
    have hStep := consolidateInto_length_le_succ uc d (UnifiedStream.consolidate tl)
    have hIh : (UnifiedStream.consolidate tl).length + 1 ≤ tl.length + 1 :=
      Nat.add_le_add_right ih 1
    show (consolidateInto uc d (UnifiedStream.consolidate tl)).length ≤ tl.length + 1
    exact Nat.le_trans hStep hIh

/-! ## Strict cardinality

When a carrier already appears in the consolidated list,
inserting it again does not grow the list — the existing bucket
absorbs the new diff. The headline `consolidate_strict_length_dup`
shows two adjacent records sharing a carrier compress to one in
the output. -/

/-- After `consolidateInto`, the carrier `uc` is in the result.
Either the input already had it (the bucket update preserves
membership) or the input did not (a fresh bucket is appended). -/
private theorem mem_after_consolidateInto
    (uc : UnifiedRow) (d : DiffWithError Int) (us : UnifiedStream) :
    ∃ d', (uc, d') ∈ consolidateInto uc d us := by
  induction us with
  | nil => exact ⟨d, List.mem_singleton.mpr rfl⟩
  | cons hd tl ih =>
    obtain ⟨uc', d'⟩ := hd
    by_cases hEq : uc = uc'
    · subst hEq
      refine ⟨d + d', ?_⟩
      rw [consolidateInto_match]
      exact List.mem_cons_self
    · obtain ⟨d'', hMem⟩ := ih
      refine ⟨d'', ?_⟩
      rw [consolidateInto_skip _ _ _ _ _ hEq]
      exact List.mem_cons_of_mem _ hMem

/-- When `uc` already appears in `us`, `consolidateInto uc d us`
does not change the length — the bucket update is in place. -/
private theorem consolidateInto_length_eq_of_mem
    (uc : UnifiedRow) (d : DiffWithError Int) (us : UnifiedStream)
    (h : ∃ d', (uc, d') ∈ us) :
    (consolidateInto uc d us).length = us.length := by
  induction us with
  | nil => obtain ⟨_, hMem⟩ := h; exact absurd hMem List.not_mem_nil
  | cons hd tl ih =>
    obtain ⟨uc', d'⟩ := hd
    by_cases hEq : uc = uc'
    · subst hEq
      rw [consolidateInto_match]
      rfl
    · have hMemTl : ∃ d', (uc, d') ∈ tl := by
        obtain ⟨d'', hMem⟩ := h
        rcases List.mem_cons.mp hMem with hHead | hTail
        · exact absurd ((Prod.mk.injEq _ _ _ _).mp hHead).1 hEq
        · exact ⟨d'', hTail⟩
      rw [consolidateInto_skip _ _ _ _ _ hEq]
      show (consolidateInto uc d tl).length + 1 = tl.length + 1
      rw [ih hMemTl]

/-- Two adjacent records sharing a carrier collapse to one in the
consolidated output. The output length is at most `rest.length + 1`,
strictly less than the input's `rest.length + 2`. -/
theorem UnifiedStream.consolidate_strict_length_dup
    (uc : UnifiedRow) (d d' : DiffWithError Int) (rest : UnifiedStream) :
    (UnifiedStream.consolidate ((uc, d) :: (uc, d') :: rest)).length
      ≤ rest.length + 1 := by
  -- consolidate ((uc, d) :: (uc, d') :: rest)
  --   = consolidateInto uc d (consolidate ((uc, d') :: rest))
  --   = consolidateInto uc d (consolidateInto uc d' (consolidate rest))
  -- The inner consolidateInto produces a list containing uc;
  -- the outer therefore preserves length.
  show (consolidateInto uc d (UnifiedStream.consolidate ((uc, d') :: rest))).length
        ≤ rest.length + 1
  have hMem : ∃ d'', (uc, d'') ∈ UnifiedStream.consolidate ((uc, d') :: rest) := by
    show ∃ d'', (uc, d'') ∈ consolidateInto uc d' (UnifiedStream.consolidate rest)
    exact mem_after_consolidateInto uc d' _
  rw [consolidateInto_length_eq_of_mem uc d _ hMem]
  -- Now bound (consolidate ((uc, d') :: rest)).length ≤ ((uc, d') :: rest).length
  --                                                    = rest.length + 1.
  have := UnifiedStream.consolidate_length_le ((uc, d') :: rest)
  show (UnifiedStream.consolidate ((uc, d') :: rest)).length ≤ rest.length + 1
  exact this

/-! ## No-error preservation

If every input diff is a `.val`, every output diff is a `.val`.
The semiring's `.val + .val = .val (· + ·)` keeps absorption from
firing. -/

/-- Inserting a `.val` diff into a list whose every record has
`.val` diff yields a list whose every record has `.val` diff. -/
private theorem consolidateInto_no_error
    (uc : UnifiedRow) (n : Int) (us : UnifiedStream)
    (h : ∀ r ∈ us, ∃ m : Int, r.2 = DiffWithError.val m) :
    ∀ r ∈ consolidateInto uc (DiffWithError.val n) us,
      ∃ m : Int, r.2 = DiffWithError.val m := by
  induction us with
  | nil =>
    intro r hMem
    have : r = (uc, DiffWithError.val n) := List.mem_singleton.mp hMem
    exact ⟨n, by rw [this]⟩
  | cons hd tl ih =>
    obtain ⟨uc', d'⟩ := hd
    have hHd : ∃ m : Int, d' = DiffWithError.val m := by
      have := h (uc', d') (List.mem_cons_self)
      exact this
    have hTl : ∀ r ∈ tl, ∃ m : Int, r.2 = DiffWithError.val m :=
      fun r hMem => h r (List.mem_cons_of_mem _ hMem)
    obtain ⟨m, hM⟩ := hHd
    intro r hMem
    by_cases hEq : uc = uc'
    · subst hEq
      rw [consolidateInto_match] at hMem
      rcases List.mem_cons.mp hMem with hHead | hTail'
      · subst hHead
        rw [hM]
        show ∃ m' : Int, DiffWithError.val n + DiffWithError.val m
                       = DiffWithError.val m'
        exact ⟨n + m, rfl⟩
      · exact hTl r hTail'
    · rw [consolidateInto_skip _ _ _ _ _ hEq] at hMem
      rcases List.mem_cons.mp hMem with hHead | hTail'
      · subst hHead
        exact ⟨m, hM⟩
      · exact ih hTl r hTail'

/-- Headline no-error: if every input diff is `.val`, every
output diff is `.val`. The consolidated total stays in the
ordinary `Int` slice of the diff-semiring. -/
theorem UnifiedStream.consolidate_no_error
    (us : UnifiedStream)
    (h : ∀ r ∈ us, ∃ n : Int, r.2 = DiffWithError.val n) :
    ∀ r ∈ UnifiedStream.consolidate us,
      ∃ n : Int, r.2 = DiffWithError.val n := by
  induction us with
  | nil => intro r hMem; exact absurd hMem (List.not_mem_nil)
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    have hHd : ∃ n : Int, d = DiffWithError.val n :=
      h (uc, d) List.mem_cons_self
    have hTl : ∀ r ∈ tl, ∃ n : Int, r.2 = DiffWithError.val n :=
      fun r hMem => h r (List.mem_cons_of_mem _ hMem)
    obtain ⟨n, hN⟩ := hHd
    have hConsTl : ∀ r ∈ UnifiedStream.consolidate tl,
                     ∃ n : Int, r.2 = DiffWithError.val n :=
      ih hTl
    intro r hMem
    have : r ∈ consolidateInto uc d (UnifiedStream.consolidate tl) := hMem
    rw [hN] at this
    exact consolidateInto_no_error uc n _ hConsTl r this

end Mz

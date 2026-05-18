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

/-! ## Carrier uniqueness

`consolidate` collapses duplicate carriers into single buckets, so
its output has each carrier at most once. Formalized as
`NoDupCarriers`: a `Pairwise` predicate stating that any two
records have distinct first components.

`lookup_eq_of_mem_noDup` then connects uniqueness to `lookup`:
when carriers are unique, `lookup uc us` returns the exact diff
of `(uc, d) ∈ us` (not some other entry's diff). -/

/-- Each carrier appears at most once in the list. -/
def UnifiedStream.NoDupCarriers (us : UnifiedStream) : Prop :=
  us.Pairwise (fun a b => a.1 ≠ b.1)

theorem UnifiedStream.NoDupCarriers.nil :
    UnifiedStream.NoDupCarriers [] := List.Pairwise.nil

/-- The carriers appearing in `consolidateInto uc d us` are
contained in `{uc} ∪ carriers(us)`. Used by
`consolidateInto_preserves_noDup` to argue the new head's
distinctness from the recursive tail. -/
private theorem mem_consolidateInto_carrier
    (uc : UnifiedRow) (d : DiffWithError Int) (us : UnifiedStream)
    (x : UnifiedRow × DiffWithError Int)
    (h : x ∈ consolidateInto uc d us) :
    x.1 = uc ∨ ∃ d'', (x.1, d'') ∈ us := by
  obtain ⟨xu, xd⟩ := x
  induction us with
  | nil =>
    have hEq : (xu, xd) = (uc, d) := List.mem_singleton.mp h
    have hUc : xu = uc := (Prod.mk.injEq _ _ _ _).mp hEq |>.1
    exact Or.inl hUc
  | cons hd tl ih =>
    obtain ⟨uc', d'⟩ := hd
    by_cases hEq : uc = uc'
    · subst hEq
      rw [consolidateInto_match] at h
      rcases List.mem_cons.mp h with hHead | hTail
      · refine Or.inr ⟨d', ?_⟩
        have : xu = uc := (Prod.mk.injEq _ _ _ _).mp hHead |>.1
        rw [this]
        exact List.mem_cons_self
      · exact Or.inr ⟨xd, List.mem_cons_of_mem _ hTail⟩
    · rw [consolidateInto_skip _ _ _ _ _ hEq] at h
      rcases List.mem_cons.mp h with hHead | hTail
      · refine Or.inr ⟨d', ?_⟩
        have : xu = uc' := (Prod.mk.injEq _ _ _ _).mp hHead |>.1
        rw [this]
        exact List.mem_cons_self
      · rcases ih hTail with hIs_uc | ⟨d'', hMem⟩
        · exact Or.inl hIs_uc
        · exact Or.inr ⟨d'', List.mem_cons_of_mem _ hMem⟩

/-- Inserting a record into a list whose carriers are all unique
preserves uniqueness. If the inserted carrier matches an existing
one, the bucket update keeps the list shape; otherwise the new
carrier joins fresh, distinct from every existing one. -/
private theorem consolidateInto_preserves_noDup
    (uc : UnifiedRow) (d : DiffWithError Int) (us : UnifiedStream)
    (h_noDup : UnifiedStream.NoDupCarriers us)
    (h_fresh : ∀ d'', (uc, d'') ∉ us) :
    UnifiedStream.NoDupCarriers (consolidateInto uc d us) := by
  -- The fresh-key precondition simplifies the proof: every
  -- insertion either matches (preserving the list) or extends.
  -- We do not need the fresh precondition for the match case,
  -- but stating it together keeps the inductive shape simple.
  induction us with
  | nil =>
    show List.Pairwise _ [(uc, d)]
    exact List.Pairwise.cons (fun _ hMem => absurd hMem List.not_mem_nil) List.Pairwise.nil
  | cons hd tl ih =>
    obtain ⟨uc', d'⟩ := hd
    obtain ⟨hHead, hTl⟩ := List.pairwise_cons.mp h_noDup
    -- The fresh precondition `∀ d'', (uc, d'') ∉ (uc', d') :: tl`
    -- gives `uc ≠ uc'`.
    have hUcNe : uc ≠ uc' := by
      intro hEq
      exact h_fresh d' (by rw [hEq]; exact List.mem_cons_self)
    have hFreshTl : ∀ d'', (uc, d'') ∉ tl :=
      fun d'' hMem => h_fresh d'' (List.mem_cons_of_mem _ hMem)
    rw [consolidateInto_skip _ _ _ _ _ hUcNe]
    -- New head's distinctness: uc' ≠ x.1 for all x in
    -- consolidateInto uc d tl.
    have hHead' : ∀ x ∈ consolidateInto uc d tl, uc' ≠ x.1 := by
      intro x hMem
      rcases mem_consolidateInto_carrier uc d tl x hMem with hUc | ⟨d'', hMem'⟩
      · exact hUc ▸ fun h => hUcNe h.symm
      · exact hHead (x.1, d'') hMem'
    exact List.Pairwise.cons hHead' (ih hTl hFreshTl)

/-- Same as above but without the fresh-key precondition: the
match branch handles the not-fresh case (the existing bucket
gets updated, list shape preserved). -/
private theorem consolidateInto_preserves_noDup_general
    (uc : UnifiedRow) (d : DiffWithError Int) (us : UnifiedStream)
    (h_noDup : UnifiedStream.NoDupCarriers us) :
    UnifiedStream.NoDupCarriers (consolidateInto uc d us) := by
  by_cases h_exists : ∃ d'', (uc, d'') ∈ us
  · -- uc already in us; existential ⊆ matching bucket on some level.
    -- Induction on us to find it.
    induction us with
    | nil => obtain ⟨_, hMem⟩ := h_exists; exact absurd hMem List.not_mem_nil
    | cons hd tl ih =>
      obtain ⟨uc', d'⟩ := hd
      obtain ⟨hHead, hTl⟩ := List.pairwise_cons.mp h_noDup
      by_cases hEq : uc = uc'
      · subst hEq
        rw [consolidateInto_match]
        exact List.Pairwise.cons hHead hTl
      · rw [consolidateInto_skip _ _ _ _ _ hEq]
        have hExistsTl : ∃ d'', (uc, d'') ∈ tl := by
          obtain ⟨d'', hMem⟩ := h_exists
          refine ⟨d'', ?_⟩
          rcases List.mem_cons.mp hMem with hH | hT
          · exact absurd ((Prod.mk.injEq _ _ _ _).mp hH).1 hEq
          · exact hT
        have hHead' : ∀ x ∈ consolidateInto uc d tl, uc' ≠ x.1 := by
          intro x hMem_x
          rcases mem_consolidateInto_carrier uc d tl x hMem_x with hUc | ⟨d''', hMem_x'⟩
          · exact hUc ▸ fun h => hEq h.symm
          · exact hHead (x.1, d''') hMem_x'
        exact List.Pairwise.cons hHead' (ih hTl hExistsTl)
  · -- uc is fresh; use the fresh-precondition variant.
    have h_fresh : ∀ d'', (uc, d'') ∉ us := by
      intro d'' hMem
      exact h_exists ⟨d'', hMem⟩
    exact consolidateInto_preserves_noDup uc d us h_noDup h_fresh

/-- The headline: `consolidate` always produces a list with unique
carriers. -/
theorem UnifiedStream.consolidate_noDup (us : UnifiedStream) :
    UnifiedStream.NoDupCarriers (UnifiedStream.consolidate us) := by
  induction us with
  | nil => exact UnifiedStream.NoDupCarriers.nil
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    show UnifiedStream.NoDupCarriers
      (consolidateInto uc d (UnifiedStream.consolidate tl))
    exact consolidateInto_preserves_noDup_general uc d _ ih

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

/-! ## Carrier preservation

`consolidate` does not lose carriers: every record that appears
in the input has a counterpart in the output with the same
carrier (the diff may have been folded into a bucket with other
records sharing the carrier). -/

private theorem consolidateInto_preserves_mem
    (uc uc' : UnifiedRow) (d d' : DiffWithError Int) (us : UnifiedStream)
    (h : (uc, d) ∈ us) :
    ∃ d'', (uc, d'') ∈ consolidateInto uc' d' us := by
  induction us with
  | nil => exact absurd h List.not_mem_nil
  | cons hd tl ih =>
    obtain ⟨uc'', d''⟩ := hd
    by_cases hEq : uc' = uc''
    · subst hEq
      rw [consolidateInto_match]
      rcases List.mem_cons.mp h with hHead | hTail
      · -- (uc, d) = (uc', d''); head of output is (uc', d' + d'').
        have hCarrier : uc = uc' := (Prod.mk.injEq _ _ _ _).mp hHead |>.1
        subst hCarrier
        exact ⟨d' + d'', List.mem_cons_self⟩
      · -- (uc, d) in tl; tail of output is tl.
        exact ⟨d, List.mem_cons_of_mem _ hTail⟩
    · rw [consolidateInto_skip _ _ _ _ _ hEq]
      rcases List.mem_cons.mp h with hHead | hTail
      · -- (uc, d) is the head record (uc'', d''); preserved verbatim.
        have hCarrier : uc = uc'' := (Prod.mk.injEq _ _ _ _).mp hHead |>.1
        have hD : d = d'' := (Prod.mk.injEq _ _ _ _).mp hHead |>.2
        subst hCarrier; subst hD
        exact ⟨d, List.mem_cons_self⟩
      · obtain ⟨d''', hMem⟩ := ih hTail
        exact ⟨d''', List.mem_cons_of_mem _ hMem⟩

/-- Every input carrier survives consolidation: an input record
with carrier `uc` has some output record with the same carrier
(diff possibly folded with siblings sharing the carrier). -/
theorem UnifiedStream.mem_consolidate_of_mem
    (us : UnifiedStream) (uc : UnifiedRow) (d : DiffWithError Int)
    (h : (uc, d) ∈ us) :
    ∃ d', (uc, d') ∈ UnifiedStream.consolidate us := by
  induction us with
  | nil => exact absurd h List.not_mem_nil
  | cons hd tl ih =>
    obtain ⟨uc', d'⟩ := hd
    rcases List.mem_cons.mp h with hHead | hTail
    · -- Head record's carrier matches; insert finds or appends bucket.
      have hCarrier : uc = uc' := (Prod.mk.injEq _ _ _ _).mp hHead |>.1
      subst hCarrier
      show ∃ d'', (uc, d'') ∈ consolidateInto uc d' (UnifiedStream.consolidate tl)
      exact mem_after_consolidateInto uc d' _
    · -- Tail contains the record; ih + carrier preservation through insert.
      obtain ⟨d'', hMem⟩ := ih hTail
      exact consolidateInto_preserves_mem uc uc' d'' d' _ hMem

/-- Reverse direction: every output carrier came from an input
carrier. The output is "no bigger" than the input on carriers. -/
theorem UnifiedStream.mem_of_mem_consolidate
    (us : UnifiedStream) (uc : UnifiedRow) :
    (∃ d, (uc, d) ∈ UnifiedStream.consolidate us)
      → ∃ d', (uc, d') ∈ us := by
  induction us with
  | nil => intro ⟨_, h⟩; exact absurd h List.not_mem_nil
  | cons hd tl ih =>
    intro ⟨d, h⟩
    obtain ⟨uc', d'⟩ := hd
    have hInto : (uc, d) ∈ consolidateInto uc' d' (UnifiedStream.consolidate tl) := h
    rcases mem_consolidateInto_carrier uc' d' _ (uc, d) hInto with hUc | ⟨d'', hMem⟩
    · -- carrier = uc'; head of input.
      subst hUc
      exact ⟨d', List.mem_cons_self⟩
    · -- carrier in consolidate tl; apply ih.
      have hMem' : (uc, d'') ∈ UnifiedStream.consolidate tl := hMem
      obtain ⟨d''', hMemTl⟩ := ih ⟨d'', hMem'⟩
      exact ⟨d''', List.mem_cons_of_mem _ hMemTl⟩

/-! ## Error-diff inversion

Reverse direction of `consolidate_preserves_error`: a `.error`
diff in the consolidated output must have come from a `.error`
diff in the input at the same carrier. Builds on the DiffSemiring
inversion `add_eq_error_left_or_right`. -/

private theorem consolidateInto_error_inv
    (uc uc' : UnifiedRow) (d' : DiffWithError Int) (us : UnifiedStream)
    (h : (uc, (DiffWithError.error : DiffWithError Int))
            ∈ consolidateInto uc' d' us) :
    (uc = uc' ∧ d' = DiffWithError.error)
      ∨ (uc, (DiffWithError.error : DiffWithError Int)) ∈ us := by
  induction us with
  | nil =>
    -- consolidateInto uc' d' [] = [(uc', d')]; so (uc, .error) = (uc', d').
    have hEq : (uc, (DiffWithError.error : DiffWithError Int)) = (uc', d') :=
      List.mem_singleton.mp h
    have hUc : uc = uc' := (Prod.mk.injEq _ _ _ _).mp hEq |>.1
    have hD  : (DiffWithError.error : DiffWithError Int) = d' :=
      (Prod.mk.injEq _ _ _ _).mp hEq |>.2
    exact Or.inl ⟨hUc, hD.symm⟩
  | cons hd tl ih =>
    obtain ⟨uc'', d''⟩ := hd
    by_cases hEq : uc' = uc''
    · subst hEq
      rw [consolidateInto_match] at h
      rcases List.mem_cons.mp h with hHead | hTail
      · -- (uc, .error) = (uc', d' + d''); two summand sources.
        have hUc : uc = uc' := (Prod.mk.injEq _ _ _ _).mp hHead |>.1
        have hD : (DiffWithError.error : DiffWithError Int) = d' + d'' :=
          (Prod.mk.injEq _ _ _ _).mp hHead |>.2
        subst hUc
        rcases DiffWithError.add_eq_error_left_or_right d' d'' hD.symm with hD' | hD''
        · exact Or.inl ⟨rfl, hD'⟩
        · subst hD''
          exact Or.inr List.mem_cons_self
      · exact Or.inr (List.mem_cons_of_mem _ hTail)
    · rw [consolidateInto_skip _ _ _ _ _ hEq] at h
      rcases List.mem_cons.mp h with hHead | hTail
      · -- (uc, .error) = (uc'', d''); preserved from input head.
        have hUc : uc = uc'' := (Prod.mk.injEq _ _ _ _).mp hHead |>.1
        have hD : (DiffWithError.error : DiffWithError Int) = d'' :=
          (Prod.mk.injEq _ _ _ _).mp hHead |>.2
        subst hUc; subst hD
        exact Or.inr List.mem_cons_self
      · rcases ih hTail with ⟨hUc, hD⟩ | hMem
        · exact Or.inl ⟨hUc, hD⟩
        · exact Or.inr (List.mem_cons_of_mem _ hMem)

/-- Every collection-err carrier in the consolidated output
corresponds to a collection-err carrier in the input at the
same carrier. The strict converse of
`consolidate_preserves_error`. -/
theorem UnifiedStream.consolidate_error_inv
    (us : UnifiedStream) (uc : UnifiedRow)
    (h : (uc, (DiffWithError.error : DiffWithError Int))
            ∈ UnifiedStream.consolidate us) :
    (uc, (DiffWithError.error : DiffWithError Int)) ∈ us := by
  induction us with
  | nil => exact absurd h List.not_mem_nil
  | cons hd tl ih =>
    obtain ⟨uc', d'⟩ := hd
    have hInto : (uc, (DiffWithError.error : DiffWithError Int))
                  ∈ consolidateInto uc' d' (UnifiedStream.consolidate tl) := h
    rcases consolidateInto_error_inv uc uc' d' _ hInto with ⟨hUc, hD⟩ | hMem
    · subst hUc; subst hD
      exact List.mem_cons_self
    · exact List.mem_cons_of_mem _ (ih hMem)

/-! ## Error-scope characterizations under `consolidate`

Set-level invariance of row-err and collection-err extractors
under consolidation. Combines carrier-preservation
(`mem_consolidate_of_mem`, `mem_of_mem_consolidate`) and error
inversion (`consolidate_preserves_error`, `consolidate_error_inv`).

These iffs are placed here (not in `Mz/SetOps.lean`) because
`Mz/TimedConsolidate.lean` cites them and the import chain forbids
SetOps as an upstream dependency. -/

theorem UnifiedStream.consolidate_errCarriers_iff
    (us : UnifiedStream) (e : EvalError) :
    e ∈ UnifiedStream.errCarriers (UnifiedStream.consolidate us)
      ↔ e ∈ UnifiedStream.errCarriers us := by
  rw [UnifiedStream.mem_errCarriers, UnifiedStream.mem_errCarriers]
  constructor
  · intro h
    exact UnifiedStream.mem_of_mem_consolidate us (UnifiedRow.err e) h
  · intro ⟨d, hMem⟩
    exact UnifiedStream.mem_consolidate_of_mem us (UnifiedRow.err e) d hMem

theorem UnifiedStream.consolidate_errorDiffCarriers_iff
    (us : UnifiedStream) (uc : UnifiedRow) :
    uc ∈ UnifiedStream.errorDiffCarriers (UnifiedStream.consolidate us)
      ↔ uc ∈ UnifiedStream.errorDiffCarriers us := by
  rw [UnifiedStream.mem_errorDiffCarriers,
      UnifiedStream.mem_errorDiffCarriers]
  exact ⟨UnifiedStream.consolidate_error_inv us uc,
         UnifiedStream.consolidate_preserves_error us uc⟩

end Mz

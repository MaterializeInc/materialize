-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

/-!
# Collections of updates

A collection is a list of `(record, diff)` updates. Its meaning is the
multiplicity of each record, the sum of that record's diffs. Two collections
are equivalent when they agree on every multiplicity, regardless of how the
updates are ordered or split.

`consolidate` merges updates to the same record and drops the ones that cancel.
It is correct when it preserves every multiplicity, leaves no zero diff, and
leaves at most one update per record.
-/

namespace Mz

abbrev Collection (α : Type) := List (α × Int)

namespace Collection

variable {α : Type} [DecidableEq α]

/-- The multiplicity of `x` in `c`. -/
def count : Collection α → α → Int
  | [], _ => 0
  | (a, d) :: rest, x => (if a = x then d else 0) + count rest x

/-- Flips the sign of every diff. -/
def negate (c : Collection α) : Collection α :=
  c.map fun (a, d) => (a, -d)

/-- Adds `d` to the update for `a`, dropping that update if it cancels. -/
def insert (a : α) (d : Int) : Collection α → Collection α
  | [] => if d = 0 then [] else [(a, d)]
  | (b, e) :: rest =>
    if b = a then
      if d + e = 0 then rest else (a, d + e) :: rest
    else
      (b, e) :: insert a d rest

/-- Merges updates to the same record and drops the ones that cancel. -/
def consolidate : Collection α → Collection α
  | [] => []
  | (a, d) :: rest => insert a d (consolidate rest)

theorem count_append (c₁ c₂ : Collection α) (x : α) :
    count (c₁ ++ c₂) x = count c₁ x + count c₂ x := by
  induction c₁ with
  | nil => simp [count]
  | cons u rest ih =>
    obtain ⟨a, d⟩ := u
    simp [count, ih, Int.add_assoc]

theorem count_negate (c : Collection α) (x : α) :
    count (negate c) x = -count c x := by
  induction c with
  | nil => simp [negate, count]
  | cons u rest ih =>
    obtain ⟨a, d⟩ := u
    simp only [negate, List.map_cons, count] at ih ⊢
    rw [ih]
    split <;> omega

/-- Appending a collection's negation cancels it. -/
theorem count_append_negate (c : Collection α) (x : α) :
    count (c ++ negate c) x = 0 := by
  rw [count_append, count_negate]
  omega

theorem count_insert (a : α) (d : Int) (c : Collection α) (x : α) :
    count (insert a d c) x = (if a = x then d else 0) + count c x := by
  induction c with
  | nil =>
    simp only [insert, count]
    split <;> simp [count, *]
  | cons u rest ih =>
    obtain ⟨b, e⟩ := u
    by_cases hb : b = a
    · subst hb
      by_cases hz : d + e = 0
      · rw [insert, ite_eq_left rfl, ite_eq_left hz, count]
        by_cases hx : b = x <;> simp [hx] <;> omega
      · rw [insert, ite_eq_left rfl, ite_eq_right hz, count, count]
        by_cases hx : b = x <;> simp [hx] <;> omega
    · rw [insert, ite_eq_right hb, count, count, ih]
      omega

/-- Consolidation preserves the multiplicity of every record. -/
theorem count_consolidate (c : Collection α) (x : α) :
    count (consolidate c) x = count c x := by
  induction c with
  | nil => rfl
  | cons u rest ih =>
    obtain ⟨a, d⟩ := u
    simp only [consolidate, count_insert, ih, count]

/-- Every update has a nonzero diff. -/
def NoZeros (c : Collection α) : Prop :=
  ∀ u ∈ c, u.2 ≠ 0

theorem noZeros_insert (a : α) (d : Int) (c : Collection α) (h : NoZeros c) :
    NoZeros (insert a d c) := by
  induction c with
  | nil =>
    simp only [insert]
    split
    · simp [NoZeros]
    · simp [NoZeros, *]
  | cons u rest ih =>
    obtain ⟨b, e⟩ := u
    have hrest : NoZeros rest := fun v hv => h v (List.mem_cons_of_mem _ hv)
    have he : e ≠ 0 := h (b, e) List.mem_cons_self
    simp only [insert]
    split
    · split
      · exact hrest
      · intro v hv
        cases List.mem_cons.mp hv with
        | inl hv => subst hv; assumption
        | inr hv => exact hrest v hv
    · intro v hv
      cases List.mem_cons.mp hv with
      | inl hv => subst hv; exact he
      | inr hv => exact ih hrest v hv

/-- Consolidation leaves no update with a zero diff. -/
theorem noZeros_consolidate (c : Collection α) : NoZeros (consolidate c) := by
  induction c with
  | nil => simp [consolidate, NoZeros]
  | cons u rest ih =>
    obtain ⟨a, d⟩ := u
    exact noZeros_insert a d _ ih

/-- The records that have an update. -/
def keys (c : Collection α) : List α :=
  c.map Prod.fst

theorem mem_keys_insert {a x : α} {d : Int} {c : Collection α}
    (h : x ∈ keys (insert a d c)) : x = a ∨ x ∈ keys c := by
  induction c with
  | nil =>
    simp only [insert] at h
    split at h <;> simp_all [keys]
  | cons u rest ih =>
    obtain ⟨b, e⟩ := u
    simp only [insert] at h
    split at h
    · split at h <;> simp_all [keys]
    · simp only [keys, List.map_cons, List.mem_cons] at h ⊢
      rcases h with h | h
      · exact Or.inr (Or.inl h)
      · rcases ih h with h | h
        · exact Or.inl h
        · exact Or.inr (Or.inr h)

theorem nodup_insert (a : α) (d : Int) (c : Collection α) (h : (keys c).Nodup) :
    (keys (insert a d c)).Nodup := by
  induction c with
  | nil =>
    simp only [insert]
    split <;> simp [keys]
  | cons u rest ih =>
    obtain ⟨b, e⟩ := u
    simp only [keys, List.map_cons, List.nodup_cons] at h
    obtain ⟨hb, hrest⟩ := h
    simp only [insert]
    split
    · subst_vars
      split
      · exact hrest
      · simp only [keys, List.map_cons, List.nodup_cons]
        exact ⟨hb, hrest⟩
    · simp only [keys, List.map_cons, List.nodup_cons]
      refine ⟨fun hmem => ?_, ih hrest⟩
      rcases mem_keys_insert hmem with h | h
      · contradiction
      · exact hb h

/-- Consolidation leaves at most one update per record. -/
theorem nodup_consolidate (c : Collection α) : (keys (consolidate c)).Nodup := by
  induction c with
  | nil => simp [consolidate, keys]
  | cons u rest ih =>
    obtain ⟨a, d⟩ := u
    exact nodup_insert a d _ ih

example : consolidate ([(1, 2), (2, 1), (1, -2)] : Collection Nat) = [(2, 1)] := by
  decide

end Collection

end Mz

-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.
--
-- Theorems 3-4: a dropped collection can't be referenced again, and
-- CreateDataflow's export ids are fresh.

import MzCompute.State
import MzCompute.Run

namespace MzCompute

/-- A successful `guard` returns its payload unchanged. -/
private theorem guard_eq_some {α : Type _} {cond : Bool} {x y : α}
    (h : guard cond x = some y) : x = y := by
  unfold guard at h
  cases cond with
  | true => simpa using h
  | false => simp at h

/-- Theorem 3, part 1. Encodes command.rs:152-153. -/
theorem schedule_fails_when_dropped (s : ProtocolState) (id : GlobalId) (h : s.dropped id = true) :
    step s (.cmd (.schedule id)) = none := by
  simp [step, guard, h]

/-- Theorem 3, part 2. Encodes command.rs:162-163. -/
theorem allowWrites_fails_when_dropped (s : ProtocolState) (id : GlobalId) (h : s.dropped id = true) :
    step s (.cmd (.allowWrites id)) = none := by
  simp [step, guard, h]

/-- Theorem 3, part 3. `command.rs` does not give index-peek-after-drop
the same explicit "invalid to" wording as Schedule/AllowWrites, but a
dropped collection's arrangement is gone, so this model treats it the
same way (see the design doc's Solution Proposal). -/
theorem peekIndex_fails_when_dropped (s : ProtocolState) (id uuid : Uuid) (h : s.dropped id = true) :
    step s (.cmd (.peek uuid (.index id))) = none := by
  simp [step, guard, h]

/-- `dropped` only ever grows: no step un-drops a collection. -/
theorem dropped_monotone (s s' : ProtocolState) (e : Event) (h : step s e = some s') (id : GlobalId) :
    s.dropped id = true → s'.dropped id = true := by
  intro hd
  cases e with
  | cmd c =>
      cases c with
      | allowCompaction id' frontier =>
          simp only [step] at h
          have hs := guard_eq_some h
          subst hs
          simp [hd]
      | _ =>
          simp only [step] at h
          have hs := guard_eq_some h
          subst hs
          exact hd
  | resp r =>
      cases r <;>
        (simp only [step] at h
         have hs := guard_eq_some h
         subst hs
         exact hd)

/-- Theorem 3, lifted across a whole execution. Once `id` is dropped,
no later `Schedule`, `AllowWrites`, or index-target `Peek` for `id`
can succeed. -/
theorem no_reference_after_drop (pre mid : List Event) (e : Event) (id uuid : Uuid)
    (s s' s'' : ProtocolState)
    (hpre : run initState pre = some s)
    (hdrop : s.dropped id = true)
    (hmid : run s mid = some s')
    (he : e = .cmd (.schedule id) ∨ e = .cmd (.allowWrites id) ∨ e = .cmd (.peek uuid (.index id)))
    (hstep : step s' e = some s'') : False := by
  have hdrop' : s'.dropped id = true := by
    clear hpre he hstep
    induction mid generalizing s with
    | nil => simp [run] at hmid; subst hmid; exact hdrop
    | cons e0 rest ih =>
        unfold run at hmid
        cases hstep0 : step s e0 with
        | none => simp [hstep0] at hmid
        | some s1 =>
            simp only [hstep0] at hmid
            exact ih s1 (dropped_monotone s s1 e0 hstep0 id hdrop) hmid
  rcases he with he | he | he
  · rw [he, schedule_fails_when_dropped s' id hdrop'] at hstep; simp at hstep
  · rw [he, allowWrites_fails_when_dropped s' id hdrop'] at hstep; simp at hstep
  · rw [he, peekIndex_fails_when_dropped s' id uuid hdrop'] at hstep; simp at hstep

/-- Decodes `freshExports`'s `Bool` into the two `Prop`s `command.rs`
states in English: no id in `exports` is already created, and
`exports` has no internal duplicate. -/
theorem freshExports_sound (created : GlobalId → Bool) (exports : List GlobalId)
    (h : freshExports created exports = true) :
    (∀ id ∈ exports, created id = false) ∧ exports.Nodup := by
  induction exports with
  | nil => exact ⟨by simp, List.nodup_nil⟩
  | cons id rest ih =>
      simp only [freshExports, Bool.and_eq_true] at h
      obtain ⟨⟨hnc, hne⟩, hrest⟩ := h
      obtain ⟨ihc, ihn⟩ := ih hrest
      refine ⟨?_, ?_⟩
      · intro id' hmem
        simp only [List.mem_cons] at hmem
        rcases hmem with rfl | hmem
        · simpa using hnc
        · exact ihc id' hmem
      · rw [List.nodup_cons]
        refine ⟨?_, ihn⟩
        intro hmem
        exact absurd (List.elem_eq_true_of_mem hmem) (by simpa using hne)

/-- Theorem 4. Encodes command.rs:107-108: a CreateDataflow's export
ids are pairwise distinct and disjoint from every id created earlier
in the execution. -/
theorem create_dataflow_ids_fresh (s s' : ProtocolState) (exports : List GlobalId)
    (h : step s (.cmd (.createDataflow exports)) = some s') :
    (∀ id ∈ exports, s.created id = false) ∧ exports.Nodup := by
  simp only [step, guard] at h
  split at h
  · rename_i hcond
    rw [Bool.and_eq_true] at hcond
    exact freshExports_sound s.created exports hcond.2
  · contradiction

end MzCompute

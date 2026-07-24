-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.
--
-- Theorems 6-7: at most one PeekResponse per Peek, and CancelPeek
-- requires a prior Peek.

import MzCompute.State
import MzCompute.Run

namespace MzCompute

/-- A successful `guard` returns its payload unchanged. Local copy of
the helper `Staging.lean`/`Frontiers.lean` use. -/
private theorem guard_eq_some {α : Type _} {cond : Bool} {x y : α}
    (h : guard cond x = some y) : x = y := by
  unfold guard at h
  cases cond with
  | true => simpa using h
  | false => simp at h

/-- A successful `guard` witnesses its condition held. -/
private theorem guard_cond {α : Type _} {cond : Bool} {x y : α}
    (h : guard cond x = some y) : cond = true := by
  unfold guard at h
  cases cond with
  | true => rfl
  | false => simp at h

theorem peekResponse_requires_pending (s s' : ProtocolState) (uuid : Uuid)
    (h : step s (.resp (.peekResponse uuid)) = some s') :
    s.peeks uuid = .pending ∧ s'.peeks uuid = .answered := by
  simp only [step, guard] at h
  split at h
  · rename_i hcond
    injection h with h
    subst h
    refine ⟨?_, by simp⟩
    cases hp : s.peeks uuid with
    | pending => rfl
    | notSeen => simp [hp, PeekState.isPending] at hcond
    | answered => simp [hp, PeekState.isPending] at hcond
  · contradiction

theorem peek_requires_notSeen (s s' : ProtocolState) (uuid : Uuid) (target : PeekTarget)
    (h : step s (.cmd (.peek uuid target)) = some s') :
    s.peeks uuid = .notSeen ∧ s'.peeks uuid = .pending := by
  simp only [step] at h
  have hcond := guard_cond h
  have hs := guard_eq_some h
  subst hs
  refine ⟨?_, by simp⟩
  cases hp : s.peeks uuid with
  | notSeen => rfl
  | pending => simp [hp, PeekState.isNotSeen] at hcond
  | answered => simp [hp, PeekState.isNotSeen] at hcond

/-- Once `.answered`, a peek's state stays `.answered`: no `step` arm
moves a peek out of `.answered`. -/
theorem peeks_answered_monotone (s s' : ProtocolState) (e : Event) (h : step s e = some s')
    (uuid : Uuid) : s.peeks uuid = .answered → s'.peeks uuid = .answered := by
  intro ha
  cases e with
  | cmd c =>
      cases c with
      | peek uuid' _ =>
          simp only [step] at h
          have hs := guard_eq_some h
          have hcond := guard_cond h
          subst hs
          simp only []
          split
          · rename_i heq
            have huuid : uuid = uuid' := eq_of_beq heq
            subst huuid
            simp only [Bool.and_eq_true] at hcond
            obtain ⟨⟨_, hnot⟩, _⟩ := hcond
            rw [ha] at hnot
            simp [PeekState.isNotSeen] at hnot
          · exact ha
      | _ =>
          simp only [step] at h
          have hs := guard_eq_some h
          subst hs
          exact ha
  | resp r =>
      cases r with
      | peekResponse uuid' =>
          simp only [step] at h
          have hs := guard_eq_some h
          subst hs
          simp only []
          split
          · rfl
          · exact ha
      | _ =>
          simp only [step] at h
          have hs := guard_eq_some h
          subst hs
          exact ha

/-- Theorem 7. Encodes command.rs:248-250. -/
theorem cancel_requires_peek (s s' : ProtocolState) (uuid : Uuid) (h : step s (.cmd (.cancelPeek uuid)) = some s') :
    s.peeks uuid ≠ .notSeen := by
  simp only [step, guard] at h
  split at h
  · rename_i hcond
    intro hcontra
    simp [hcontra, PeekState.isNotSeen] at hcond
  · contradiction

/-- A single step either leaves a peek's `notSeen`-ness reflected in the
prior state, or is exactly the `Peek` that first touched this uuid. Only
a `Peek uuid _` moves `peeks uuid` out of `notSeen`. A `PeekResponse
uuid` also writes `peeks uuid`, but its guard already required the peek
to be `pending`, so the prior state was not `notSeen` either. -/
theorem peeks_notSeen_of_step (s s' : ProtocolState) (e : Event) (uuid : Uuid)
    (h : step s e = some s') (hns : s'.peeks uuid ≠ .notSeen) :
    s.peeks uuid ≠ .notSeen ∨ ∃ target, e = .cmd (.peek uuid target) := by
  cases e with
  | cmd c =>
      cases c with
      | peek uuid' target =>
          by_cases huuid : uuid' = uuid
          · exact Or.inr ⟨target, by rw [huuid]⟩
          · left
            simp only [step] at h
            have hs := guard_eq_some h
            subst hs
            simp only [] at hns
            have hne : (uuid == uuid') = false := by
              simp only [beq_eq_false_iff_ne, ne_eq]
              exact fun heq => huuid heq.symm
            simpa only [hne, if_false, Bool.false_eq_true] using hns
      | _ =>
          left
          simp only [step] at h
          have hs := guard_eq_some h
          subst hs
          exact hns
  | resp r =>
      cases r with
      | peekResponse uuid' =>
          left
          simp only [step] at h
          have hs := guard_eq_some h
          have hcond := guard_cond h
          subst hs
          by_cases huuid : uuid' = uuid
          · subst huuid
            simp only [Bool.and_eq_true] at hcond
            obtain ⟨_, hpend⟩ := hcond
            cases hp : s.peeks uuid' with
            | pending => simp
            | notSeen => rw [hp] at hpend; simp [PeekState.isPending] at hpend
            | answered => simp
          · simp only [] at hns
            have hne : (uuid == uuid') = false := by
              simp only [beq_eq_false_iff_ne, ne_eq]
              exact fun heq => huuid heq.symm
            simpa only [hne, if_false, Bool.false_eq_true] using hns
      | _ =>
          left
          simp only [step] at h
          have hs := guard_eq_some h
          subst hs
          exact hns

theorem peeks_notSeen_of_run_from (es : List Event) (s0 s : ProtocolState) (h : run s0 es = some s)
    (uuid : Uuid) (hns : s.peeks uuid ≠ .notSeen) :
    s0.peeks uuid ≠ .notSeen ∨ ∃ target, .cmd (.peek uuid target) ∈ es := by
  induction es generalizing s0 with
  | nil => simp [run] at h; subst h; exact Or.inl hns
  | cons e rest ih =>
      unfold run at h
      cases hstep : step s0 e with
      | none => simp [hstep] at h
      | some s1 =>
          simp only [hstep] at h
          rcases ih s1 h with h1 | ⟨target, hmem⟩
          · rcases peeks_notSeen_of_step s0 s1 e uuid hstep h1 with h0 | ⟨target, heq⟩
            · exact Or.inl h0
            · exact Or.inr ⟨target, by rw [heq]; exact List.mem_cons_self⟩
          · exact Or.inr ⟨target, List.mem_cons_of_mem e hmem⟩

/-- Lifting `peeks_answered_monotone` across a whole execution. -/
theorem peeks_answered_run (es : List Event) (s s' : ProtocolState) (uuid : Uuid)
    (h : run s es = some s') : s.peeks uuid = .answered → s'.peeks uuid = .answered := by
  induction es generalizing s with
  | nil => intro ha; simp [run] at h; subst h; exact ha
  | cons e rest ih =>
      intro ha
      unfold run at h
      cases hstep0 : step s e with
      | none => simp [hstep0] at h
      | some sm =>
          simp only [hstep0] at h
          exact ih sm h (peeks_answered_monotone s sm e hstep0 uuid ha)

/-- Theorem 6. Encodes response.rs:66-69: exactly one PeekResponse per
Peek. Phrased as "a second PeekResponse for the same uuid, anywhere
later in a well-formed execution, is impossible". See the design
doc's "Target theorems" section for why this form captures
uniqueness. -/
theorem peek_response_unique (pre mid : List Event) (uuid : Uuid) (s1 s2 s3 s4 : ProtocolState)
    (hpre : run initState pre = some s1)
    (hstep1 : step s1 (.resp (.peekResponse uuid)) = some s2)
    (hmid : run s2 mid = some s3)
    (hstep2 : step s3 (.resp (.peekResponse uuid)) = some s4) : False := by
  have h2 : s2.peeks uuid = .answered := (peekResponse_requires_pending s1 s2 uuid hstep1).2
  have h3 : s3.peeks uuid = .answered := peeks_answered_run mid s2 s3 uuid hmid h2
  have h4 : s3.peeks uuid = .pending := (peekResponse_requires_pending s3 s4 uuid hstep2).1
  rw [h3] at h4
  exact absurd h4 (by simp)

/-- Theorem 6, provenance half: a `PeekResponse` is always preceded by
a matching `Peek`. -/
theorem peek_response_preceded_by_peek (pre : List Event) (uuid : Uuid) (s s' : ProtocolState)
    (hpre : run initState pre = some s)
    (hstep : step s (.resp (.peekResponse uuid)) = some s') :
    ∃ target, .cmd (.peek uuid target) ∈ pre := by
  have hp : s.peeks uuid = .pending := (peekResponse_requires_pending s s' uuid hstep).1
  rcases peeks_notSeen_of_run_from pre initState s hpre uuid (by simp [hp]) with h0 | hex
  · simp [initState] at h0
  · exact hex

end MzCompute

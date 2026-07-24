-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.
--
-- Staging-order theorems: the first two commands of any legal
-- execution are `Hello` then `CreateInstance`, and any event naming a
-- collection `id` requires a prior `CreateDataflow` exporting `id`.
-- These are the plan's Minimal Viable Prototype, validating the
-- state/step/run shape before the drop, frontier, and peek theorems.

import MzCompute.Run

namespace MzCompute

/-- Theorem 1. Encodes protocol.rs:52-58: Hello is the first command
of the Creation stage. -/
theorem hello_first (e : Event) (rest : List Event) (s : ProtocolState)
    (h : run initState (e :: rest) = some s) : e.isHello = true := by
  unfold run at h
  cases e with
  | cmd c =>
      cases c with
      | hello _ => rfl
      | _ => simp [step, guard, initState, Stage.isCreation] at h
  | resp r =>
      cases r <;> simp [step, guard, initState, Stage.isCreation] at h

/-- Theorem 1, continued. Encodes protocol.rs:52-58: CreateInstance is
the second command, completing the Creation stage. -/
theorem create_instance_second (e1 e2 : Event) (rest : List Event) (s : ProtocolState)
    (h : run initState (e1 :: e2 :: rest) = some s) : e2.isCreateInstance = true := by
  have he1 : e1.isHello = true := hello_first e1 (e2 :: rest) s h
  unfold run at h
  cases e1 with
  | cmd c =>
      cases c with
      | hello n =>
          cases e2 with
          | cmd c2 =>
              cases c2 with
              | createInstance => rfl
              | _ => simp [run, step, guard, initState, Stage.isCreation] at h
          | resp r2 => cases r2 <;> simp [run, step, guard, initState, Stage.isCreation] at h
      | _ => simp [Event.isHello] at he1
  | resp _ => simp [Event.isHello] at he1

/-- A successful `guard` returns its payload unchanged. -/
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

/-- A single step either preserves `created` or, for a `CreateDataflow`
event, adds exactly its export list. -/
theorem created_of_step (s s' : ProtocolState) (e : Event) (h : step s e = some s') (id : GlobalId) :
    s'.created id = true →
      s.created id = true ∨ ∃ exports, e = .cmd (.createDataflow exports) ∧ exports.elem id = true := by
  intro hc
  cases e with
  | cmd c =>
      cases c with
      | createDataflow exports =>
          simp only [step] at h
          have hs := guard_eq_some h
          subst hs
          simp only [Bool.or_eq_true] at hc
          rcases hc with h1 | h2
          · exact Or.inl h1
          · exact Or.inr ⟨exports, rfl, h2⟩
      | _ =>
          simp only [step] at h
          have hs := guard_eq_some h
          subst hs
          exact Or.inl hc
  | resp r =>
      cases r <;>
        (simp only [step] at h
         have hs := guard_eq_some h
         subst hs
         exact Or.inl hc)

/-- Generalization of `created_of_run` over the starting state, the
form the induction needs. -/
theorem created_of_run_from (es : List Event) (s0 s : ProtocolState) (h : run s0 es = some s)
    (id : GlobalId) (hc : s.created id = true) :
    s0.created id = true ∨
      ∃ (pre : List Event) (exports : List GlobalId) (suf : List Event),
        es = pre ++ [.cmd (.createDataflow exports)] ++ suf ∧ exports.elem id = true := by
  induction es generalizing s0 with
  | nil => simp [run] at h; subst h; exact Or.inl hc
  | cons e rest ih =>
      unfold run at h
      cases hstep : step s0 e with
      | none => simp [hstep] at h
      | some s1 =>
          simp only [hstep] at h
          rcases ih s1 h with h1 | ⟨pre, exports, suf, heq, hel⟩
          · rcases created_of_step s0 s1 e hstep id h1 with h0 | ⟨exports, heq, hel⟩
            · exact Or.inl h0
            · exact Or.inr ⟨[], exports, rest, by simp [heq], hel⟩
          · exact Or.inr ⟨e :: pre, exports, suf, by simp [heq], hel⟩

/-- Lifting `created_of_step` across a whole execution: if `id` is
created at the end, some `CreateDataflow` event in the execution
exported it. -/
theorem created_of_run (es : List Event) (s : ProtocolState) (h : run initState es = some s)
    (id : GlobalId) (hc : s.created id = true) :
    ∃ (pre : List Event) (exports : List GlobalId) (suf : List Event),
      es = pre ++ [.cmd (.createDataflow exports)] ++ suf ∧ exports.elem id = true := by
  rcases created_of_run_from es initState s h id hc with h0 | hex
  · simp [initState] at h0
  · exact hex

/-- Theorem 2. Encodes command.rs:148-153, :158-163, :185-187,
:219-224 and response.rs:52-54, :106-107, :120-121: any event naming
`id` (other than the `CreateDataflow` that creates it) requires a
prior `CreateDataflow` exporting `id`. -/
theorem create_before_reference (pre : List Event) (mid : Event) (id : GlobalId)
    (s s' : ProtocolState)
    (hpre : run initState pre = some s)
    (hstep : step s mid = some s')
    (href : mid.references id = true) :
    ∃ (pre' : List Event) (exports : List GlobalId) (suf : List Event),
      pre = pre' ++ [.cmd (.createDataflow exports)] ++ suf ∧ exports.elem id = true := by
  have hc : s.created id = true := by
    cases mid with
    | cmd c =>
        cases c with
        | schedule id' =>
            simp only [Event.references, beq_iff_eq] at href
            subst href
            simp only [step] at hstep
            have hcond := guard_cond hstep
            simp only [Bool.and_eq_true] at hcond
            simp_all
        | allowWrites id' =>
            simp only [Event.references, beq_iff_eq] at href
            subst href
            simp only [step] at hstep
            have hcond := guard_cond hstep
            simp only [Bool.and_eq_true] at hcond
            simp_all
        | allowCompaction id' _ =>
            simp only [Event.references, beq_iff_eq] at href
            subst href
            simp only [step] at hstep
            have hcond := guard_cond hstep
            simp only [Bool.and_eq_true] at hcond
            simp_all
        | peek _ target =>
            cases target with
            | index id' =>
                simp only [Event.references, beq_iff_eq] at href
                subst href
                simp only [step] at hstep
                have hcond := guard_cond hstep
                simp only [Bool.and_eq_true] at hcond
                simp_all
            | persist _ => simp [Event.references] at href
        | _ => simp [Event.references] at href
    | resp r =>
        cases r with
        | frontiers id' _ _ _ =>
            simp only [Event.references, beq_iff_eq] at href
            subst href
            simp only [step] at hstep
            have hcond := guard_cond hstep
            simp only [Bool.and_eq_true] at hcond
            simp_all
        | subscribeResponse id' =>
            simp only [Event.references, beq_iff_eq] at href
            subst href
            simp only [step] at hstep
            have hcond := guard_cond hstep
            simp only [Bool.and_eq_true] at hcond
            simp_all
        | copyToResponse id' =>
            simp only [Event.references, beq_iff_eq] at href
            subst href
            simp only [step] at hstep
            have hcond := guard_cond hstep
            simp only [Bool.and_eq_true] at hcond
            simp_all
        | _ => simp [Event.references] at href
  exact created_of_run pre s hpre id hc

end MzCompute

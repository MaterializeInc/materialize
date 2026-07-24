-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.
--
-- Theorem 5: reported frontiers never regress, and once a kind
-- reports the empty frontier for an id, it stays empty.

import MzCompute.State
import MzCompute.Run

namespace MzCompute

theorem Frontier.advances_refl (f : Frontier) : Frontier.advances f f = true := by
  cases f with
  | «at» t => simp [Frontier.advances, Nat.ble_eq]
  | empty => rfl

theorem Frontier.advances_trans {a b c : Frontier}
    (hab : Frontier.advances b a = true) (hbc : Frontier.advances c b = true) :
    Frontier.advances c a = true := by
  cases a with
  | empty => cases b with
      | empty => cases c with
          | empty => rfl
          | «at» _ => simp [Frontier.advances] at hbc
      | «at» _ => simp [Frontier.advances] at hab
  | «at» o =>
      cases b with
      | empty => cases c with
          | empty => rfl
          | «at» _ => simp [Frontier.advances] at hbc
      | «at» m =>
          cases c with
          | empty => rfl
          | «at» n =>
              simp only [Frontier.advances] at hab hbc ⊢
              exact Nat.ble_eq_true_of_le
                (Nat.le_trans (Nat.le_of_ble_eq_true hab) (Nat.le_of_ble_eq_true hbc))

/-- Only `.empty` can advance past `.empty`. -/
theorem Frontier.advances_empty_inv (new : Frontier) (h : Frontier.advances new .empty = true) :
    new = .empty := by
  cases new with
  | empty => rfl
  | «at» _ => simp [Frontier.advances] at h

/-- A passing `checkAdvance` guarantees the applied result advances the
old value: when the report is absent the value is unchanged (reflexive),
and when present the guard is exactly `Frontier.advances`. -/
theorem checkAdvance_advances (new? : Option Frontier) (old : Frontier)
    (h : checkAdvance new? old = true) : Frontier.advances (applyAdvance new? old) old = true := by
  cases new? with
  | none => exact Frontier.advances_refl old
  | some new => simpa [checkAdvance, applyAdvance] using h

/-- Theorem 5, single-step form. Encodes response.rs:37-42
(monotonicity) and :44-51 (terminality, which is the same fact
specialized to `old = .empty` via `advances_empty_inv`). -/
theorem frontiers_advance (s s' : ProtocolState) (id : GlobalId) (w i o : Option Frontier)
    (h : step s (.resp (.frontiers id w i o)) = some s') (k : FrontierKind) :
    Frontier.advances (s'.reportedFrontiers id k) (s.reportedFrontiers id k) = true := by
  simp only [step, guard] at h
  split at h
  · rename_i hcond
    injection h with h
    subst h
    simp only [Bool.and_eq_true] at hcond
    obtain ⟨⟨⟨⟨-, -⟩, hw⟩, hi⟩, ho⟩ := hcond
    cases k with
    | write => simpa using (checkAdvance_advances w (s.reportedFrontiers id .write) hw)
    | input => simpa using (checkAdvance_advances i (s.reportedFrontiers id .input) hi)
    | output => simpa using (checkAdvance_advances o (s.reportedFrontiers id .output) ho)
  · contradiction

/-- A successful `guard` returns its payload unchanged. Local copy of
the helper `Legality.lean` uses. -/
private theorem guard_eq_some {α : Type _} {cond : Bool} {x y : α}
    (h : guard cond x = some y) : x = y := by
  unfold guard at h
  cases cond with
  | true => simpa using h
  | false => simp at h

/-- Theorem 5, lifted across a whole execution. -/
theorem reportedFrontiers_monotone (s0 s : ProtocolState) (es : List Event)
    (h : run s0 es = some s) (id : GlobalId) (k : FrontierKind) :
    Frontier.advances (s.reportedFrontiers id k) (s0.reportedFrontiers id k) = true := by
  induction es generalizing s0 with
  | nil => simp [run] at h; subst h; exact Frontier.advances_refl _
  | cons e rest ih =>
      unfold run at h
      cases hstep : step s0 e with
      | none => simp [hstep] at h
      | some s1 =>
          simp [hstep] at h
          have h1 : Frontier.advances (s1.reportedFrontiers id k) (s0.reportedFrontiers id k) = true := by
            cases e with
            | resp r =>
                cases r with
                | frontiers id' w i o =>
                    by_cases hid : id' = id
                    · subst id'; exact frontiers_advance s0 s1 id w i o hstep k
                    · -- A Frontiers response for a different id leaves
                      -- `reportedFrontiers id k` untouched.
                      simp only [step] at hstep
                      have hs := guard_eq_some hstep
                      subst hs
                      have hne : (id == id') = false := by
                        simp only [beq_eq_false_iff_ne, ne_eq]
                        exact fun heq => hid heq.symm
                      simp only [hne, if_false, Bool.false_eq_true]
                      exact Frontier.advances_refl _
                | peekResponse _ =>
                    simp only [step] at hstep
                    have hs := guard_eq_some hstep; subst hs
                    exact Frontier.advances_refl _
                | subscribeResponse _ =>
                    simp only [step] at hstep
                    have hs := guard_eq_some hstep; subst hs
                    exact Frontier.advances_refl _
                | copyToResponse _ =>
                    simp only [step] at hstep
                    have hs := guard_eq_some hstep; subst hs
                    exact Frontier.advances_refl _
                | status =>
                    simp only [step] at hstep
                    have hs := guard_eq_some hstep; subst hs
                    exact Frontier.advances_refl _
            | cmd c =>
                cases c <;>
                  (simp only [step] at hstep
                   have hs := guard_eq_some hstep
                   subst hs
                   exact Frontier.advances_refl _)
          exact Frontier.advances_trans h1 (ih s1 h)

/-- Theorem 5, terminality corollary: once a kind reports empty, it
stays empty for the rest of the execution. -/
theorem reportedFrontiers_terminal (s0 s : ProtocolState) (es : List Event)
    (h : run s0 es = some s) (id : GlobalId) (k : FrontierKind)
    (hempty : s0.reportedFrontiers id k = .empty) :
    s.reportedFrontiers id k = .empty :=
  Frontier.advances_empty_inv _ (hempty ▸ reportedFrontiers_monotone s0 s es h id k)

/-- Theorem 5, combined statement: monotonicity plus terminality, over
an execution rooted at `initState`. -/
theorem frontiers_monotone_and_terminal (es : List Event) (s : ProtocolState)
    (h : run initState es = some s) (id : GlobalId) (k : FrontierKind) :
    Frontier.advances (s.reportedFrontiers id k) (initState.reportedFrontiers id k) = true ∧
      (initState.reportedFrontiers id k = .empty → s.reportedFrontiers id k = .empty) :=
  ⟨reportedFrontiers_monotone initState s es h id k,
   fun hempty => reportedFrontiers_terminal initState s es h id k hempty⟩

end MzCompute

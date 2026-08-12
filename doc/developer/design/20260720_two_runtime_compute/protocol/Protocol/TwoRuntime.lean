/-
Copyright Materialize, Inc. and contributors. All rights reserved.

Use of this software is governed by the Business Source License
included in the LICENSE file at the root of this repository.

As of the Change Date specified in that file, in accordance with
the Business Source License, use of this software will be governed
by the Apache License, Version 2.0.
-/

/-!
# The two-runtime compute command protocol

A model of the command protocol between the compute controller and a `clusterd`
process that hosts two compute runtimes. `CreateDataflow` for an interactive
dataflow goes only to the interactive runtime while `AllowCompaction` for the
index it imports goes only to the maintenance runtime, and the two runtimes drain
their command streams independently. A single stream used to order those two
against each other. The multiplexer
(`mz_compute_client::multiplex::Multiplexer`) is the only point that sees both,
so it is where the ordering has to be restored.

Two properties are proved of the protocol as implemented:

* `since_le_as_of`, the invariant the split otherwise loses: an index is never
  compacted past the `as_of` of a dataflow that has been created and has not yet
  rendered.
* `physical_le_since`: the publisher never forwards a physical compaction
  frontier beyond the published `since`. `set_physical_compaction F` promises the
  trace stays readable at every frontier at or beyond `F`, and every `as_of` the
  controller may offer is at or beyond `since`, so this is what makes a reader's
  cut available.

and one supporting property, `no_regression`: a compaction frontier in flight to
maintenance is never below what maintenance has already applied.

Both properties failed in earlier revisions of the implementation, in two
independent ways. Rather than model only the fixed protocol, `Step` is
parameterised by two booleans that select the old behaviour, and each is given a
counterexample: `release_on_drop_violates_invariant` and
`physical_from_upper_violates_invariant`. A model that can only express the fixed
system cannot tell you it fixed anything.

## What is modelled

One index, one interactive dataflow, the controller's read hold, the
multiplexer's cap, two independent command queues, and the point at which each
runtime realizes a command. Frontiers are single times rather than antichains,
which is faithful for the single-element antichains a dataflow `as_of` carries.

## What is not

Data, rendering cost, failure and reconnection, more than one dataflow or index,
and liveness. The queued-compaction flush is modelled as an action that may fire
(`muxFlush`), not as one that must, so this says nothing about a deferred
compaction actually being forwarded. `Multiplexer::reset` and the hold leak it
addresses are outside the model: they concern a second connection, and the model
has one.
-/

set_option autoImplicit false

namespace Protocol

/-- A frontier, as a single time. See the module note on antichains. -/
abbrev Time := Nat

/--
The protocol state.

`since` and `physical` are the maintenance replica's two compaction frontiers for
the index. `upper` is the writer's seal frontier, which is present only so that
the discarded "forward the stream upper as the physical target" rule can be
expressed and refuted.
-/
structure State where
  /-- The controller's read hold on the index, or `none`. -/
  ctlHold : Option Time
  /-- The lowest frontier the index may still be told to compact to.
      `Multiplexer::compaction_floor`. -/
  floor : Time
  /-- Compaction commands the maintenance runtime has not processed. -/
  maintQueue : List Time
  /-- Dataflow `as_of`s the interactive runtime has not processed. -/
  interQueue : List Time
  /-- The index's logical compaction frontier on the replica. -/
  since : Time
  /-- The index's physical compaction frontier on the replica. -/
  physical : Time
  /-- The writer's seal frontier. -/
  upper : Time
  /-- Whether the interactive dataflow has rendered, and so holds the shared
      trace itself. -/
  rendered : Bool
  /-- Whether the interactive runtime has reported a frontier for the dataflow.
      This is what retires the multiplexer's hold. -/
  reported : Bool
  /-- The interactive dataflow's `as_of`, or `none` if there is no dataflow. -/
  dfAsOf : Option Time
  /-- The multiplexer's recorded hold. `Multiplexer::interactive_holds`. -/
  muxHold : Option Time
  deriving Repr, DecidableEq

/-- Nothing created, nothing compacted, both queues empty. -/
def init : State where
  ctlHold := none
  floor := 0
  maintQueue := []
  interQueue := []
  since := 0
  physical := 0
  upper := 0
  rendered := false
  reported := false
  dfAsOf := none
  muxHold := none

/--
The frontier the multiplexer forwards when the controller asks for `t`.

Mirrors the `capped` binding in `Multiplexer::send`. The `floor ≤ h` conjunct is
the "decline to cap" case: a hold already below what has been released cannot
restore anything, and capping there would regress the collection's frontier.
-/
def capped (muxHold : Option Time) (floor t : Time) : Time :=
  match muxHold with
  | some h => if h < t ∧ floor ≤ h then h else t
  | none => t

/-- The cap never lands below the floor it was given. -/
theorem le_capped {muxHold : Option Time} {floor t : Time} (hfloor : floor ≤ t) :
    floor ≤ capped muxHold floor t := by
  cases muxHold with
  | none => simpa [capped] using hfloor
  | some h =>
    simp only [capped]
    by_cases hc : h < t ∧ floor ≤ h
    · rw [if_pos hc]; exact hc.2
    · rw [if_neg hc]; exact hfloor

/--
With a hold recorded at `a` and the floor already at or below `a`, the cap lands at
or below `a`.

Both branches give it. The cap branch returns `a` itself. The decline-to-cap
branch is only reachable when the requested frontier is already at or below `a`,
because the floor bound rules out the other way of failing the condition. That
second half is exactly what the multiplexer's hold retiring on a render rather
than on the controller's drop buys: without it the floor can pass `a`, the decline
branch fires, and the frontier goes through uncapped.
-/
theorem capped_le_of_hold {a floor t : Time} (hfa : floor ≤ a) :
    capped (some a) floor t ≤ a := by
  simp only [capped]
  by_cases hc : a < t ∧ floor ≤ a
  · rw [if_pos hc]
    exact Nat.le_refl a
  · rw [if_neg hc]
    exact Nat.not_lt.mp (fun h => hc ⟨h, hfa⟩)

/-- Every element of the list is at or below every element after it. -/
def Ascending : List Time → Prop
  | [] => True
  | t :: rest => (∀ u ∈ rest, t ≤ u) ∧ Ascending rest

theorem ascending_append {q : List Time} {t : Time}
    (hq : Ascending q) (hle : ∀ u ∈ q, u ≤ t) : Ascending (q ++ [t]) := by
  induction q with
  | nil => exact ⟨by simp, trivial⟩
  | cons a rest ih =>
    obtain ⟨hhead, htail⟩ := hq
    refine ⟨?_, ih htail (fun u hu => hle u (by simp [hu]))⟩
    intro u hu
    rcases List.mem_append.mp hu with h | h
    · exact hhead u h
    · simp at h
      exact h ▸ hle a (by simp)

/--
One protocol step.

`releaseOnDrop` selects the retired behaviour where the multiplexer drops its
hold when the controller enqueues the dataflow's drop. `physicalFromUpper`
selects the retired behaviour where the publisher forwards the stream `upper` as
its physical compaction target. Both are `false` in the implementation.
-/
inductive Step (releaseOnDrop physicalFromUpper : Bool) : State → State → Prop where
  /--
  The controller creates the interactive dataflow at `t`, taking a read hold
  there. The multiplexer records its hold BEFORE the command reaches interactive,
  which is the ordering everything else rests on.

  `since ≤ t` and `floor ≤ t` are the controller's own discipline: it only offers
  an `as_of` the index can still serve, and it never offers one below a frontier
  it has already allowed the index to compact to.
  -/
  | ctlCreate {s : State} {t : Time} :
      s.dfAsOf = none → s.since ≤ t → s.floor ≤ t →
      Step releaseOnDrop physicalFromUpper s
        { s with ctlHold := some t, dfAsOf := some t, muxHold := some t,
                 interQueue := s.interQueue ++ [t] }
  /--
  The controller allows compaction of the index to `t`, capped by the
  multiplexer. It only does this once its own view of the reader is finished,
  which is the point: the interactive runtime may not have started.
  -/
  | ctlCompact {s : State} {t : Time} :
      s.floor ≤ t → s.since ≤ t → (∀ c, s.ctlHold = some c → t ≤ c) →
      Step releaseOnDrop physicalFromUpper s
        { s with maintQueue := s.maintQueue ++ [capped s.muxHold s.floor t],
                 floor := capped s.muxHold s.floor t }
  /--
  The controller decides the dataflow is done and releases its own hold. In the
  real system this is an `AllowCompaction` to the empty frontier on the
  dataflow's export.

  Under `releaseOnDrop` the multiplexer's hold goes too, which is the defect:
  this is the controller's view of the dataflow's lifetime and says nothing about
  whether interactive has rendered.
  -/
  | ctlDrop {s : State} {a : Time} :
      s.dfAsOf = some a →
      Step releaseOnDrop physicalFromUpper s
        { s with ctlHold := none,
                 muxHold := if releaseOnDrop then none else s.muxHold }
  /--
  The multiplexer retires its hold, because interactive reported a frontier for
  the dataflow and therefore holds the shared trace itself.
  -/
  | muxRetire {s : State} :
      s.reported = true →
      Step releaseOnDrop physicalFromUpper s { s with muxHold := none }
  /--
  The multiplexer forwards compaction that a retired hold released.
  `Multiplexer::flush_pending_compaction`, including its refusal to regress the
  floor.
  -/
  | muxFlush {s : State} {t : Time} :
      s.muxHold = none → s.floor ≤ t →
      Step releaseOnDrop physicalFromUpper s
        { s with maintQueue := s.maintQueue ++ [t], floor := t }
  /--
  Maintenance processes its next compaction command. This is where compaction is
  realized, and where the publisher recomputes the physical target it forwards.
  -/
  | maintStep {s : State} {t : Time} {rest : List Time} :
      s.maintQueue = t :: rest →
      Step releaseOnDrop physicalFromUpper s
        { s with maintQueue := rest,
                 since := max s.since t,
                 physical := if physicalFromUpper then s.upper else max s.since t }
  /--
  Interactive processes its next create. Rendering is where the reader's hold on
  the shared trace becomes real, and it can be arbitrarily later than the create.
  -/
  | interStep {s : State} {t : Time} {rest : List Time} :
      s.interQueue = t :: rest →
      Step releaseOnDrop physicalFromUpper s
        { s with interQueue := rest, rendered := true }
  /-- Interactive reports a frontier for a dataflow it has rendered. -/
  | interReport {s : State} :
      s.rendered = true →
      Step releaseOnDrop physicalFromUpper s { s with reported := true }
  /-- The writer seals more input, advancing the stream frontier. -/
  | writerAdvance {s : State} {t : Time} :
      s.upper ≤ t →
      Step releaseOnDrop physicalFromUpper s { s with upper := t }

/-- States the protocol can reach from `init`. -/
inductive Reachable (releaseOnDrop physicalFromUpper : Bool) : State → Prop where
  | init : Reachable releaseOnDrop physicalFromUpper init
  | step {s s' : State} :
      Reachable releaseOnDrop physicalFromUpper s →
      Step releaseOnDrop physicalFromUpper s s' →
      Reachable releaseOnDrop physicalFromUpper s'

/--
The inductive invariant.

`held` is the load-bearing clause and the reason the proof goes through at all: it
says that while a created dataflow has not rendered, the multiplexer still holds
at its `as_of` AND the floor has not passed that `as_of`. The second half is what
rules out the "decline to cap" branch of `capped`, which would otherwise let a
compaction through uncapped.
-/
structure Inv (s : State) : Prop where
  /-- The published physical frontier never leads the published `since`. -/
  physLeSince : s.physical ≤ s.since
  /-- In-flight compaction commands are ordered. -/
  queueAsc : Ascending s.maintQueue
  /-- No in-flight command would regress what maintenance has applied. -/
  queueGeSince : ∀ t ∈ s.maintQueue, s.since ≤ t
  /-- No in-flight command exceeds the floor the multiplexer has recorded. -/
  queueLeFloor : ∀ t ∈ s.maintQueue, t ≤ s.floor
  /-- Maintenance never runs ahead of the recorded floor. -/
  sinceLeFloor : s.since ≤ s.floor
  /-- A frontier is only reported for a dataflow that rendered. -/
  reportedRendered : s.reported = true → s.rendered = true
  /-- While an unrendered dataflow exists, its hold is recorded and the floor
      respects it. -/
  held : ∀ a, s.dfAsOf = some a → s.rendered = false →
           s.muxHold = some a ∧ s.since ≤ a ∧ s.floor ≤ a

theorem inv_init : Inv init := by
  constructor
  · exact Nat.le_refl 0
  · trivial
  · intro t ht; simp [init] at ht
  · intro t ht; simp [init] at ht
  · exact Nat.le_refl 0
  · intro h; simp [init] at h
  · intro a ha; simp [init] at ha

/--
`Inv` is preserved by every step of the protocol as implemented.

Both booleans are `false` here. Weakening either one breaks a clause, which is
what the counterexamples below exhibit.
-/
theorem inv_step {s s' : State} (hinv : Inv s) (hstep : Step false false s s') : Inv s' := by
  obtain ⟨hphys, hasc, hge, hle, hsf, hrr, hheld⟩ := hinv
  cases hstep with
  | @ctlCreate t _ hsince hfloor =>
    refine ⟨hphys, hasc, hge, hle, hsf, hrr, ?_⟩
    intro a ha _
    have hat : a = t := by simpa using ha.symm
    subst hat
    exact ⟨rfl, hsince, hfloor⟩
  | @ctlCompact t hfloor hsince _ =>
    -- The cap never lands below the floor, so the queue stays ordered, and never
    -- above the recorded hold, so `held`'s floor bound survives.
    have hcap_ge_floor : s.floor ≤ capped s.muxHold s.floor t := le_capped hfloor
    have hcap_ge_since : s.since ≤ capped s.muxHold s.floor t :=
      Nat.le_trans hsf hcap_ge_floor
    refine ⟨hphys, ?_, ?_, ?_, hcap_ge_since, hrr, ?_⟩
    · exact ascending_append hasc (fun u hu => Nat.le_trans (hle u hu) hcap_ge_floor)
    · intro u hu
      rcases List.mem_append.mp hu with h | h
      · exact hge u h
      · have heq : u = capped s.muxHold s.floor t := by simpa using h
        subst heq; exact hcap_ge_since
    · intro u hu
      rcases List.mem_append.mp hu with h | h
      · exact Nat.le_trans (hle u h) hcap_ge_floor
      · have heq : u = capped s.muxHold s.floor t := by simpa using h
        subst heq; exact Nat.le_refl _
    · intro a ha hr
      obtain ⟨hmux, hsa, hfa⟩ := hheld a ha hr
      refine ⟨hmux, hsa, ?_⟩
      show capped s.muxHold s.floor t ≤ a
      rw [hmux]
      exact capped_le_of_hold hfa
  | ctlDrop _ =>
    -- `releaseOnDrop` is false, so the multiplexer's hold survives and `held`
    -- is untouched.
    exact ⟨hphys, hasc, hge, hle, hsf, hrr, hheld⟩
  | muxRetire hrep =>
    refine ⟨hphys, hasc, hge, hle, hsf, hrr, ?_⟩
    intro a ha hr
    -- A report implies a render, so an unrendered dataflow cannot have been
    -- reported, and this step cannot fire in a state where `held` has content.
    rw [hrr hrep] at hr
    exact absurd hr (by simp)
  | @muxFlush t hmux hfloor =>
    refine ⟨hphys, ?_, ?_, ?_, Nat.le_trans hsf hfloor, hrr, ?_⟩
    · exact ascending_append hasc (fun u hu => Nat.le_trans (hle u hu) hfloor)
    · intro u hu
      rcases List.mem_append.mp hu with h | h
      · exact hge u h
      · have heq : u = t := by simpa using h
        subst heq; exact Nat.le_trans hsf hfloor
    · intro u hu
      rcases List.mem_append.mp hu with h | h
      · exact Nat.le_trans (hle u h) hfloor
      · have heq : u = t := by simpa using h
        subst heq; exact Nat.le_refl _
    · intro a ha hr
      -- The flush requires no recorded hold, but `held` says there is one.
      obtain ⟨hmux', _, _⟩ := hheld a ha hr
      rw [hmux] at hmux'
      exact absurd hmux' (by simp)
  | @maintStep t rest hq =>
    have hmem : t ∈ s.maintQueue := by rw [hq]; simp
    have hasc' : Ascending s.maintQueue := hasc
    rw [hq] at hasc'
    obtain ⟨hhead, htail⟩ := hasc'
    have hmax : max s.since t = t := Nat.max_eq_right (hge t hmem)
    refine ⟨?_, ?_, ?_, ?_, ?_, hrr, ?_⟩
    · exact Nat.le_refl _
    · exact htail
    · intro u hu
      show max s.since t ≤ u
      rw [hmax]
      exact hhead u hu
    · intro u hu
      exact hle u (by rw [hq]; exact List.mem_cons_of_mem t hu)
    · show max s.since t ≤ s.floor
      rw [hmax]
      exact hle t hmem
    · intro a ha hr
      obtain ⟨hmux, _, hfa⟩ := hheld a ha hr
      refine ⟨hmux, ?_, hfa⟩
      show max s.since t ≤ a
      rw [hmax]
      exact Nat.le_trans (hle t hmem) hfa
  | @interStep t rest hq =>
    refine ⟨hphys, ?_, hge, hle, hsf, ?_, ?_⟩
    · have hasc' : Ascending s.maintQueue := hasc
      exact hasc'
    · intro _; rfl
    · -- The dataflow has rendered, so `held` has nothing left to say.
      intro a _ hr
      exact absurd hr (by simp)
  | interReport hrend =>
    exact ⟨hphys, hasc, hge, hle, hsf, fun _ => hrend, hheld⟩
  | writerAdvance _ =>
    exact ⟨hphys, hasc, hge, hle, hsf, hrr, hheld⟩

theorem inv_reachable {s : State} (h : Reachable false false s) : Inv s := by
  induction h with
  | init => exact inv_init
  | step _ hstep ih => exact inv_step ih hstep

/--
The protocol invariant the runtime split would otherwise lose: an index is never
compacted past the `as_of` of a created dataflow that has not yet rendered.

This is the property whose four-step counterexample the earlier implementation
admitted, see `release_on_drop_violates_invariant`.
-/
theorem since_le_as_of {s : State} {a : Time}
    (h : Reachable false false s) (hdf : s.dfAsOf = some a) (hr : s.rendered = false) :
    s.since ≤ a :=
  ((inv_reachable h).held a hdf hr).2.1

/--
The publisher never forwards a physical compaction frontier beyond the published
`since`.

`set_physical_compaction F` promises the trace stays readable at every frontier at
or beyond `F`. Every `as_of` the controller may offer is at or beyond `since`, so
a physical frontier at or below `since` is what makes a reader's cut available
without the reader having to synchronize with the publishing worker.
-/
theorem physical_le_since {s : State} (h : Reachable false false s) : s.physical ≤ s.since :=
  (inv_reachable h).physLeSince

/-- A compaction frontier in flight to maintenance never regresses what
maintenance has already applied. -/
theorem no_regression {s : State} (h : Reachable false false s) :
    ∀ t ∈ s.maintQueue, s.since ≤ t :=
  (inv_reachable h).queueGeSince

/-!
## The retired behaviours, refuted

A model that only expresses the fixed protocol cannot tell you the protocol was
ever broken. Each counterexample below is a concrete reachable state under one of
the retired rules.
-/

/-- Releasing the multiplexer's hold when the controller drops the dataflow
admits a state where the index has compacted past an unrendered dataflow's
`as_of`. Four steps: create at 2, drop, allow compaction to 3, apply it. -/
theorem release_on_drop_violates_invariant :
    ∃ s : State, Reachable true false s ∧
      s.dfAsOf = some 2 ∧ s.rendered = false ∧ ¬ (s.since ≤ 2) := by
  -- Each `have` names the state the previous step produced, so the final
  -- assertions are decided against a fully concrete state.
  have h1 := (Reachable.init (releaseOnDrop := true) (physicalFromUpper := false)).step
    (Step.ctlCreate (t := 2) rfl (by decide) (by decide))
  have h2 := h1.step (Step.ctlDrop (a := 2) rfl)
  have h3 := h2.step (Step.ctlCompact (t := 3) (by decide) (by decide) (by simp))
  have h4 := h3.step (Step.maintStep (t := 3) (rest := []) rfl)
  exact ⟨_, h4, rfl, rfl, by decide⟩

/-- Forwarding the stream `upper` as the physical compaction target admits a
state whose physical frontier leads its `since`, so a reader's cut at a legal
`as_of` is no longer guaranteed. -/
theorem physical_from_upper_violates_invariant :
    ∃ s : State, Reachable false true s ∧ ¬ (s.physical ≤ s.since) := by
  have h1 := (Reachable.init (releaseOnDrop := false) (physicalFromUpper := true)).step
    (Step.writerAdvance (t := 5) (by decide))
  have h2 := h1.step (Step.ctlCompact (t := 0) (by decide) (by decide) (by simp))
  have h3 := h2.step (Step.maintStep (t := 0) (rest := []) rfl)
  exact ⟨_, h3, by decide⟩

end Protocol

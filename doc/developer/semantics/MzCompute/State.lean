-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.
--
-- The protocol state machine: `ProtocolState` plus the `step`
-- relation whose guards encode `command.rs`/`response.rs`'s "invalid
-- to" / "must" contracts. A `none` result means the doc comments call
-- the transition undefined behavior. The model stops there rather
-- than inventing recovery semantics the protocol does not specify.

import MzCompute.Basic
import MzCompute.Event

namespace MzCompute

/-- Everything `step`'s guards depend on. `created`/`scheduled`/
`allowedWrites`/`dropped` are total functions `GlobalId → Bool`
rather than a set or map type, so the model needs no map/set library
(see Global Constraints in the implementation plan: no Mathlib). -/
structure ProtocolState where
  stage : Stage := .creation
  helloSeen : Bool := false
  createInstanceSeen : Bool := false
  /-- Ids exported by a `CreateDataflow` seen so far. -/
  created : GlobalId → Bool := fun _ => false
  scheduled : GlobalId → Bool := fun _ => false
  allowedWrites : GlobalId → Bool := fun _ => false
  /-- Whether `id` has been allowed to compact to the empty frontier,
  the canonical "drop" per `command.rs`'s `AllowCompaction` doc. -/
  dropped : GlobalId → Bool := fun _ => false
  /-- Last reported value per id and frontier kind. `.at 0` is both
  the real value `0` and the "nothing reported yet" sentinel. See
  `Frontier.advances`'s doc comment for why that is sound. -/
  reportedFrontiers : GlobalId → FrontierKind → Frontier := fun _ _ => .at 0
  peeks : Uuid → PeekState := fun _ => .notSeen

/-- `true` iff every id in `exports` is currently un-created and the
list has no internal duplicates, matching `command.rs`'s "dataflow
exports have unique IDs... do not repeat". Written by hand recursion
rather than `List.Nodup`/`List.Pairwise` (Mathlib-only in the relevant
form. See Global Constraints). -/
def freshExports (created : GlobalId → Bool) : List GlobalId → Bool
  | [] => true
  | id :: rest => !created id && !rest.elem id && freshExports created rest

/-- The protocol step relation. Each `ComputeCommand`/`ComputeResponse`
variant gets one equation. The comment above each cites the
`command.rs`/`response.rs` sentence its guard encodes. -/
def step (s : ProtocolState) : Event → Option ProtocolState
  -- protocol.rs: Hello is the first command of the Creation stage.
  | .cmd (.hello _) =>
      guard (s.stage.isCreation && !s.helloSeen)
        { s with helloSeen := true }
  -- protocol.rs: CreateInstance completes the Creation stage and
  -- begins Initialization.
  | .cmd .createInstance =>
      guard (s.stage.isCreation && s.helloSeen && !s.createInstanceSeen)
        { s with createInstanceSeen := true, stage := .initialization }
  -- protocol.rs: InitializationComplete marks the end of the
  -- Initialization stage.
  | .cmd .initializationComplete =>
      guard (match s.stage with | .initialization => true | _ => false)
        { s with stage := .computation }
  -- protocol.rs: UpdateConfiguration is a computation-stage command.
  | .cmd .updateConfiguration =>
      guard (!s.stage.isCreation) s
  -- command.rs:107-108: dataflow exports have unique IDs and do not
  -- repeat.
  | .cmd (.createDataflow exports) =>
      guard (!s.stage.isCreation && freshExports s.created exports)
        { s with created := fun id => s.created id || exports.elem id }
  -- command.rs:148-153: Schedule requires a prior CreateDataflow and
  -- is invalid once the collection has been allowed to compact to
  -- the empty frontier.
  | .cmd (.schedule id) =>
      guard (!s.stage.isCreation && s.created id && !s.dropped id)
        { s with scheduled := fun id' => id' == id || s.scheduled id' }
  -- command.rs:158-163: AllowWrites requires a prior CreateDataflow
  -- and is invalid once the collection has been dropped.
  | .cmd (.allowWrites id) =>
      guard (!s.stage.isCreation && s.created id && !s.dropped id)
        { s with allowedWrites := fun id' => id' == id || s.allowedWrites id' }
  -- command.rs:185-187, :200-201: AllowCompaction requires a prior
  -- CreateDataflow. An empty frontier is the canonical way to drop a
  -- collection.
  | .cmd (.allowCompaction id frontier) =>
      let isEmpty := match frontier with | .empty => true | .at _ => false
      guard (!s.stage.isCreation && s.created id)
        { s with dropped := fun id' => s.dropped id' || (id' == id && isEmpty) }
  -- command.rs:219-224: an index-target Peek requires a prior
  -- CreateDataflow for the target. Persist-target peeks do not.
  -- command.rs:223-224: the peek's uuid must be unique.
  | .cmd (.peek uuid target) =>
      guard (!s.stage.isCreation && (s.peeks uuid).isNotSeen
             && (match target with | .index id => s.created id | .persist _ => true))
        { s with peeks := fun u => if u == uuid then .pending else s.peeks u }
  -- command.rs:248-250: CancelPeek requires a prior matching Peek.
  | .cmd (.cancelPeek uuid) =>
      guard (!s.stage.isCreation && !(s.peeks uuid).isNotSeen) s
  -- response.rs:37-52: Frontiers reports must never regress, and
  -- response.rs:52-54 requires a prior CreateDataflow.
  | .resp (.frontiers id write? input? output?) =>
      guard (!s.stage.isCreation && s.created id
             && checkAdvance write? (s.reportedFrontiers id .write)
             && checkAdvance input? (s.reportedFrontiers id .input)
             && checkAdvance output? (s.reportedFrontiers id .output))
        { s with reportedFrontiers := fun id' k =>
            if id' == id then
              match k with
              | .write => applyAdvance write? (s.reportedFrontiers id .write)
              | .input => applyAdvance input? (s.reportedFrontiers id .input)
              | .output => applyAdvance output? (s.reportedFrontiers id .output)
            else s.reportedFrontiers id' k }
  -- response.rs:66: exactly one PeekResponse per Peek, so this
  -- requires the peek to currently be pending.
  | .resp (.peekResponse uuid) =>
      guard (!s.stage.isCreation && (s.peeks uuid).isPending)
        { s with peeks := fun u => if u == uuid then .answered else s.peeks u }
  -- response.rs:106-107: SubscribeResponse requires a prior
  -- CreateDataflow.
  | .resp (.subscribeResponse id) =>
      guard (!s.stage.isCreation && s.created id) s
  -- response.rs:120-121: CopyToResponse requires a prior
  -- CreateDataflow.
  | .resp (.copyToResponse id) =>
      guard (!s.stage.isCreation && s.created id) s
  -- response.rs:130-131: Status has no effect on collection
  -- lifecycles and carries no GlobalId in the current protocol.
  | .resp .status =>
      some s

end MzCompute

namespace MzCompute

-- Hello is legal from the initial state.
example : (step {} (.cmd (.hello 0))).isSome = true := by decide
-- CreateInstance is illegal before Hello.
example : step {} (.cmd .createInstance) = none := by decide
-- Schedule is illegal before any CreateDataflow.
example : step { stage := .computation, createInstanceSeen := true, helloSeen := true }
    (.cmd (.schedule 7)) = none := by decide
-- Schedule is legal once id 7 has been created.
example : (step { stage := .computation, createInstanceSeen := true, helloSeen := true, created := fun id => id == 7 }
    (.cmd (.schedule 7))).isSome = true := by decide
-- Schedule is illegal once id 7 has been dropped.
example : step { stage := .computation, createInstanceSeen := true, helloSeen := true, created := fun id => id == 7, dropped := fun id => id == 7 }
    (.cmd (.schedule 7)) = none := by decide
-- CreateDataflow rejects a duplicate id within the same export list.
example : step { stage := .computation, createInstanceSeen := true, helloSeen := true }
    (.cmd (.createDataflow [7, 7])) = none := by decide
-- A second PeekResponse for an already-answered uuid is illegal.
example : step { stage := .computation, createInstanceSeen := true, helloSeen := true, peeks := fun u => if u == 0 then .answered else .notSeen }
    (.resp (.peekResponse 0)) = none := by decide

end MzCompute

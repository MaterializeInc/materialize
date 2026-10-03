-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.
--
-- Core types shared by the compute protocol model: identifiers,
-- frontiers, and the small enums `step` (see `State.lean`) dispatches
-- on.

namespace MzCompute

/-- Compute-collection identifier. Only equality matters for
protocol-level legality, so a bare `Nat` stands in for the real
`mz_repr::GlobalId`. -/
abbrev GlobalId := Nat

/-- Peek request identifier, standing in for `uuid::Uuid`. Also reused
for `Hello`'s nonce, which the model does not otherwise constrain
(see the design doc's open question on reconnection). -/
abbrev Uuid := Nat

/-- A single-dimensional frontier over `mz_repr::Timestamp`. Because
`Timestamp` is totally ordered, an antichain over it has at most one
element: `.empty` models the empty antichain (the terminal frontier,
nothing more will ever be written), `.at t` models the singleton
antichain `{t}`. -/
inductive Frontier where
  | at (t : Nat)
  | empty
  deriving Repr

/-- `Frontier.advances new old` holds (as `true`) exactly when `new`
is at least as advanced as `old` (`new ⊒ old` in `response.rs`'s
terms). `.empty` is the top element: nothing exceeds it, and only
`.empty` can follow it. `.at 0` doubles as the "nothing reported yet"
sentinel (see `State.lean`), which is sound because `Nat`'s minimum
value is `0`: any real first report `.at t` satisfies `t ≥ 0`. -/
def Frontier.advances (new old : Frontier) : Bool :=
  match old, new with
  | .empty, .empty => true
  | .empty, .at _ => false
  | .at _, .empty => true
  | .at o, .at n => Nat.ble o n

/-- The three frontier kinds a `Frontiers` response can report, per
`response.rs`'s `FrontiersResponse`. -/
inductive FrontierKind where
  | write | input | output
  deriving Repr

/-- The three protocol stages from `protocol.rs`'s module doc. -/
inductive Stage where
  | creation | initialization | computation
  deriving Repr

def Stage.isCreation : Stage → Bool
  | .creation => true
  | _ => false

/-- Lifecycle of a single `Peek` (identified by its `uuid`), per the
1:1 `Peek`/`PeekResponse` contract in `command.rs`/`response.rs`. -/
inductive PeekState where
  | notSeen | pending | answered
  deriving Repr

def PeekState.isNotSeen : PeekState → Bool
  | .notSeen => true
  | _ => false

def PeekState.isPending : PeekState → Bool
  | .pending => true
  | _ => false

def PeekState.isAnswered : PeekState → Bool
  | .answered => true
  | _ => false

/-- A `step` arm's precondition check: `some s` if `cond` holds, else
`none`. A `none` result models the doc comments' "undefined behavior"
clause: the model has nothing further to say, rather than inventing
recovery behavior the protocol does not specify. -/
def guard (cond : Bool) (s : α) : Option α :=
  if cond then some s else none

/-- A successful `guard` returns its payload unchanged. -/
theorem guard_eq_some {α : Type _} {cond : Bool} {x y : α}
    (h : guard cond x = some y) : x = y := by
  unfold guard at h
  cases cond with
  | true => simpa using h
  | false => simp at h

/-- A successful `guard` witnesses its condition held. -/
theorem guard_cond {α : Type _} {cond : Bool} {x y : α}
    (h : guard cond x = some y) : cond = true := by
  unfold guard at h
  cases cond with
  | true => rfl
  | false => simp at h

/-- Whether an optional new frontier report (`none` = this kind was
not reported this time) is compatible with the previously reported
value. `none` is always compatible: an absent field means "unchanged",
per `FrontiersResponse`'s doc comment. -/
def checkAdvance (new? : Option Frontier) (old : Frontier) : Bool :=
  match new? with
  | none => true
  | some new => Frontier.advances new old

/-- Applies an optional new frontier report, keeping the old value
when the report is absent. -/
def applyAdvance (new? : Option Frontier) (old : Frontier) : Frontier :=
  match new? with
  | none => old
  | some new => new

end MzCompute

namespace MzCompute

example : Frontier.advances (.at 5) (.at 3) = true := by rfl
example : Frontier.advances (.at 2) (.at 3) = false := by rfl
example : Frontier.advances .empty (.at 3) = true := by rfl
example : Frontier.advances (.at 3) .empty = false := by rfl
example : Frontier.advances .empty .empty = true := by rfl
example : checkAdvance none (.at 3) = true := by rfl
example : checkAdvance (some (.at 2)) (.at 3) = false := by rfl
example : applyAdvance none (.at 3) = .at 3 := by rfl
example : applyAdvance (some (.at 5)) (.at 3) = .at 5 := by rfl

end MzCompute

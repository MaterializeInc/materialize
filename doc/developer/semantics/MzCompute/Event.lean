-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.
--
-- Inductive mirror of `ComputeCommand` (command.rs) and
-- `ComputeResponse` (response.rs). Payload fields with no bearing on
-- protocol-level legality (`RelationDesc`, `Row`, `DataflowDescription`'s
-- internals, `RowSetFinishing`, and similar) are left out entirely
-- rather than modeled as an opaque placeholder: nothing in this
-- model's theorems needs them, so there is nothing to erase. See
-- doc/developer/design/20260724_compute_protocol_semantics.md's
-- "State and events" section for the opaque/concrete boundary this
-- follows.

import MzCompute.Basic

namespace MzCompute

/-- `Peek`'s target, per `command.rs`'s `PeekTarget`. Only `.index`
peeks are required to reference a previously created collection (see
theorem 2 in `Staging.lean`). `.persist` peeks target a storage
collection this model does not track. -/
inductive PeekTarget where
  | index (id : GlobalId)
  | persist (id : GlobalId)
  deriving Repr

/-- Mirrors `command.rs`'s `ComputeCommand`. -/
inductive ComputeCommand where
  | hello (nonce : Uuid)
  | createInstance
  | initializationComplete
  | updateConfiguration
  | createDataflow (exports : List GlobalId)
  | schedule (id : GlobalId)
  | allowWrites (id : GlobalId)
  | allowCompaction (id : GlobalId) (frontier : Frontier)
  | peek (uuid : Uuid) (target : PeekTarget)
  | cancelPeek (uuid : Uuid)
  deriving Repr

/-- Mirrors `response.rs`'s `ComputeResponse`. `frontiers` carries the
three optional per-kind reports from `FrontiersResponse` directly
(write, input, output), rather than one event per kind, matching the
real wire response. -/
inductive ComputeResponse where
  | frontiers (id : GlobalId) (write input output : Option Frontier)
  | peekResponse (uuid : Uuid)
  | subscribeResponse (id : GlobalId)
  | copyToResponse (id : GlobalId)
  | status
  deriving Repr

inductive Event where
  | cmd (c : ComputeCommand)
  | resp (r : ComputeResponse)
  deriving Repr

def Event.isHello : Event → Bool
  | .cmd (.hello _) => true
  | _ => false

def Event.isCreateInstance : Event → Bool
  | .cmd .createInstance => true
  | _ => false

/-- Whether `e` is one of the event kinds `command.rs`/`response.rs`
require a prior `CreateDataflow` for `id`: `Schedule`, `AllowWrites`,
`AllowCompaction`, `Peek` on an index target, `Frontiers`,
`SubscribeResponse`, `CopyToResponse`. `Status` is excluded: it
carries no `GlobalId` in the current protocol (see theorem 2's doc
comment in `Staging.lean`). `CreateDataflow` itself is excluded: it is
the event that satisfies the requirement, not one that needs it. -/
def Event.references : Event → GlobalId → Bool
  | .cmd (.schedule id'), id => id' == id
  | .cmd (.allowWrites id'), id => id' == id
  | .cmd (.allowCompaction id' _), id => id' == id
  | .cmd (.peek _ (.index id')), id => id' == id
  | .resp (.frontiers id' _ _ _), id => id' == id
  | .resp (.subscribeResponse id'), id => id' == id
  | .resp (.copyToResponse id'), id => id' == id
  | _, _ => false

end MzCompute

namespace MzCompute

example : Event.isHello (.cmd (.hello 0)) = true := by rfl
example : Event.isHello (.cmd .createInstance) = false := by rfl
example : Event.isCreateInstance (.cmd .createInstance) = true := by rfl
example : Event.references (.cmd (.schedule 7)) 7 = true := by rfl
example : Event.references (.cmd (.schedule 7)) 8 = false := by rfl
example : Event.references (.resp .status) 7 = false := by rfl
example : Event.references (.cmd (.createDataflow [7])) 7 = false := by rfl
example : Event.references (.cmd (.peek 0 (.persist 7))) 7 = false := by rfl
example : Event.references (.cmd (.peek 0 (.index 7))) 7 = true := by rfl

end MzCompute

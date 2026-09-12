-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.
--
-- Folding `step` over a list of events into an execution, and the
-- well-formedness predicate the plan's theorems are stated over.

import MzCompute.State

namespace MzCompute

def initState : ProtocolState := {}

/-- Folds `step` over `es` starting from `s`, short-circuiting to
`none` at the first illegal event. -/
def run (s : ProtocolState) : List Event → Option ProtocolState
  | [] => some s
  | e :: rest =>
      match step s e with
      | none => none
      | some s' => run s' rest

/-- An execution is well-formed iff folding `step` from `initState`
never hits an illegal event. -/
def WellFormed (es : List Event) : Prop := ∃ s, run initState es = some s

end MzCompute

namespace MzCompute

/-- A minimal legal trace: handshake, create a dataflow exporting id
`1`, schedule it, report its write frontier advancing to `5`, peek it,
answer the peek, then drop it. -/
def legalTrace : List Event :=
  [ .cmd (.hello 0)
  , .cmd .createInstance
  , .cmd .initializationComplete
  , .cmd (.createDataflow [1])
  , .cmd (.schedule 1)
  , .resp (.frontiers 1 (some (.at 5)) none none)
  , .cmd (.peek 10 (.index 1))
  , .resp (.peekResponse 10)
  , .cmd (.allowCompaction 1 .empty)
  ]

example : WellFormed legalTrace := by
  refine ⟨?_, ?_⟩
  · exact (run initState legalTrace).get (by decide)
  · rfl

/-- Same trace, but with the `Schedule` moved before `CreateDataflow`:
illegal per command.rs:148-150. -/
def illegalTrace : List Event :=
  [ .cmd (.hello 0)
  , .cmd .createInstance
  , .cmd .initializationComplete
  , .cmd (.schedule 1)
  , .cmd (.createDataflow [1])
  ]

example : ¬ WellFormed illegalTrace := by
  rintro ⟨s, hs⟩
  rw [show run initState illegalTrace = none from rfl] at hs
  cases hs

end MzCompute

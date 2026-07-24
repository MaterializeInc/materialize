# Compute protocol semantics (Lean 4 model), Phase 1: staging and command/response legality

- Associated: MaterializeInc/materialize#36614 (Lean 4 mechanization precedent, error-handling semantics)

## The problem

The compute protocol (`src/compute-client/src/protocol/{command,response,history}.rs`)
encodes several non-obvious invariants only in prose: a three-stage handshake,
per-command legality preconditions tied to `GlobalId`s, frontier monotonicity
and terminality for responses, and a 1:1 contract between `Peek` and
`PeekResponse`. These invariants are load-bearing (violating them causes
replica undefined behavior per the doc comments) but are currently checked
only by code review and by whatever the implementation happens to enforce.
There is no artifact that states the invariants independently of the
implementation, so it is hard to know whether the invariants are mutually
consistent, whether a proposed protocol change preserves them, or whether the
prose is complete.

There is no open GitHub issue tracking this work. It originated from a
conversation about whether the protocol's invariants could be captured in a
semantic model and checked with a proof assistant, following the precedent
set by #36614's Lean 4 mechanization of the error-handling design.

This doc proposes a Lean 4 model of the protocol's control-plane contract: a
state machine plus a small set of safety theorems that codifies the doc
comments as checked mathematics. The goal is to verify the model's internal
consistency (the invariants do not contradict each other, a well-formed
execution cannot get stuck), not to verify the Rust implementation against
the model.

This is Phase 1 of a three-phase layered plan. Phase 2 (read-hold lifecycle
against `AllowCompaction`) and Phase 3 (command sharing and response
reconciliation across replicas and workers) extend Phase 1's state machine
rather than starting fresh; they are not covered by this doc.

## Success criteria

- A Lean 4 project (a new `lean_lib` in the existing `doc/developer/semantics/`
  package) defines `ProtocolState`, `Event` (mirroring `ComputeCommand` /
  `ComputeResponse`), and a `step : ProtocolState → Event → Option
  ProtocolState` relation whose preconditions match the "invalid to" / "must"
  statements in `command.rs`, `response.rs`, and `protocol.rs`.
- At least the five theorems listed under Solution Proposal are stated and
  proved with no `sorry`, each traceable to an explicit doc-comment contract.
- The model builds in CI via the existing `ci/test/lean-semantics.sh` harness.
- A reader can go from a doc-comment sentence in `command.rs` / `response.rs`
  to the Lean lemma that encodes it, and back.

## Out of scope

- Payload semantics: the contents of `Row`, `DataflowDescription`,
  `RelationDesc`, and similar types are opaque and uninterpreted in the
  model. Phase 1 is about control flow and identifiers, not data.
- Read-hold / `since` correctness against `AllowCompaction`. That is a
  controller-side accounting property (Phase 2), not a protocol-legality
  property. Phase 1 only models what the wire contract says about
  `AllowCompaction`, which is silent on monotonicity of the frontier the
  controller chooses to send.
- Multi-replica and multi-worker command sharing and response demuxing
  (Phase 3).
- Liveness ("must eventually respond", "must eventually report empty
  frontiers"). Phase 1 proves safety only: what must hold if an event
  occurs, not that a required event eventually occurs.
- Wire transport (bincode / CTP encoding, delivery scheduling). In-order,
  reliable delivery is a hypothesis of the model, not something proved.
- Verifying the Rust implementation against the model. Out of scope for the
  whole layered project, not only Phase 1.

## Solution proposal

Model the protocol as a single event stream folded through one
state-transition relation, with safety properties stated as invariants
preserved by that fold. This is the same technique #36614 uses for
`Collection.refines` / `NoRowErr`: define state and step, prove step
preserves the invariant, induct over the execution.

### State and events

`ProtocolState` tracks exactly what the doc comments make legality depend
on:

- `created : Set GlobalId`, ids exported by a `CreateDataflow` seen so far.
- `scheduled`, `allowedWrites : Set GlobalId`.
- `since : GlobalId → Option (Antichain Timestamp)`, most recent
  `AllowCompaction` frontier per id.
- `reportedFrontiers : GlobalId → FrontierKind → Option (Antichain
  Timestamp)`, last `Frontiers` response per id per kind (write / input /
  output), used to check monotonicity and terminality of future responses.
- `peeks : Uuid → PeekState` where `PeekState := Pending | Answered`.
- `stage : Stage` where `Stage := Creation | Initialization | Computation`,
  plus `helloSeen` / `createInstanceSeen` flags to pin the first two events.

`Event := Cmd ComputeCommand | Resp ComputeResponse`, an inductive mirror of
the two real Rust enums. Fields with no bearing on protocol-level legality
(`RelationDesc`, `Row`, `DataflowDescription`'s internals, `RowSetFinishing`,
and similar) are represented by an opaque placeholder type rather than
reconstructed field for field. Fields legality does depend on (`GlobalId`,
`Uuid`, `Antichain Timestamp`, the `PeekTarget`'s id) are kept concrete.

### Step relation

`step : ProtocolState → Event → Option ProtocolState`. Each command or
response variant gets one equation. Example shapes, illustrative rather than
final Lean syntax:

- `step s (Cmd (Schedule id)) = if id ∈ s.created then some {s with scheduled := s.scheduled.insert id} else none`
- `step s (Resp (Frontiers id fr)) = if id ∈ s.created ∧ frontierAdvances s id fr then some (s.withReportedFrontiers id fr) else none`
- `step s (Cmd (Peek p)) = if p.uuid ∉ s.peeks.keys then some {s with peeks := s.peeks.insert p.uuid Pending} else none`
- `step s (Resp (PeekResponse uuid _ _)) = if s.peeks.find? uuid = some Pending then some {s with peeks := s.peeks.insert uuid Answered} else none`

A `none` result models the "may cause undefined behavior" clause: the model
has nothing further to say about what happens next, matching the doc's
contract rather than inventing recovery behavior the real protocol does not
specify.

An execution is `List Event`, well-formed iff folding `step` from an initial
empty state never produces `none`.

### Target theorems

1. `hello_create_instance_first`: in any well-formed execution, the first
   event is `Cmd (Hello _)` and the second is `Cmd (CreateInstance _)`.
2. `create_before_reference`: any event that names `id` other than the
   `CreateDataflow` that creates it (Schedule, AllowWrites, AllowCompaction,
   Peek on an index target, Frontiers, SubscribeResponse, CopyToResponse,
   Status) occurs strictly after `id`'s `CreateDataflow` in the execution.
3. `frontiers_monotone_and_terminal`: for each id and frontier kind, the
   sequence of reported frontiers in a well-formed execution is
   non-decreasing, and once a kind reports the empty frontier for an id, no
   later event reports a non-empty value for that (id, kind).
4. `peek_response_unique`: for each `Uuid`, a well-formed execution contains
   at most one `PeekResponse` for it, and if present, it is preceded by
   exactly one `Peek` command with the same uuid.
5. `cancel_requires_peek`: `CancelPeek uuid` is only legal after a `Peek`
   with matching uuid.

Each theorem's statement should be traceable to the specific doc-comment
sentence it encodes, linked from the Lean docstring rather than narrated in
the proof.

### Project structure

Add a `MzCompute` `lean_lib` target to the existing
`doc/developer/semantics/lakefile.toml`, alongside the current `Mz` target.
This reuses the Docker image, pinned Mathlib version, and
`ci/test/lean-semantics.sh` harness, which are expensive to duplicate, and
later phases may want `Finset` / `BTreeMap`-style reasoning for the
multi-replica layer. It is kept as a separate library and namespace rather
than folded into `Mz`, since `Mz`'s existing docstring scopes it to scalar
evaluation and collection semantics, a different subsystem (data plane, not
control plane). New `MzCompute/` module directory, own root import file, own
`compute-protocol.md` reading-order doc parallel to the existing `model.md`
and `transforms.md`, with `README.md` updated to list both subsystems and
their relationship.

## Minimal viable prototype

A single Lean module defining `ProtocolState` / `Event` / `step` and proving
`hello_create_instance_first` and `create_before_reference`, the two
simplest and most structural theorems, is the MVP. It validates the
state/step shape before investing in the frontier-monotonicity and
peek-uniqueness proofs, which touch more of the state and are more likely to
reveal that the chosen state shape is wrong.

## Alternatives

**Fully integrated single model (stages, read holds, and replica fan-out
together).** Considered and rejected for Phase 1: it would catch
cross-cutting invariants at subsystem boundaries, but the combined state
space is large before anything is proved, and it front-loads the exact
complexity a layered approach exists to avoid. Worth revisiting once Phases
1 through 3 exist independently, as a possible Phase 4 "boundary" pass.

**Fully separate models per phase, no shared kernel.** Rejected: each phase
would carry its own simplified assumptions about the other phases, and the
subtle bugs this protocol actually has tend to live at exactly those
boundaries (for example, a race between `AllowCompaction` and an in-flight
`Peek`). The layered approach instead has Phase 2 and Phase 3 extend Phase
1's `ProtocolState` / `Event` / `step`, so cross-phase reasoning stays
possible without Phase 1 paying Phase 3's cost.

**Trace well-formedness predicate instead of a state machine.** An
alternative shape defines well-formedness directly as a predicate over
`List Event` (for example, "id's `CreateDataflow` appears before any other
event naming id") rather than through a `step` relation. Rejected because it
does not compose across phases the same way: Phase 2's read-hold invariants
need a running `since` per id to state "since never exceeds a live hold's
frontier", which needs state that accumulates across the fold, not a
predicate over the whole list. The `step`-relation shape generalizes to that
need; the trace-predicate shape would need to be rebuilt for Phase 2 anyway.

**New standalone Lean project instead of a second `lean_lib` in the existing
package.** Rejected for now: it duplicates the Docker image, Mathlib pin,
and CI harness for no isolation benefit `MzCompute` does not already get
from being a separate library target. Worth reconsidering if the two
subsystems' dependency needs diverge enough to matter, for instance if
`MzCompute` never needs Mathlib and its presence meaningfully slows
iteration.

## Open questions

- Does "protocol iteration" in the doc comments (peek uuid uniqueness,
  `Hello` nonce uniqueness) mean one CTP connection's lifetime, or does the
  model need to handle reconnection (a second `Hello` after a connection
  drop) within Phase 1? The current proposal assumes a single connection per
  execution and defers reconnection modeling to a later phase; confirm this
  does not undercut the Phase 1 theorems' relevance to the real,
  reconnecting system.
- `UpdateConfiguration` and `Hello` are broadcast to all workers rather than
  routed through the first worker only. Phase 1 treats the protocol as a
  single logical stream (one controller, one logical receiver) and defers
  the worker-fan-out distinction entirely to Phase 3. Confirm the Phase 1
  theorems remain meaningful stated over the logical single-stream view.
- Should `frontiers_monotone_and_terminal` also state the doc's stronger
  requirement that the first reported frontier of any kind is not less than
  the dataflow's initial `as_of`? That requires `CreateDataflow`'s `as_of`
  to be tracked in state, currently left opaque. Left for the Phase 1
  implementation to decide once the state shape is in hand.

# Compute protocol semantics (Lean 4 model), Phase 1: staging and command/response legality

- Associated: MaterializeInc/materialize#36614 (Lean 4 mechanization precedent, error-handling semantics. Unmerged, referenced as prior art only, not a dependency)

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
rather than starting fresh. They are not covered by this doc.

## Success criteria

- A Lean 4 project (a new `lean_lib` in a `doc/developer/semantics/` package,
  see Project Structure) defines `ProtocolState`, `Event` (mirroring
  `ComputeCommand` / `ComputeResponse`), and a `step : ProtocolState → Event →
  Option ProtocolState` relation whose preconditions match the "invalid to" /
  "must" statements in `command.rs`, `response.rs`, and `protocol.rs`.
- At least the seven theorems listed under Solution Proposal are stated and
  proved with no `sorry`, each traceable to an explicit doc-comment contract.
- The model builds in CI via a `ci/test/lean-semantics.sh` harness, bootstrapped
  as part of this phase (see Project Structure). This does not depend on
  #36614 landing first.
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
  `CreateDataflow`'s command payload keeps its export id set (`Finset
  GlobalId`) concrete even though the rest of `DataflowDescription` is
  opaque, since export-id freshness is itself a Phase 1 legality theorem
  (see Target theorems).
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

Opaquing a field erases whatever structural facts it carries, not only its
data content. For example `Peek::literal_constraints`'s documented "the
vector is never empty" becomes unstatable once that field is opaque. This is
an accepted Phase 1 cut, not an oversight: only the seven theorems below are
committed deliverables, and structural payload facts unrelated to them are
left for a later phase to pick up if they turn out to matter.

### Step relation

`step : ProtocolState → Event → Option ProtocolState`. Each command or
response variant gets one equation. Example shapes, illustrative rather than
final Lean syntax:

- `step s (Cmd (CreateDataflow exports _)) = if exports ∩ s.created = ∅ ∧ exports.Nodup then some {s with created := s.created ∪ exports} else none`
- `step s (Cmd (Schedule id)) = if id ∈ s.created ∧ s.since id ≠ some ∅ then some {s with scheduled := s.scheduled.insert id} else none`
- `step s (Cmd (AllowWrites id)) = if id ∈ s.created ∧ s.since id ≠ some ∅ then some {s with allowedWrites := s.allowedWrites.insert id} else none`
- `step s (Resp (Frontiers id fr)) = if id ∈ s.created ∧ frontierAdvances s id fr then some (s.withReportedFrontiers id fr) else none`
- `step s (Cmd (Peek p)) = if p.uuid ∉ s.peeks.keys then some {s with peeks := s.peeks.insert p.uuid Pending} else none`
- `step s (Resp (PeekResponse uuid _ _)) = if s.peeks.find? uuid = some Pending then some {s with peeks := s.peeks.insert uuid Answered} else none`

`Schedule` and `AllowWrites` both check `s.since id ≠ some ∅`: `command.rs`
states it is invalid to send either once the collection has been allowed to
compact to the empty frontier (the canonical drop). `CreateDataflow` checks
its export ids are pairwise distinct (`exports.Nodup`) and disjoint from
everything already created, mirroring `command.rs`'s "dataflow exports have
unique IDs... do not repeat (within a single protocol iteration)".

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
   Peek on an index target, Frontiers, SubscribeResponse, CopyToResponse)
   occurs strictly after `id`'s `CreateDataflow` in the execution. `Status`
   is excluded: `ComputeResponse::Status` carries no `GlobalId` in the
   current protocol (`StatusResponse` is an unimplemented placeholder), so
   the doc's per-collection `Status` obligation isn't statable against the
   real enum yet. Revisit if `Status` grows collection-scoped variants.
3. `no_reference_after_drop`: `Schedule`, `AllowWrites`, and `Peek` (index
   target) on `id` are only legal while `since id` has not yet reached the
   empty frontier. Once an `AllowCompaction` with the empty frontier for
   `id` has taken effect, no later event may `Schedule`, `AllowWrites`, or
   `Peek` against it.
4. `create_dataflow_ids_fresh`: a `CreateDataflow`'s export ids are pairwise
   distinct and disjoint from every id created earlier in the execution.
5. `frontiers_monotone_and_terminal`: for each id and frontier kind, the
   sequence of reported frontiers in a well-formed execution is
   non-decreasing, and once a kind reports the empty frontier for an id, no
   later event reports a non-empty value for that (id, kind).
6. `peek_response_unique`: for each `Uuid`, a well-formed execution contains
   at most one `PeekResponse` for it, and if present, it is preceded by
   exactly one `Peek` command with the same uuid.
7. `cancel_requires_peek`: `CancelPeek uuid` is only legal after a `Peek`
   with matching uuid.

Each theorem's statement should be traceable to the specific doc-comment
sentence it encodes, linked from the Lean docstring rather than narrated in
the proof.

### Project structure

`doc/developer/semantics/` (Docker image, pinned Mathlib version,
`lakefile.toml`, `ci/test/lean-semantics.sh` harness) does not exist on this
branch: it was built on #36614's still-unmerged fork branch. Phase 1
bootstraps this scaffolding itself, following #36614's shape as prior art
(same Lean/Mathlib pin approach, same Docker-image-plus-CI-script structure)
without depending on that PR landing. Concretely: a `doc/developer/semantics/`
package with a `lakefile.toml` declaring an `MzCompute` `lean_lib` target, a
`Dockerfile` and `ci/test/lean-semantics.sh` harness for CI, and a
`MzCompute/` module directory with its own root import file. If #36614 lands
first, the two efforts reconcile by merging into a single `lakefile.toml`
with two `lean_lib` targets (`Mz` and `MzCompute`) sharing one Docker image.
Until then `MzCompute` stands alone. It is designed as a separate library and
namespace from the start rather than planned as a merge into `Mz`, since
`Mz`'s docstring (on the #36614 branch) scopes it to scalar evaluation and
collection semantics, a different subsystem (data plane, not control plane).
Own `compute-protocol.md` reading-order doc, matching the naming #36614 uses
for `model.md` / `transforms.md`, to keep the two efforts easy to merge later.

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
event naming id") rather than through a `step` relation. The two shapes are
formally interchangeable: any such predicate can be written as a fold that
threads state through the list, which is exactly what `step` does under a
different name. The reason to prefer `step` isn't expressiveness, it's
reuse: `step` names and exposes the intermediate state (`ProtocolState`) as
a first-class definition that Phase 2 can literally extend with a few more
fields and Phase 3 can literally lift to a product over replicas, whereas a
predicate's internal fold state is private to its own proof and would need
to be re-extracted and renamed for each later phase to build on.

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
  execution and defers reconnection modeling to a later phase. Confirm this
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

# Two-runtime arrangement sharing: capture-based lifecycle

## Summary

The interactive compute runtime reads index arrangements maintained by the
maintenance runtime through a per-process sharing registry.
Today there is no compute-level handshake governing when it is safe for one
runtime to act on shared state the other still references.
Safety rests entirely on the controller's read-hold ordering plus immutable
Arc'd batches, both of which live outside this code and are raced by the two
runtimes executing on independent command channels with no cross-runtime
ordering.

This document defines a capture-based lifecycle that makes the sharing sound at
the compute level, and simplifies the primitive by removing the incremental
replay machinery that is the direct cause of two memory-correctness-class bugs.

## Motivation

Four observed failures share one root cause, a missing capture/teardown
handshake between the runtimes:

* **Row-doubling.** A differential join over a shared import read a record from
  both the trace and the stream because the publisher advances the two from
  unsynchronized sources.
* **Delayed-capability panic.** `import_snapshot_at` downgrades its capability to
  the trace `upper` (read from `map_batches`) and then tries to emit a batch
  whose hint (from the stream) is below that. The panic aborts the process under
  two-runtime shared fate. Reproduced deterministically on
  `linear-join-fuel.td` (a peek on `mz_dataflow_operators`, a fast-advancing
  logging arrangement).
* **Drop-vs-pending-work hang and panic.** Dropping a maintained index removes
  its registry slot on the maintenance runtime only. An interactive pending peek
  on that id re-enqueues forever rather than failing, and a deferred interactive
  build treats the removal signal as a publication and builds into a
  `handles() == None` panic.
* **Silent stale read.** The interactive import path lacks the `since <= as_of`
  assertion the maintenance import path has, so a compaction that races past
  `as_of` yields coalesced (inaccurate) data with no error.

The unifying observation is that the single-instance protocol invariant, "when
the worker processes `AllowCompaction{empty}`, nothing local still reads X", no
longer holds process-wide once two runtimes share one arrangement.

## The single-time invariant

The interactive runtime only ever performs single-time reads.

* Peeks are single-time.
* Transient query dataflows are bounded at `until = as_of.step_forward()`, so
  every input is read at one frontier.
* The only multi-time consumers, subscribes and copy-tos, route to the
  maintenance runtime.
* The interactive runtime maintains no logging or introspection arrangements
  (`enable_logging = false`).

This is not a simplification we accept for convenience.
It is the correct boundary of the split.
A continuously-maintained dataflow hosted on the interactive runtime would read
shared arrangements that advance only as fast as the maintenance runtime seals
them, so its output frontier is clamped to the maintenance tick rate and it
gains no freshness.
It would only move maintenance CPU into the interactive runtime, which is the
low-latency uncontended read lane the split exists to protect.
Maintained work therefore belongs on the maintenance runtime by definition.

We make this invariant an enforced contract rather than an assumption. See
Barrier A'.

## The capture-based lifecycle model

A shared-arrangement reference is in exactly one of two states.

* **Un-captured.** A pending peek or a deferred build. Vulnerable to teardown.
  It must be explicitly resolved (served, failed, or cancelled) when the source
  retires.
* **Captured.** The published chain has been cloned under the registry lock, its
  batches are Arc-pinned, and a read hold is registered. Immune to teardown. The
  maintenance runtime may compact, merge, or drop the source freely, because the
  Arc pins the specific captured batches. Spine teardown never frees batches an
  importer already captured.

The registration-under-lock inside `import_snapshot_at` is the capture
transition, the concrete "safe because the Arc'd data has been captured" point.
Two barriers make the model sound, one for each state.

### Barrier A: seal-gate the interactive build (capture)

Today `handle_create_dataflow` defers an interactive dataflow only until each
dependency is published, and `resolve_dirty` treats publication as terminal,
ignoring later seal advances.

Change: a dependency is ready for a build at `as_of` only when it is published
**and** its published `upper` is beyond `as_of`.
The seal-dirty wake already fires `note_frontier` on every frontier advance, so
a deferred build re-checks the seal condition on each advance until the
dependency seals past `as_of`, then builds.
The check is per worker, against that worker's published `upper`, using the
dataflow's own `as_of`.
The registry exposes the published `upper` for an id and worker.

With the build seal-gated, at registration time `upper > as_of` holds, so the
cloned chain already covers `[.., as_of]` completely.
The importer never needs the incremental path.
This moves the seal-wait from the buggy incremental replay into clean build
deferral at the same latency the peek path already pays.

### Barrier A': enforce single-time at the routing boundary

The multiplexer's `to_interactive` predicate gains a single-time requirement.
A transient dataflow is routed to the interactive runtime only if it is
single-time (`until == as_of.step_forward()`, no persistent exports).
A non-single-time transient falls back to the maintenance runtime, which can
host any dataflow, so a misclassification degrades gracefully rather than
aborting the shared-fate process.
The interactive `CreateDataflow` path carries a `debug_assert` for the same
condition, a test-only tripwire for routing bugs.

### `import_snapshot_at` becomes a pure one-shot

Because the build is seal-gated, `import_snapshot_at` no longer needs a replay
queue.
The operator:

1. Mints a handle, which registers a read hold at the current `since`.
2. Checks `since <= as_of`, returning a graceful error otherwise (see Error
   handling).
3. Clones the chain under the state lock.
4. Emits every batch wrapped in `BatchFrontier(as_of, until)` under a single
   capability, then drops the capability.

There is no queue, no `Frontier` instruction stream, and no `delayed(hint)`.
The delayed-capability panic and the row-doubling double-count are impossible by
construction rather than patched.
`import_snapshot_at` is the only interactive import path.

### Barrier B: tombstone teardown (un-captured references)

The registry distinguishes "not yet published" from "dropped".
Today both are `handles() == None`, which is why pending work cannot tell wait
from fail.
The registry gains three states for an id: `Published`, `Tombstoned`, `Absent`.
`remove(id)` transitions the slot to `Tombstoned` and wakes importers.

Interactive consumers resolve against the state:

* A pending peek on a `Tombstoned` dependency sends a definitive error
  `PeekResponse` and drops from `pending_work`, rather than re-enqueuing forever.
* A deferred build whose dependency becomes `Tombstoned` cancels via
  `cancel_deferred_dataflow` and synthesizes empty `Frontiers`, rather than
  building into a `handles() == None` panic.

Already-captured imports are unaffected, since their batches are Arc-pinned.
Tombstones compose with the existing re-export alias rule: a source dropped
while a re-export still imports it stays alive via its target until the target
drops.

### M3: graceful `since <= as_of` check

The interactive import path adds the `since <= as_of` check the maintenance
import path already has (`render.rs` `import_index`).
Because a compute-worker panic aborts the process under shared fate, the check
degrades to a dataflow or peek error rather than a panic.

## What gets deleted

Once single-time is enforced, the following are dead and are removed:

* Live `import()` (the multi-time import) in `shared_trace.rs`.
* The `ImportQueue` and per-importer replay `queues`.
* The publisher's per-importer instruction push (the `arrived` stream batches
  and `Frontier(upper)` pushes), which is the two-source hazard.

The publisher shrinks to: refresh `chain`, `since`, and `upper` under the lock,
wake importers so pending peeks and deferred builds re-check, and drive
writer-driven compaction with the reader-hold meet.
Both `snapshot_at` (fast-path peek) and `import_snapshot_at` (dataflow import)
then read `state.chain` directly, a consistent under-lock snapshot, so the
two-source class of bug cannot recur.

## Protocol framing

The controller addresses one logical compute instance.
The multiplexer fans its command stream to two runtimes with no cross-runtime
ordering.
Lifecycle commands for a shared arrangement are split: create on maintenance,
peek on interactive, drop on maintenance.
This design does not add a new protocol command.
It makes the registry the cross-runtime synchronization authority for the
lifecycle of a shared arrangement:

* The capture transition (registration under the lock, gated by seal via Barrier
  A) is where the interactive runtime takes a self-sufficient reference.
* The tombstone (Barrier B) is the teardown signal the interactive runtime reads
  to resolve un-captured references.

The controller's read-hold discipline remains the primary safety mechanism that
prevents a drop from being issued while a captured reader is live.
The barriers here make the compute layer sound under the async window the
controller ordering does not itself close, converting hangs and process aborts
into graceful, explicit outcomes.

## Error handling

* Dependency `Tombstoned` while a peek is pending: definitive error
  `PeekResponse`. Under correct controller ordering this cannot happen, so it is
  a diagnostic for a controller or routing bug, not an expected path.
* Dependency `Tombstoned` while a build is deferred: cancel the build, emit empty
  `Frontiers` so the controller can release the input read holds.
* `since > as_of` at capture: graceful dataflow or peek error, never a panic.
* Non-single-time transient reaching the interactive router: routed to
  maintenance instead. `debug_assert` tripwire on the interactive
  `CreateDataflow` path.
* Publisher close vs a late importer: unchanged. The `closed` flag observed under
  the registration lock still lets a late importer seed its own terminal
  frontier.

## Testing

* Unit, `shared_trace.rs`: single-time snapshot import at `as_of` with
  `upper > as_of` returns exact rows, including over a merged chain, and is
  deterministic. Adapt the existing join and reduce tests to the one-shot path.
* Unit, `sharing.rs`: a pending peek fails on tombstone, a deferred build cancels
  on tombstone, and a captured import is immune to a subsequent `remove`.
* Regression under two-runtime on: `information_schema_columns.slt` and
  `object_ownership.slt` (row-doubling), `linear-join-fuel.td` (the panic),
  `materializations.td` (drop index), and the reported slt pairs.
* Concurrency: a create/drop/peek stress over shared indexes to exercise the
  async window between the runtimes.

## Non-goals

* No new `ComputeCommand` or `ComputeResponse` variant.
* No change to subscribe or copy-to routing.
* No live or multi-time import on the interactive runtime. That capability is
  removed, and the single-time contract is enforced.
* No change to the controller's read-hold model. This design hardens the compute
  layer beneath it.

## Rollout

This is a proof-of-concept branch (`mh/two-runtime-stage2`).
The changes are gated behind `enable_two_runtime_compute`, default on only in the
test suites that exercise it.
The single-time enforcement and the tombstone are internal to the compute layer
and require no catalog or protocol version change.

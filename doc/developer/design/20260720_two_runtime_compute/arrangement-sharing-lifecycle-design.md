# Two-runtime arrangement sharing lifecycle

## Summary

The interactive compute runtime reads index arrangements maintained by the
maintenance runtime through a per-process sharing registry.
A per-process multiplexer fronts both runtimes, splitting the controller's one
command stream between them.
Today the interactive sub-protocol is malformed (it references maintained ids it
never created), and the read path hand-rolls differential's stream and trace
consistency across the two-timely-world boundary, which panics.

This document defines the lifecycle on three principles:
build on a correct protocol and panic on anything outside it, keep dataflow
construction deterministic across workers, and have the multiplexer split the one
correct protocol into two correct sub-protocols.
It corrects the read path rather than replacing it, keeping the frontier-tracked
replay that gives a differential join its correctness and fixing only the
unsynchronized feed that panics.
It adds no teardown coordination, because a correct protocol already drops a
maintained arrangement only after every reader has completed.

## Motivation

Observed failures, all rooted in the missing cross-runtime lifecycle:

* **Row-doubling.** A differential join over a shared import read a record from
  both the stream and the trace, because the two advanced from unsynchronized
  sources. Fixed on the branch by a frontier-tracked replay that keeps
  `stream.frontier == trace.upper`. That fix is load-bearing and is kept.
* **Delayed-capability panic.** `import_snapshot_at`'s replay feed is
  two-source: batches arrive from the arrangement stream, frontiers from the
  trace's `map_batches`, which runs ahead. A frontier downgrade past a
  not-yet-emitted batch's hint panics, aborting the process under shared fate.
  Reproduced deterministically on `linear-join-fuel.td`.
* **Drop-vs-pending-work hang and panic.** Dropping a maintained index removes
  its registry slot on the maintenance runtime only. An interactive deferred
  build treats the removal signal as a publication and builds into a
  `handles() == None` panic, and a pending peek can wait indefinitely.
* **Silent stale read.** The interactive dataflow-import path lacks the
  `since <= as_of` check the maintenance import path has, so a compaction that
  races past `as_of` yields coalesced data with no error.

The single-instance protocol invariant, "when the worker processes
`AllowCompaction{empty}`, nothing local still reads X", no longer holds
process-wide once two runtimes share one arrangement.

## Design principles

1. **Build on a correct protocol, panic outside it.** Compute assumes the command
   protocol it receives is a correct instantiation of the spec. A protocol
   outside the spec is undefined behavior, and compute panics rather than
   defending against it locally. In particular, compute relies on the controller's
   read-hold discipline: a maintained arrangement is dropped only after every
   reader has completed. Teardown safety follows from this, with no lease and no
   withheld command. A panic takes down both runtimes of the process, which is
   correct: the two runtimes share fate, and there is nothing to isolate.
2. **The multiplexer splits into two correct sub-protocols.** The multiplexer
   splits the controller's one correct protocol into two, one per runtime, each
   itself a correct instantiation. The maintenance sub-protocol is well-formed
   already. The interactive sub-protocol references maintained ids created on the
   maintenance side, so the multiplexer makes it well-formed with an `Import`
   command (below).
3. **Deterministic construction.** All workers of a runtime must build dataflows
   in the same order, because timely allocates exchange-channel identifiers from
   a per-worker construction-order counter. We render in command arrival order
   and never reorder or defer a build. The current per-worker deferral, which
   builds on each worker's own publication order, is a latent nondeterminism this
   design removes.

### Sharing is per-process

A timely compute runtime spans multiple processes.
Arrangement sharing does not cross a process boundary: the shared batches are
`Arc`'d in memory, so the interactive runtime in a process reads only the
maintenance arrangements published in that same process, through that process's
registry.
Across processes the workers coordinate through timely's network exchanges, as
any distributed dataflow does, but no arrangement `Arc` is shared between
processes.

Only process 0's multiplexer receives the routable commands (`Peek`,
`CreateDataflow`, `AllowCompaction`).
It forwards them onto the runtime command channel, whose worker-0 broadcast
delivers them to every worker of that runtime on every process.
So the `Import` commands the multiplexer synthesizes are authored once on
process 0 and reach every worker through the existing broadcast, with no shared
memory and no per-process coordination.

## The bounded-read boundary

The interactive runtime serves a read only when it is **bounded**: its `until` is
a finite frontier.
A bounded read at `[as_of, until)` is answerable from the maintained arrangement,
since all of its data lies below `until`, which the source seals to.
An **unbounded** read (`until` empty, that is live-follow such as a subscribe)
runs on the maintenance runtime.
It would otherwise track the source's live frontier and gain no freshness beyond
the maintenance seal rate while consuming the interactive lane.

This is broader than a single-time read.
Multi-time bounded reads are fine on the interactive runtime.
The read is still captured, not live-followed: the import tracks the source only
up to `until`, then completes.

Routing keys on `until` finiteness, not on transience, since a maintained index
has an unbounded `until` and so lands on maintenance regardless of id.
Copy-to is a finite-`until` exception, since it drives an S3 sink and is refused
by reconciliation, so it is routed to maintenance explicitly, alongside
subscribes.
Concretely, the multiplexer routes a `CreateDataflow` to interactive only when
`until` is non-empty and the description has no subscribe or copy-to sink.

## Arrival-order rendering with pre-allocated publication points

Both runtimes render dataflows in command arrival order, identical across all
workers, so timely construction order matches and exchange channels line up.
There is no deferral and no reordering.

When the interactive runtime renders a dataflow whose imported arrangement is not
yet published, it must still build the import operator in order, against a real
trace handle, because a differential join or reduce captures its input trace by
value at construction. It cannot be swapped later.
The mechanism is a pre-allocated publication point.

This mechanism was validated with a spike: a join built over an empty placeholder
handle, captured by value at construction, produces the correct output once the
same `Arc` is filled in place, with no doubling and no panic. It relies on
`SharedTraceHandle` being a live proxy into shared state, so a trace filled in
place is visible to every later cursor the join issues, which is differential's
ordinary "an arrangement starts empty and fills" behavior.

### Placeholder and adopt

A publication point is an `Arc<SharedTrace>` holding the shared state (chain,
`since`, `upper`, importer queues, reader holds).
The `TraceAgent` that writes it lives in the publisher's sink closure, not in
`SharedTrace`, so the writer is decoupled from the shared state.

* **Placeholder.** A publication point can be created empty: chain empty, `since`
  and `upper` at the minimum frontier (`from_elem(minimum)`, never the empty
  antichain, which reads as sealed through the end of time). Handles mint
  immediately. An import built on a placeholder holds its output frontier at the
  minimum while empty, so the downstream dataflow makes no progress, which is
  correct, since it has no input yet.
* **Adopt.** The maintenance publisher attaches its sink to an existing
  publication-point `Arc` rather than constructing a fresh one. Publishing is the
  degenerate case of adopting a publication point that has no prior reader.
* **Hold forwarding.** A reader hold registered against a placeholder lands in
  the shared `logical_holds` and `physical_holds`. The publisher forwards their
  meet to its agent on its first refresh, so a hold taken before adoption pins
  the adopted trace with no extra machinery.

### Registry get-or-create

Whichever runtime touches an id first creates its publication point.

* Interactive-first: on `Import(id)` (or when a read of `id` renders) the
  interactive runtime creates a placeholder slot and builds live imports against
  it.
* Maintenance-first: the maintenance runtime creates the slot and adopts it in
  the same step, the current behavior.

The registry replaces its create-fresh-and-overwrite `insert` with a
get-or-create that returns the slot to adopt, so a placeholder a reader already
imports is filled in place and never overwritten out from under it.
The slot covers both the oks and errs arrangements.
This get-or-create is where the genuine two-runtime race on an id lives and gets
a dedicated cross-thread test.

### Never adopted

An import registered against a placeholder that is never adopted, because index
creation is cancelled before it publishes, would otherwise hold its frontier at
the minimum forever.
The publication point supports a terminal close: it is marked closed and its
importer queues receive a terminal empty frontier, mirroring the publisher's
existing close-on-drop path, which completes the interactive read rather than
wedging it.
The `Import` withdrawal is what triggers the close, so a placeholder is closed
exactly when the controller withdraws the maintained id.

## The import: frontier-tracked replay, single-sourced

The import stays a frontier-tracked replay.
That is what keeps `stream.frontier == trace.upper`, which is what stops a
differential join from reading a match from both stream and trace and doubling
it. A one-shot dump of the chain under a single capability, with a trace that
presents the whole chain at once, is exactly the doubling bug and is not used.

The panic is not the replay. It is the replay's **feed**.
The publisher splices two sources into each importer's queue: batches from the
arrangement stream (hint = the stream capability time) and frontiers from the
trace's `map_batches` (the authoritative upper).
The trace advances ahead of the stream within a worker step, so a
`Frontier(upper)` can be enqueued before a `Batch` whose hint is below it. The
importer downgrades its capability to `upper` and then cannot delay a later batch
to its lower hint.

The fix is to feed the replay from a single source, the trace, so a batch and the
frontier that closes it are always mutually ordered.
On each refresh the publisher derives both the batch delta and the new upper from
one `map_batches` snapshot, and never advertises an upper beyond the batches it
has enqueued.
This matches how differential's own `TraceAgent` import replays a trace, batches
before the frontier that closes them, from a single authoritative source.
The delta and merge handling (emitting only the part of a merged batch beyond the
importer's frontier) mirrors differential's import and is the second mechanism to
validate with a spike, as the placeholder mechanism was.

The readiness and `since` checks operate on the meet of the oks and errs
frontiers, since an index publishes two arrangements whose frontiers advance
independently.
The import is ready to complete when `meet(oks.upper, errs.upper)` is at or beyond
`until`.
The import asserts `meet(oks.since, errs.since) <= as_of` at capture, matching the
assert the maintenance import path already makes.
A violation is a protocol error (the controller offered an `as_of` below the
readable frontier), so it panics rather than reading coalesced data silently.
This is the check the interactive path is missing today.

## The interactive sub-protocol: the Import command

The controller's protocol is well-formed for one instance: every id a `Peek` or a
`CreateDataflow` references was created by a prior `CreateDataflow` and not yet
compacted to empty.
The multiplexer splits that protocol in two.
The maintenance sub-protocol keeps the durable creates and drops and stays
well-formed.
The interactive sub-protocol keeps the reads, but those reference maintained ids
created on the maintenance side, so on its own it is malformed.

The multiplexer makes it well-formed with an `Import` command, a new command the
controller never sends and never sees.
When the multiplexer routes a maintained index's `CreateDataflow` to maintenance,
it also sends `Import(id)` to interactive, declaring that `id` is available to
import from the shared registry.
When it routes that index's drop, it sends the matching withdrawal to interactive.
A `Peek` or `CreateDataflow` that references `id` on the interactive runtime is
then well-formed, since `id` was declared by a prior `Import`.

`Import` also carries the placeholder lifecycle.
On `Import(id)` the interactive runtime get-or-creates the registry slot for `id`,
a placeholder if maintenance has not published yet, the real arrangement once it
has.
On the withdrawal it closes that slot, delivering a terminal empty frontier to
any importer still bound to it.
This is the close that a placeholder needs, since a placeholder's `since` is the
minimum frontier and the `since` check below cannot fire for it.
Because a correct protocol drops `id` only after every reader has completed
(Principle 1), the withdrawal arrives after interactive's reads of `id` are done,
so the close is safe.

There is no lease, no refcount, and no withheld command.
Teardown safety is the controller's read-hold discipline, which a correct
protocol already provides.
The multiplexer's only job is to keep each sub-protocol well-formed, and a
reference the tracking cannot explain is a malformed protocol, so it panics.

## What changes

* The publisher stops splicing stream batches with trace frontiers. It feeds the
  replay from a single trace-derived source.
* `SharedTrace` gains a placeholder constructor and an adopt path, and the
  registry gains get-or-create in place of create-and-overwrite.
* The publication point gains a terminal close, triggered by the `Import`
  withdrawal.
* The multiplexer synthesizes `Import(id)` and its withdrawal to the interactive
  runtime, mirroring maintained-index creates and drops, and gains the
  bounded-read routing predicate.
* Live `import()` (the unbounded, non-snapshot import) is removed, since the
  interactive runtime never issues an unbounded read. Its only callers are tests.

## Error handling

Compute assumes a correct protocol and does not degrade gracefully around a
malformed one. It panics, which under shared fate takes down both runtimes.

* `since > as_of` at capture: a protocol error (bad `as_of`), so it panics via the
  assert, rather than reading coalesced data silently.
* A reference on the interactive runtime to an id no `Import` declared: a
  malformed sub-protocol, so it panics rather than wedging on a missing slot.
* An index dropped while a read is outstanding does not occur under a correct
  protocol, since the controller drops a maintained id only after every reader
  has completed.
* Reconciliation drop-and-recreate of the same id: the withdrawal closes the old
  slot, a fresh `Import` and get-or-create make a new one, and a placeholder import
  fills in place. No spurious failure.
* Non-bounded, subscribe, or copy-to dataflow reaching the interactive router:
  routed to maintenance instead, with a `debug_assert` on the interactive
  `CreateDataflow` path as a tripwire for a routing bug, itself a protocol error.

## Testing

* Unit, `shared_trace.rs`: a join built over a placeholder, captured at
  construction, produces the correct output once the slot is adopted and filled,
  with no doubling. A single-sourced replay feed does not panic when the trace
  advances ahead of the stream within a step.
* Unit, `sharing.rs`: get-or-create races between a placeholder-creating reader
  and an adopting publisher, cross-thread, converge on one slot. The `Import`
  withdrawal closes a placeholder's importers with a terminal frontier.
* Unit, multiplexer: `Import(id)` is synthesized to interactive on a maintained
  index create and its withdrawal on the drop. The bounded-read predicate routes
  a subscribe and a copy-to to maintenance and a bounded query to interactive.
* Regression under two-runtime on: `information_schema_columns.slt` and
  `object_ownership.slt` (row-doubling), `linear-join-fuel.td` (the panic),
  `materializations.td` (drop index), and the reported slt pairs.
* Concurrency: a create, drop, and peek stress over shared indexes to exercise
  the async window between the runtimes.

## Non-goals

* No new controller-visible command or response. `Import` is internal to the
  multiplexer-to-interactive stream, never seen by the controller.
* No teardown coordination in compute. A correct protocol drops a maintained id
  only after every reader has completed.
* No change to subscribe or copy-to routing beyond making the copy-to exclusion
  explicit.
* No unbounded, live-following import on the interactive runtime. Bounded
  multi-time reads remain.
* No change to the controller's read-hold model. This design assumes it.

## Open validations

* **Single-sourced replay feed.** The placeholder mechanism is validated. The
  single-source feed, including delta and merge handling that matches
  differential's `TraceAgent` import, is the remaining mechanism to validate with
  a spike before implementation.
* **`Import` across reconnect.** `Import` and its withdrawal are commands in the
  interactive sub-protocol, so they belong in the interactive command history and
  reconcile like any other command. Confirm the multiplexer re-derives them
  deterministically from the replayed controller commands on reconnect, so the
  interactive history stays consistent with maintenance's.

## Rollout

This is a proof-of-concept branch (`mh/two-runtime-stage2`).
The changes are gated behind `enable_two_runtime_compute`, default on only in the
test suites that exercise it.
`Import` is internal to the compute layer and requires no catalog or
controller-protocol version change.

# Rendezvous protocol model

`RendezvousProtocol.lean` is a Veil model of the rendezvous publication primitive, the shared-trace layer on top of it, and the compute lifecycle that drives both across the maintenance and interactive runtimes, reconciliation included.
It checks three rules that together let the registry keep identifying an incarnation by `(GlobalId, as_of)`: a publication point accepts one writer in its life, the registry never hands out a slot whose writer closed or dropped, and the two runtimes synchronize their reconciliations with a barrier.
The safety clauses state that an import reads one writer's chain without missing updates, and that every reader is bound to its own incarnation.
SMT proves the clauses inductive for all instance sizes, and an explicit-state check confirms them on a small instance.
Deleting each guard the design relies on in turn reaches a violation, except the single-attach refusal, a backstop that is inert in the model, and the physical-meet bound, which needs a larger instance than the committed one (see the mutation table).
The [interactive compute runtime design](../20261107_interactive_runtime.md) describes the system this model abstracts.

## Running

Veil pins its own Lean toolchain (`leanprover/lean4:v4.32.0`) and a no-Mathlib `lean-smt`, which differ from the toolchain and dependencies of `misc/lean`.
No CI job builds this file, so it can drift from the code it models.
The checked run used Veil at commit `0a20c4c7154bdb020bda4404e5051e4298a06d92`.

```sh
git clone https://github.com/verse-lab/veil ~/dev/repos/veil
curl -sSfL https://raw.githubusercontent.com/leanprover/elan/master/elan-init.sh | sh -s -- -y --default-toolchain none
cd ~/dev/repos/veil && lake build
mkdir -p Examples/Materialize
cp <this directory>/RendezvousProtocol.lean Examples/Materialize/
lake build Examples.Materialize.RendezvousProtocol
```

`lake env lean` cannot load cvc5's native library, so the file has to be built as a module of the Veil project.
The SMT check and the explicit-state check run in one build of about seven minutes, and either can be commented out to run the other alone.
SMT carries the proof for all sizes, and the explicit-state run is only a cross-check.
Its cost is elaboration and compilation of the checker, not exploration: alone, the committed instance builds in 4 min 52 s at 4.06 GB peak RSS, and the same instance with two epochs, 7,011 states, still takes 4 min 13 s at 3.99 GB, measured with other builds running.
A mutation run, which keeps only the safety clauses, takes about 3 min at under 3 GB.

A mutation is checked on a copy of the file.
The design switches (`barrier`, `barrier_first`, `epoch_match`, `single_attach`, `fresh_slot`) are flipped in both the `assumption [design]` line and the `#model_check` theory.
The other mutations delete the named guard.
The explicit-state checker stops at the first violated clause, and a supporting invariant usually breaks first, so a mutation run deletes every `invariant` line and keeps the `safety` lines.
To see a specific clause fail, delete the other `safety` lines too.

## What is modeled

The model tracks one index id with one worker per runtime and only the `oks` arrangement.
A `writer` is one incarnation of the index on the maintenance runtime, a `point` is one `Rendezvous`, an `imp` is one interactive import with its subscription, its reader, and the trace it hands downstream, and an `epoch` is one controller connection.
The collection itself is abstract: its content at each time is identical across incarnations, so an incarnation differs only in its `upper`, its span boundaries, and its compaction frontiers.
A downstream read through the import's acknowledged frontier `f` is correct when the chain its trace reads covers `f`, has a span boundary at `f`, delivered every item without a gap, and is not logically compacted past the read's `as_of`.
Holds are one registration per holder.
The importer's queue is the importer's own view of the point, so every precondition the importer acts on reads its queue and never the live point.

Reconciliation is modeled per runtime as four phases.
A runtime that sees a newer connection while serving starts reconciling it, possibly skipping connections in between.
In the drop phase it drops every incarnation, peer bundle, and import its history replaces, then arrives at the barrier.
It waits until the other runtime arrived at the same connection or a later one, creates the connection's incarnation, peer bundle, and imports, and serves.
The model also lets a runtime waiting at the barrier begin a newer connection, which the code cannot do, since a waiting worker reads no command.
This over-approximates the code's behavior, so the safety results carry over, but the model says nothing about how the code's wait ends.
Each connection's history installs one incarnation, `goal e`, which either keeps the previous connection's or names one never seen before.
A `stray` holder may clone a slot's `Arc` at any time and drop it later, standing for a peek in flight or any other reference the lifecycle does not account for.

Abstractions to keep in mind when reading the results:

* Items are reduced to the frontier through which a subscriber received them, plus a flag for a gap.
* The downstream consumer of an import's trace is folded into the import: it reads through the acknowledged frontier and holds physically there.
* Every incarnation has the same `as_of` in the explicit-state check, the worst case for the registry key. The SMT check leaves `as_of` free.
* Both runtimes decide their drops from the same history of a connection. A connection only one runtime finishes reconciling, as the design section describes, is not modeled.
* Liveness is not checked, so neither is the claim that the barrier wait ends. The design section argues it.
* `errs`, the logging dataflow's extra points, and the publisher's reserved peer holds are left out. The peer's hold is registered when it binds.
* A live writer never publishes the empty frontier: `m_seal` requires a time below `top`, and `no_empty_upper` and `no_empty_queue` state that no point and no queue ever show it.
  The code allows it. A writer whose collection is complete, such as an index over a constant collection, seals its `upper` to the empty frontier while it lives, and the import completes when it receives it.
  What holds in the code is the narrower rule that a closed writer never publishes the empty frontier, see `Attachment::close`.
* Each writer backs one point, which it attaches fresh, with an empty chain compacted to its `as_of`.
  In the code a trace can back several points, and attach to a further point with a non-empty chain and advanced frontiers: an index that re-exports another index's arrangement publishes the other index's trace under its own id.
  Such a re-export point lives as long as the backing trace, not as long as the re-exporting index, so it does not freeze when that index drops, and the fresh-slot rule does not apply to it.
  The model's `closed_dropped` invariant, which equates a dropped incarnation with a frozen point, does not hold for these points.
  A recreated re-export gets a fresh slot only because reconciliation drops every holder of the old slot first. A `stray` holder of it, which the model allows, would hand the old, unfrozen slot to the next incarnation.
* Registering an import's reader and subscribing it are one action, `x_create`.
  In the code they take separate acquisitions of the point's lock, and the writer can publish and merge between them. The import's physical hold, taken at registration, keeps the chain's boundaries in that window.
* The writer reads the holds' meet, publishes it and applies it to its trace in one action, `m_sync`. In the code the point advertises the new `since` before the trace applies it, a window that refuses readers the trace could still serve.

## Lifecycle facts

The facts below are about code the later changes of this stack add. They name the code by symbol.

* Both runtimes receive every non-peek command, each at its own rate (`Multiplexer::send`).
* The transport serves one controller connection at a time and finishes the previous one before it builds the client of the next (`mz_service::transport::serve`).
  The process records each connection when the multiplexing client forwards its `Hello`, before either runtime receives a command of it (`ConnectionOrder::record`, called from clusterd's `ProcessComputeClient::send`), so the recorded order is the order the process accepted connections in.
* A connection's position comes from one sequencer per runtime and travels with its commands.
  The controller sends `Hello` and `UpdateConfiguration` to every process and every other command to process 0 only (`mz_compute_client::service`), so a process other than 0 can record a connection after its workers received the connection's commands, or never.
  Global worker 0 of each runtime runs in process 0 and looks the position up in that process's order when it sequences the connection's first command (`mz_compute::command_channel::render`), and the command channel delivers it with every command to every worker of the runtime (`UnifiedCommand::Compute`).
  Both runtimes' worker 0 read the one order of process 0, so the two workers of a pair agree on every position, and a worker arrives at the barrier with the delivered position and never looks it up (`Worker::await_reconcile_barrier`).
  The model hands a runtime the connection it begins to reconcile with its place in the order, as an action parameter of `m_begin` and `i_begin`, which is this delivery, so the state space does not change.
* Each runtime reconciles on its own (`Worker::reconcile`).
  It discards peeks and reduces the command history before it compares it with the new connection's commands, so two runtimes with the same history make the same decisions.
* Reconciliation runs the compactions and drops of existing dataflows, then waits at the barrier, then runs the rest (`Worker::reconcile`).
* The drop phase does not wait on another worker.
  An `AllowCompaction` moves this worker's trace holds or drops the collection, and `drop_collection` drops the dataflow with `drop_dataflow`.
  Timely's `drop_dataflow` removes the dataflow from this worker's map and drops it (`src/worker.rs:739` in timely 0.32.0), a dropped capability updates a worker-local change batch (`src/dataflow/operators/capability.rs:172`), and a dropped channel pushes its id onto a worker-local queue (`src/allocator/canary.rs:19` in timely_communication 0.32.0).
* A worker waiting at the barrier steps its Timely worker and reads no command (`Worker::await_reconcile_barrier`).
  The workers of its runtime that already passed are therefore not held up, and since passing is the only way out, every worker of a runtime executes the same commands in the same order.
* The barrier admits a runtime once the paired worker of the other runtime arrived at the same connection or a later one (`ReconcileBarrier::passable`). The design section says why a later one counts.
* The barrier is per worker pair, as the registry is (`ArrangementSharingRegistry::per_worker`). A slot is only ever touched by the two workers of its pair.
* The maintenance runtime publishes an index into `get_or_create(id, as_of)` (`ArrangementSharingRegistry::publish`), and the interactive runtime binds its peer bundle to the same call (`ArrangementSharingRegistry::peer_bundle`).
* `get_or_create` returns the slot in the map when it is alive, its `as_of` is equal, and it is not frozen, and replaces the map entry otherwise.
  A slot is frozen once its traces closed or dropped.
* A slot is alive while the publisher's token, a peer bundle, or an importing dataflow's tokens hold it (`PublishToken`).
* `Rendezvous::attach` panics on a point that ever had a writer, and a point freezes when its writer closes or drops (`Attachment::close`, `Attachment`'s `Drop`).
* `drop_dataflow` drops an index's writer, and the trace box closes its attachments so the empty seal stays out of the point (`TraceBox::close`).
* The import's trace reads whatever chain its point currently publishes (`SharedReader`). With one writer per point, that is the chain the import subscribed to.

Whether the controller ever chooses an equal `as_of` for a replaced incarnation is not settled by this code.
The model checks the worst case, where every incarnation has the same `as_of`.

## The design

### One writer per point

A publication point accepts exactly one writer attach in its life.
A second attach would replace the chain with one that can regress below what the point's subscribers acknowledged, and a subscriber cannot follow a regression.
The earlier design cut such subscribers with `Event::Detached`, and a live import panicked on it, but a downstream read could miss updates before the import observed the cut.
With one writer per point there is nothing to cut: a point's chain only grows, and freezes when its writer closes or drops.
The fence that kept a stale writer from publishing goes too, since a point has no second writer, and a closed writer still publishes nothing while its holds keep adjusting.

### A fresh slot for a frozen one

The registry holds one slot per `GlobalId`, and an incarnation is the `GlobalId` together with the `as_of` stored in its slot.
When the slot under an id is frozen, `get_or_create` creates a fresh slot for the new incarnation instead of returning the frozen one, whose points would refuse the new writer.
Readers of the frozen slot keep its final view.
This is the local backstop against a stray `Arc` keeping an old slot alive past the barrier: whoever binds the id after the old writer dropped gets the fresh slot.
It does not cover a re-export's point, which does not freeze when the re-exporting index drops.

### The reconciliation barrier

Without coordination, the interactive runtime can reconcile a connection that recreates an index at the same `as_of` before the maintenance runtime dropped the old incarnation.
`get_or_create` then returns the old incarnation's live slot, the new peer binds to it, and once the old writer drops, imports through that peer read a frozen chain that never advances.
The barrier closes this: each worker arrives once its drops for a connection are done, and creates nothing until the paired worker of the other runtime arrived at the same connection or a later one.

Scope: one barrier per worker pair, created with the pair's registry.
A per-process barrier is not required: a slot is shared by exactly the two workers of its pair, and a worker of one runtime never waits on a worker of its own runtime at the barrier.
`Solo` runtimes skip it.

Where it blocks: between the drop phase and the create phase of `Worker::reconcile`, on every reconciliation, including the first connection, whose drop phase is empty.
After reconciliation, ids are unique, so the live command stream needs no barrier.

A worker waiting at the barrier cannot leave it by skipping the connection's creates, because the other workers of its runtime may already have passed, and timely requires every worker of a runtime to build the same dataflows in the same order.
So the barrier counts the other runtime's arrival at the same connection or any later one, for two reasons.
First, the barrier keeps only each side's latest arrival.
The other runtime can arrive at a connection, pass, serve, and arrive at the next connection before the waiting worker rechecks, which overwrites the arrival the waiting worker needs.
Second, a runtime may never finish reconciling a connection the other runtime finished.
Each runtime's channel adapter stops forwarding a connection's queued commands once a response send to the connection fails (`spawn_channel_adapter`), and the two runtimes notice the dropped connection independently.
One runtime can then receive the connection's `InitializationComplete` and arrive, while the other moves on to the next connection without arriving.
A runtime that arrived at a later connection has dropped everything its later history replaces, and creates nothing until the waiting runtime arrives at that later connection too, which the waiting runtime does only after it finished the earlier connection.
Of two waiting workers, the one at the earlier connection passes, and the controller's last connection reaches both runtimes, so the wait ends.
Shutdown needs no abort: a panic on either runtime aborts the process (the panic hook clusterd's `main` installs), and workers do not exit otherwise.

## Results

SMT proves the conjunction of 16 safety clauses and 48 supporting invariants inductive across all 24 actions, `(64 + 1) x (24 + 1) = 1625` checks.
The explicit-state checker explores 961,101 states of the instance with three times, three epochs, two incarnations, two points, and one import without a violation.
That is the smallest instance in which every mutation below reaches its counterexample, except the physical-meet row: three epochs let a runtime skip a connection, two points let the registry choose between a frozen and a fresh slot, and three times let a writer seal once before it drops.

## Mutation table

Each row deletes one guard from the committed model, or flips one design switch, and runs the explicit-state check over the safety clauses.
The failing clauses are the first violation the checker reports, which it reaches.

| Guard deleted or switch flipped | Failing clauses |
| --- | --- |
| No barrier (`barrier = false`) | `peer_own_writer`, `peer_holds_own_incarnation` |
| Barrier after the creates (`barrier_first = false`) | `peer_own_writer`, `peer_holds_own_incarnation` |
| Barrier counts any arrival of the other runtime, not the same or a later connection (`epoch_match = false`) | `peer_own_writer`, `peer_holds_own_incarnation` |
| Reuse a closed slot: no single attach and no fresh slot | `peer_own_writer`, and with only the read clauses kept, `ack_covered`: the new writer's chain regresses below what an import acknowledged |
| No fresh slot for a frozen one (`fresh_slot = false`) | `no_panic`: the new writer's attach is refused |
| No single-attach refusal alone (`single_attach = false`) | inert, the fresh-slot rule never hands a frozen slot to a writer (961,101 states) |
| Seed under the registering lock | `queue_current` |
| Close before the drop seal | `no_empty_upper` |
| Merge only below the applied physical frontier | `ack_on_boundary` |
| Physical meet bounded by the holds | inert with three times (1,005,837 states), `ack_on_boundary` with four times and two epochs |
| Release on read completion | `holds_balance` |

The single-attach refusal is inert because the model's only attach path goes through the registry, which the fresh-slot rule already guards.
It stays in the code as the backstop for any path that does not.

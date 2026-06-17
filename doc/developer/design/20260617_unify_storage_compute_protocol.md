# Unify the storage and compute cluster protocol

- Associated: (TODO: link epic / issue)
- Motivating instance: [SS-199: Elevated freshness in mz_catalog_server in staging](https://linear.app/materializeinc/issue/SS-199/elevated-freshness-in-mz-catalog-server-in-staging)

## The Problem

Storage and compute run as two independent timely runtimes inside a single
`clusterd` process, each driven by its own controller-side protocol stack: a
separate `transport::Client` connection per cluster process, a separate
`Partitioned` fan-out client, and a separate per-replica `ReplicaTask`.
The two command streams reach a worker in a nondeterministic relative order,
because they arrive on independent connections driven by independent async
tasks.
This lack of a cross-subsystem order is the central obstacle to ever unifying
the two timely runtimes: a single runtime must render dataflows in an identical
order on every worker, and order can only come from a single, totally-ordered
command stream.
A prior attempt to share a runtime ([#34713]) ran into exactly this — without a
protocol-level total order, workers diverge and render invalid dataflows.

[#34713]: https://github.com/MaterializeInc/materialize/pull/34713

### Motivation: cross-subsystem hold-back (SS-199)

[SS-199] is a concrete instance of the cost of separate runtimes.
With `enable_storage_introspection_logs` on, storage ticks at a 10s interval and
holds back compute introspection, raising `mz_catalog_server` freshness from
~1s to ~10s.
A direct fix is hard: it runs into a bootstrapping problem we do not currently
know how to solve.
Unifying the two runtimes would categorically eliminate this class of
cross-subsystem hold-back — the two would share one progress domain rather than
gating each other.
This design is the prerequisite step toward that unification, not the fix for
SS-199 itself; the fix lands with the runtime-unification follow-up.

[SS-199]: https://linear.app/materializeinc/issue/SS-199/elevated-freshness-in-mz-catalog-server-in-staging

## Success Criteria

* A single, totally-ordered command stream carries both storage and compute
  commands from the controller to each cluster process, established at one
  serialization point.
* Behavior is unchanged: with the unified protocol in place but the runtimes
  still separate, the system behaves exactly as today.
  The total order is present on the wire but dormant.
* The change does not touch the timely allocator or the rendering path, so it
  cannot reintroduce the [#34713] divergence class.
* The result is a foundation on which runtime unification becomes a localized,
  worker-side change rather than a protocol redesign.

## Out of Scope

* Unifying the two timely runtimes on the cluster side.
  This design only unifies the protocol and demultiplexes back to the existing
  storage and compute runtimes.
  Runtime unification is the follow-up that the total order enables.
* Extending sequential hydration to storage collections.
  Storage has no `Schedule`/suspend protocol; adding one is a separate feature.
* Independent failure domains for storage and compute.
  We accept shared fate, consistent with the existing recommendation to keep
  storage and compute objects on separate replicas.

## Solution Proposal

Introduce a union message type and a single unified `ReplicaTask` per replica
that owns one CTP connection per cluster process and interleaves storage and
compute commands into it.
The interleaving order chosen by that task is the total order, by construction:
it is the single point that owns the connection write.
On the cluster side, a thin demultiplexer splits the union stream back into the
existing storage and compute command handlers, preserving today's behavior.
The divergent routing and merge logic is reused verbatim through delegation, not
rewritten, because the underlying machinery (`Partitioned`, `Partitionable`,
CTP `transport::Client`) is already generic over the message type.

### Union message type

```rust
enum ClusterCommand {
    Storage(StorageCommand),
    Compute(ComputeCommand),
}

enum ClusterResponse {
    Storage(StorageResponse),
    Compute(ComputeResponse),
}
```

CTP (`src/service/src/transport.rs`) is already generic: `Client<Out, In>` with
`(Out, In): Partitionable<Out, In>`.
One `Client<ClusterCommand, ClusterResponse>` per cluster process replaces the
two per-process connections.
A single physical connection per process is deliberate: CTP is framed FIFO, so
the wire preserves the task's send order with no explicit sequence number in the
happy path, and one connection matches the accepted shared-fate model (the
connection dies, both subsystems' incarnation ends).

### Delegating `PartitionedState`

`Partitioned<P, C, R>` (`src/service/src/client.rs:110`) is a single generic
struct; routing and merging live entirely in the two
`PartitionedState<C, R>` impls.
We add `impl PartitionedState<ClusterCommand, ClusterResponse>` that holds both
existing states and dispatches by variant:

* `split_command(Storage(c))` delegates to `PartitionedStorageState::split_command(c)`,
  re-wrapping each part as `Storage(..)`.
* `split_command(Compute(c))` delegates to `PartitionedComputeState::split_command(c)`,
  re-wrapping as `Compute(..)`.
* `absorb_response` dispatches by variant into the corresponding sub-state.

The existing routing (storage broadcasts all; compute unicasts to worker 0
except `Hello`/`UpdateConfiguration`) and merge logic (frontier union vs. meet,
peek/subscribe row merging) are reused unchanged.

### Unified `ReplicaTask` — the serialization point

Today storage (`src/storage-controller/src/instance.rs:852`) and compute
(`src/compute-client/src/controller/replica.rs:159`) each run a structurally
identical task: `connect()` (retry forever) then a `select!` loop over a command
channel and `client.recv()`.
The unified task replaces both:

```text
run():
  state = hydration state machine (fresh per incarnation)
  client = connect()                       // one union CTP connection per process
  loop select:
    cmd from storage_cmd_rx  -> specialize -> client.send(Storage(cmd))
    cmd from compute_cmd_rx  -> specialize -> for c in state.absorb_command(cmd):
                                                  client.send(Compute(c))
    resp from client.recv()  -> match:
        Storage(r) -> storage_response_tx.send(r)
        Compute(r) -> for c in state.observe_response(&r): client.send(Compute(c))
                      compute_response_tx.send(r)
```

The task's pull order across the two command channels is the total order.
The single union `Partitioned` client owns the write, so there is no separate
sequencer to build and no second writer to race with.
`specialize_command` (Hello nonce, `CreateInstance`/config) is dispatched by
variant.

### Cluster-side demultiplexer

The receive loop on each cluster process matches `ClusterCommand` and forwards
to the existing storage or compute worker handler.
Relative order between a storage and a compute command is discarded at this
split — which is correct, because the two runtimes are still separate and do not
consume cross-subsystem order yet.
**Nothing may depend on cross-subsystem order until the runtimes are unified.**

### Sequential hydration placement

`SequentialHydration` (`src/compute-client/src/controller/sequential_hydration.rs`)
is a per-replica feedback controller: it holds back `Schedule` commands and
releases them as hydration capacity frees up, where capacity is freed by
observing `Frontiers` responses.
Its placement constraints (behind the controller; before the `Partitioned`
split so all workers see `Schedule` in one order and so it observes every
compute command) must be preserved.

We refactor it from a `GenericClient<ComputeCommand, ComputeResponse>` wrapper
into a synchronous interceptor state machine that the unified `ReplicaTask`
pumps on the compute command and response branches.
Its core (`collections`, `hydration_queue`, `hydration_token`, `absorb_command`,
`observe_response`, `hydrate_collections`) is already synchronous; only the
`GenericClient` impl, the forwarder task, and the two mpsc channels are removed.
Those channels existed solely to make `recv` cancel-safe while internally
sending to the wrapped client; once the unified loop owns the sends, that
guarantee is provided by the loop, exactly as today's `client.send(cmd).await?`.
State reset on reconnect is automatic: the state machine is recreated per
incarnation, mirroring today's `SequentialHydration::new` inside `connect()`.

### Reconnect and reconciliation

Neither subsystem reconnects in-task today.
On disconnect, `run_message_loop` returns `Err`, the task finishes, and
`is_failed()`/`failed()` (`task.is_finished()`) flips.
The instance controller watches that flag, drops the dead task, and spawns a
fresh one with a new epoch; reconciliation is the controller replaying its full
command stream into the fresh task.
This die-and-respawn model is identical on both sides, so unification is a
supervisor merge:

* A unified replica supervisor (merging the two `Replica`/`ReplicaClient`
  structs) owns the single task, one `connected` flag, and one `failed()` check.
* On failure it bumps a shared epoch, respawns the unified task, and signals
  **both** controllers to replay — even when only one subsystem triggered the
  failure (shared fate).
* Each controller keeps its existing reconcile logic and replays independently
  into its command channel; only failure detection, respawn, and epoch are
  unified.

## Minimal Viable Prototype

Land the union type, delegating `PartitionedState`, unified `ReplicaTask`, and
cluster-side demux behind the existing behavior, then verify the system is
unchanged (existing storage and compute integration tests pass, replicas
reconnect and reconcile correctly).
The total order is observable on the wire (e.g. via tracing) but exercised by
nothing yet.

## Alternatives

### Sequential hydration for all objects, including storage

Rejected.
Storage broadcasts every command and self-coordinates ordering internally; it
has no `Schedule`/suspend protocol.
Gating storage through sequential hydration would require inventing a
storage-side suspend mechanism — a separate feature with changed semantics, not
step-1 plumbing.

### Keep `SequentialHydration` as a wrapper, generalized to `ClusterCommand`, in front of the unified task

Rejected.
It retains the forwarder task and two mpsc channels (needed only for the
wrapper's `recv` cancel-safety), adds an outer layer above the merge point, and
routes storage commands through compute-owned middleware as passthrough — extra
task hop and latency on storage frontier commands for zero benefit.

### Two physical connections per process with explicit sequence numbers

Rejected for step 1.
Two TCP streams race, so recovering the total order requires explicit sequence
numbers plus a resequencing buffer on the cluster side.
A single connection gets the order from CTP's FIFO framing for free and matches
the accepted shared-fate model.

### Bolt a sequencer onto the two existing streams

Rejected.
Stamping epochs onto two independently-driven streams is a retrofit that can
desync.
Making one task own the single connection write produces the order
intrinsically, with no separate component to keep consistent.

## Open questions

* **Epoch unification.**
  Compute carries a per-incarnation `epoch` to discard stale responses
  (`replica.rs:148`); storage keys responses by `ReplicaId` only.
  The unified path needs the storage response path to honor the shared epoch so
  a respawn can distinguish stale storage responses.
  Confirm there is no other storage-side assumption that the absence of an epoch
  encodes.
* **Protocol versioning.**
  CTP `connect` takes a single `version`.
  Unifying the connection means storage and compute are versioned together from
  now on; confirm this does not break independent rollout assumptions.
* **Reconcile ordering on respawn.**
  Both controllers replay into the fresh task; confirm there is no implicit
  dependency on storage reconciling before compute (or vice versa) that the
  separate-connection model accidentally provided.

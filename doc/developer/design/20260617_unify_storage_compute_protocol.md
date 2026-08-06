# Unify the storage and compute clusters

- Associated: (TODO: link epic / issue)
- Motivating instance: [SS-199: Elevated freshness in mz_catalog_server in staging](https://linear.app/materializeinc/issue/SS-199/elevated-freshness-in-mz-catalog-server-in-staging)

## The problem

Storage and compute run as two independent timely runtimes inside a single
`clusterd` process, each driven by its own controller-side stack: a separate
`transport::Client` connection per cluster process, a separate `Partitioned`
fan-out client, and a separate per-replica `ReplicaTask`.
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
This design is the prerequisite work toward that unification, not the fix for
SS-199 itself; the fix lands with the runtime-unification phase.

[SS-199]: https://linear.app/materializeinc/issue/SS-199/elevated-freshness-in-mz-catalog-server-in-staging

## The dependency chain

The end goal is one runtime, but it cannot be approached directly.
The work decomposes into three links that must be built in order, because each
depends on the one before it:

* **Runtime unification** needs a single, totally-ordered command stream.
  Without it, workers render dataflows in divergent orders ([#34713]).
* **A total order on the wire** needs a single serialization point that owns one
  connection per cluster process and interleaves both subsystems' commands into
  it.
  That point can only live where both command streams already converge — and
  today they never do, because each controller independently owns its own
  connection, replica task, failure detection, reconcile, and epoch.
* **A single serialization point** therefore needs the controller-side replica
  management of the two subsystems to converge first: one component that owns the
  connection and replica lifecycle, which both controllers feed.

So the order is **controllers → protocol → runtime**, and the first link is the
prerequisite that has been missing.

An earlier iteration of this design attacked the middle link first (unify the
protocol, demultiplex back to the existing runtimes).
That work landed the easy parts cleanly — a union message type, a cluster-side
demultiplexer, and a single-port server — but stalled on the controller side.
Forcing one connection requires both controllers to give up connection
ownership to a shared per-replica supervisor, which is a controller-side
unification in all but name.
Bolting that supervisor onto two still-independent controllers meant threading
command channels and exposing response sinks across two mismatched
architectures: the storage controller is synchronous, while the compute
controller's `Instance` is a decoupled async task that owns its response sink and
produces commands from within itself.
The lesson is that the protocol cannot be cleanly unified before the controllers
are.
This revision reorders the work accordingly.

## Success criteria

* **Phase 1** establishes one replica-management component per replica that both
  controllers register with; behavior is identical to today, with two connections
  still under the hood, and it is fully validatable on a real cluster.
* **Phase 2** collapses those two connections into one totally-ordered
  `ClusterCommand` stream with no change to controller domain logic; the total
  order is present on the wire but exercised by nothing yet.
* **Phase 3** unifies the runtimes as a localized, worker-side change rather than
  a protocol redesign, consuming the total order Phase 2 produced.
* No phase touches the timely allocator or the rendering path until Phase 3, so
  the [#34713] divergence class cannot reappear early.
* Each phase is independently shippable and testable on its own merits.

## Out of scope

* Unifying the storage and compute *domain* controllers.
  Their domain logic — storage collections, ingestions, sinks, persist,
  frontiers; compute dataflows, peeks, subscribes, hydration — stays separate.
  Only the replica-management and transport layer beneath them converges.
* Independent failure domains for storage and compute.
  We accept shared fate, consistent with the existing recommendation to keep
  storage and compute objects on separate replicas.
* The runtime unification itself (Phase 3) and the SS-199 fix it enables; both
  follow this work.

## Solution proposal

### Phase 1 — unified replica-management layer (no wire change)

Extract one per-replica component that owns the connection and replica lifecycle
— connect, drop, failure detection, reconcile, and epoch — and refactor both
controllers to be producers and consumers against it rather than each owning
their own `ReplicaTask` and connection.

Each domain controller registers, per replica, three things with the layer:

* a command stream it produces (storage commands; compute commands),
* a response sink it consumes, and
* a reconcile hook the layer invokes on (re)connect to replay the controller's
  command history.

The layer owns the per-incarnation `epoch`, detects failure, respawns, and drives
both controllers' reconcile hooks on respawn.
Crucially, **Phase 1 keeps two connections under the hood**: the layer opens a
storage connection and a compute connection internally.
There is no total order yet, and behavior is identical to today, so Phase 1 is a
pure refactor that can be exercised by the existing storage and compute
integration tests and by replica kill/reconnect tests.

This phase is where the controller asymmetry is resolved once, deliberately,
rather than worked around under the pressure of also changing the wire.
The storage controller is synchronous; the compute `Instance` is a decoupled
async task reached via `instance.call(|i| ...)` that owns its response sink and
produces commands internally.
For both to register uniformly, the layer's interface is defined at the
controller level (synchronous handles to a command stream, a response sink, and a
reconcile hook), and the compute side either exposes those at its controller
boundary or — preferably — adopts the same async-task shape as compute so the two
register symmetrically.
See [Open questions](#open-questions).

### Phase 2 — collapse to one connection (the total order)

With replica management unified, swapping two connections for one is localized to
that single layer.
The layer stops opening a storage connection and a compute connection and instead
opens one `Client<ClusterCommand, ClusterResponse>` per cluster process,
interleaving both subsystems' commands into it.
The order in which the layer pulls from the two command streams is the total
order, by construction, because it is the single point that owns the connection
write.
On the cluster side, a thin demultiplexer splits the union stream back into the
existing storage and compute worker handlers, preserving today's behavior.

The pieces this needs are small and largely already prototyped:

* **Union message type.**

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

  CTP (`src/service/src/transport.rs`) is already generic over the message type:
  `Client<Out, In>` with `(Out, In): Partitionable<Out, In>`.
  A single physical connection per process is deliberate: CTP is framed FIFO, so
  the wire preserves the layer's send order with no explicit sequence number, and
  one connection matches the accepted shared-fate model.
  A single shared CTP `version` covers both subsystems, since storage and compute
  are built and rolled out together.

* **Delegating `PartitionedState`.**
  `Partitioned<P, C, R>` is one generic struct; routing and merging live in the
  two `PartitionedState<C, R>` impls.
  A delegating impl holds both existing states and dispatches by variant, reusing
  the divergent routing (storage broadcasts all; compute unicasts to worker 0
  except `Hello`/`UpdateConfiguration`) and merge logic (frontier union vs. meet,
  peek/subscribe row merging) unchanged.

* **Cluster-side demultiplexer.**
  The receive path on each cluster process matches `ClusterCommand` and forwards
  to the existing storage or compute handler, merging their responses back into
  the union stream.
  Relative order between a storage and a compute command is discarded here, which
  is correct because the runtimes are still separate.
  **Nothing may depend on cross-subsystem order until Phase 3.**

* **Sequential hydration.**
  `SequentialHydration` is refactored from a `GenericClient` wrapper into a
  synchronous interceptor the layer pumps on the compute command and response
  branches, dropping its forwarder task and two mpsc channels.
  (This refactor is independent of the rest and can land early.)

This is a hard cutover: clusterd serves the union on one port in place of the two
legacy ports, and clusterd and environmentd roll together, so there is no
mixed-version window.

### Phase 3 — unify the runtimes

With a total order on the wire, the two timely runtimes can be merged into one on
the cluster side, so the rendering path consumes one totally-ordered command
stream rather than two racing ones.
This phase eliminates the cross-subsystem hold-back behind SS-199 and is detailed
in a follow-up design once Phase 1 and Phase 2 land.

It has a non-obvious prerequisite: **the storage self-channel must become
controller-ordered first.**
The wire total order from Phase 2 orders controller→cluster commands, but it is
not the only source of dataflow construction on the storage side.
Storage does not render a dataflow directly in response to the controller's
`RunIngestion`; that command only sets up reporting state and kicks an async
worker, which computes resume-uppers from persist and then broadcasts an
`InternalStorageCommand::CreateIngestionDataflow` over storage's internal command
fabric — and *that* internal command is what actually renders the dataflow on
every worker (`src/storage/src/storage_state.rs`, the `RunIngestion` flow; the
internal command is documented to be able to "overtake" the external command).
`SuspendAndRestart` (self-healing on a source error) and `DropDataflow` originate
the same way.
Today this is sound because storage broadcasts internal commands across its own
workers via the timely fabric — so all storage workers render identically — and
storage has its own timely id space.

Under one shared runtime with one id allocator, that self-channel becomes a
second, controller-invisible source of timely-id allocation.
Even though it is consistent across storage workers, its interleaving with
compute's controller-driven `CreateDataflow` is pinned to no single total order:
two workers can allocate storage-vs-compute dataflow ids in different relative
orders and diverge — exactly the [#34713] failure class.
The fix is to make storage's dataflow lifecycle controller-ordered, as compute's
already is.
The async/persist computation has to stay on the cluster (it reads persist), but
the *decision to render* and its *ordering* — `CreateIngestionDataflow`,
`SuspendAndRestart`, `DropDataflow` — must round-trip through the controller and
re-enter the single totally-ordered stream, leaving exactly one ordering
authority.
This collapses storage toward compute's model, where the worker never
self-allocates and the controller drives all construction.
It is a behavioral change — a source restart gains a controller round-trip — and
is its own work item within Phase 3, not a detail of merging the allocators.

## Minimal viable prototype

Phase 1 alone: extract the unified replica-management layer with two connections
still under the hood, refactor both controllers to register against it, and verify
the system is unchanged — existing storage and compute integration tests pass, and
replicas reconnect and reconcile correctly after a kill.
No wire change, no total order yet; the value is the foundation and the resolved
controller asymmetry.

## Alternatives

### Unify the protocol before the controllers

Rejected, based on a prototype.
Unifying the wire first lands the union type, demultiplexer, and single-port
server cleanly, but the unified serialization point forces both controllers to
surrender connection ownership to a shared supervisor — controller unification by
another name.
Doing that while the controllers remain independent means threading channels and
exposing sinks across the synchronous storage controller and the decoupled async
compute `Instance`, an asymmetric, hard-to-validate change.
Building the controller layer first makes the protocol collapse a localized edit.

### Sequential hydration for all objects, including storage

Rejected.
Storage broadcasts every command and self-coordinates ordering internally; it has
no `Schedule`/suspend protocol.
Gating storage through sequential hydration would require inventing a
storage-side suspend mechanism — a separate feature with changed semantics.

### Two physical connections per process with explicit sequence numbers

Rejected for Phase 2.
Two TCP streams race, so recovering the total order requires explicit sequence
numbers plus a resequencing buffer on the cluster side.
A single connection gets the order from CTP's FIFO framing for free and matches
the accepted shared-fate model.

### Bolt a sequencer onto the two existing streams

Rejected.
Stamping epochs onto two independently-driven streams is a retrofit that can
desync.
Making one component own the single connection write produces the order
intrinsically, with no separate component to keep consistent.

## Open questions

* **Phase 1 layer interface.**
  The exact shape of the unified replica-management layer: the handles each
  controller registers (command stream, response sink, reconcile hook), where the
  layer lives, and how it owns failure detection, respawn, and epoch.
  This is the core design work of Phase 1 and will be detailed before
  implementation.
* **Storage controller symmetry.**
  The compute `Instance` is a decoupled async task; the storage controller is
  synchronous.
  Either the layer accommodates both shapes, or the storage controller is
  refactored to the same async-task shape so both register symmetrically.
  Making storage async is a larger, orthogonal refactor (full storage-controller
  decoupling) and is not strictly required, but it yields a uniform integration.
  Decide during Phase 1 design.
* **Epoch ownership.**
  The unified layer owns the per-incarnation `epoch`.
  Compute already carries an epoch to discard stale responses; storage keys
  responses by `ReplicaId` only and gains the epoch as beneficial hardening.
* **Reconcile ordering on respawn.**
  Reconciliation is order-independent; there is no storage/compute dependence, so
  the order in which the two controllers replay is irrelevant.
  Reconcile compute first by slight preference if a choice is forced.
* **De-self-channeling storage (Phase 3 prerequisite).**
  How to move storage's dataflow-lifecycle decisions
  (`CreateIngestionDataflow`, `SuspendAndRestart`, `DropDataflow`) onto the
  controller-ordered stream while keeping the async resume-upper computation on
  the cluster: what the cluster reports to the controller, what the controller
  re-issues, and how source restarts behave with the added round-trip.
  Scoped during Phase 3 design; flagged here because it is invisible until the
  runtimes actually share an id space.

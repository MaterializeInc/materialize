# Decoupling Materialize: capabilities and consequences

The [architecture overview](20260903_decoupling_materialize.md) separates intent,
process management and execution. This companion explains the resulting
capabilities and the ownership assumptions that must change to support them.
It describes the design rather than an implementation-status report. The
[detailed design](20260903_decoupled_coordination.md) defines the contracts and
scope, and the [handoff](20260903_decoupled_coordination_log.md) tracks acceptance.

## Capabilities

### Horizontal adapters and independent recovery

Multiple adapters serve the same durable catalog and use the same deployment's
replicas. Adding an adapter adds serving capacity without duplicating maintained
dataflows or selecting a privileged serving adapter. Planning and connection
capacity can scale independently of maintained execution. Shared catalog and
write contention remain scaling limits, not work eliminated by adding adapters.

Each adapter has its own sessions, transient requests and execution protection.
Replacing one does not replace a peer or reset shared tables. Available peers
can continue serving while controllerd restores the failed adapter. Routing and
client reconnection remain external concerns. Sessions attached to the failed
adapter are not transparently transferred, and cancellation remains
endpoint-local.

### Controller-independent execution

Controllerd is responsible for processes, not for keeping their database work
alive. Stopping or restarting it leaves healthy adapters and replicas running,
including existing sessions and maintained dataflows. Its absence can delay
provisioning and recovery of failed processes without becoming a database-wide
execution outage.

Zero adapters is also a valid inventory. Controllerd can maintain the committed
replica processes and accept an administrative request to create an adapter.
Replica-set policy and adapter-owned maintenance may pause in the meantime.
Neither an empty inventory nor a controller restart reinitializes the database.

### Warm upgrades with explicit cutover

Compatible deployments have separate controllers, adapters and replicas, but
share logical definitions and durable coordination. A new deployment hydrates
its replicas while the existing deployment continues serving. Compatible
full-writer deployments can both serve requests and commit catalog and table
writes. This does not give every deployment external-sink output authority.

Promotion selects the active deployment and transfers active execution authority
without throwing away warmed replicas. It does not automatically retire the
previous deployment. Routing changes and eventual retirement are explicit.
This is the concrete continuity contract behind zero-downtime upgrades, not a
promise to migrate sessions or make every output handover interruption-free.

### Independent lifecycle and process management

Adapters become named declarations with stable identity, requested resources,
discoverable endpoints and observed readiness. SQL and controller HTTP management
use the same validated catalog operations. Creation acknowledges intent, not a
ready process. Rename need not restart a process, and an assigned endpoint is
not evidence that it is ready to serve.

Separating desired inventory from enactment makes the process boundary usable
with local processes or Kubernetes pods without giving adapters provisioning
privileges. M3 targets the local-process backend. Kubernetes and operator
integration remain follow-up work, not an already-verified capability.
Operational commands and examples belong in the
[developer guide](../controllerd.md).

## Engineering consequences

### Initialization is not joining

A singleton adapter could treat startup as environment bootstrap: register shared
infrastructure, republish catalog snapshots and reset process-local observations.
With several adapters, another process may already be serving against that state.
Repeating bootstrap can duplicate facts or erase a peer's work.

Initialization therefore establishes joinable catalog and table infrastructure
as a finite operation. Joining reconstructs local handles without resetting
shared state or granting deployment authority. Controllerd owns initialization
and active-deployment configuration reconciliation. Adapters retain planning,
but joining is neither initialization nor promotion.

The distinction also constrains recovery. An interrupted initialization needs to
finish required infrastructure, not replay every startup side effect. A default
adapter is created only for a fresh environment, not whenever inventory is empty.
Existing-environment conversion is a separate rollout decision.

### Planning is not installation

Moving installation into replicas does not imply moving the optimizer there.
The adapter committing a maintained compute object also commits its selected
plan and the protection needed to install it. Replicas install written plans
rather than asking the originating adapter to reconstruct its command sequence.

Plans have build ownership. A deployment's adapters prepare plans for their build,
and do not overwrite another build's selections. Removing an imported object
requires repairing affected selections while preserving running execution's
protection. A new index does not by itself replan every existing dataflow.

This makes another distinction explicit: catalog existence and planning
eligibility are not installation or hydration. An index can exist before any
replica has installed it. EXPLAIN need not wait for execution readiness, while an
actual read must establish readability and wait for required progress.

### Catalog catch-up is part of execution ordering

A single coordinator usually sees its own catalog changes before it sends their
effects to a replica. Independent adapters and replicas do not share that order.
An adapter can be behind a completed peer transaction, and a fast request can
arrive before its definitions have been applied at the replica.

Catalog freshness and request admission therefore have explicit contracts.
Statements validate a suitable catalog prefix, and execution requests identify
the state they require. Commit timestamps participate in the shared oracle's
ordering and completion discipline. Observing local object existence is not a
substitute for freshness.

Even catalog catch-up has an important ordering boundary. Applying several
transactions and then enacting their combined effects against only the final
state loses intermediate CREATE, ALTER and DROP semantics. Effects must be
enacted against their matching committed transaction state. Conversely,
protection-only progress must not invalidate planning as if it were structural
DDL. These distinctions preserve useful concurrency without accepting stale
decisions or imposing an all-replica barrier on every commit.

### Read protection outlives its local accounting

Removing the adapter's lifecycle authority also removes the place that used to
aggregate read requirements and drive compaction. Independent local hold maps
cannot establish a shared permission to discard history. A disconnected reader
may still need old data, and a replacement replica must recover requirements
before advancing compaction.

The catalog coordinates requirements and compaction permission. Protection
covers client reads, maintained recovery and object-owned retention. Logical
inputs remain protected even when optimization removes them from the running
plan, while selected plans also protect actual imports such as indexes. This
allows reconstruction without assuming the same access paths remain available.

The distinction matters with zero replicas. Removing execution does not remove
an object's retention policy. Conversely, recovery protection need not pin its
creation timestamp forever when durable progress proves older input unnecessary.
Replica-local logs remain volatile rather than acquiring a new durability promise.

Clients aggregate ordinary holds locally under committed protection. Advancement
can be coalesced, retaining extra history rather than writing the catalog for
every query. New protection must still be established before use. A committed
compaction frontier is permission, not proof that physical Persist compaction
has caught up. These choices trade catalog traffic and retention cost against
independent recovery, without making observed frontiers into implicit read holds.

### Identity determines the cleanup boundary

A logical adapter or replica, its realization in a deployment, its current
execution, and its sessions have different lifetimes. A name is not a process
identity, a binary version is not a deployment identity, and a connection is not
the owner of a maintained object.

Replacement supersedes one execution and releases its transient protection
without touching peer executions or object-owned requirements. Retirement
revokes a deployment's authority durably. Neither waits for physical process
death to establish the authority boundary. A stale process cannot register fresh
protection merely because it reconnects.

Silence alone does not retire an owner. A declared but dead execution may retain
history until replacement or removal. That conservative behavior avoids treating
a pause as permission to discard data. Temporary objects carry execution
ownership when needed for cleanup, without adding a catalog transaction for
every connection.

Cleanup of separately published observations needs another precaution: absence
from a cached catalog is not proof of retirement. An observation can arrive
before its new owner's catalog record is visible locally. Cleanup uses the live
ownership set and allocation boundary from one committed snapshot to distinguish
retired executions from possibly newer ones. This is not a liveness service or
another lease protocol.

### Maintenance must cooperate, not elect a replacement singleton

Table ticking, webhook progress and shard finalization do not move into
controllerd just because they affect shared state. Authorized adapters can
cooperate through the existing write protocols. If a peer has already advanced
an empty tick to the required frontier, no additional write is necessary.
That rule cannot acknowledge a nonempty user write that never committed.

Table registration, schema changes, appends and retirement retain transaction-WAL
and oracle ordering. Conflicts require fresh validation rather than replaying a
stale candidate. A process detaching its local handles does not remove shared
table membership. Physical shard finalization requires committed eligibility,
definitive WAL Forget and application of preceding writes.

These are ownership corrections at the existing storage boundaries, not a reason
to introduce a general background-job scheduler. Some duplicated attempts are
acceptable. Conflicting publication, lost writes and premature deletion are not.

### Introspection is shared data, not harmless telemetry

Duplicating an adapter also duplicates any collector it starts. Two collectors
that snapshot the same rows and each publish replacements or retractions can
corrupt the relation even if each write is individually atomic. Startup is no
longer a period with no competing writer.

Ownership follows the fact: replica observations belong to replicas, session and
query activity to adapter executions, process observations to controllerd, and
catalog-derived facts preferably to catalog-backed relations. Shared reductions,
sampling and pruning require concurrency-safe publication. Expensive scans can
consult shared completed-work progress and stagger attempts without guaranteeing
exactly one execution of each scan.

Current state, counters and history require different treatment. A successor must
not inherit stale predecessor health as its current report. Missing current
health can honestly be `unknown`, while zero-replica placement is derived as
`paused`. Retained history does not become current authority merely because a
newer event was pruned. Cumulative webhook contributions must remain counted
after their producer is replaced, rather than being deleted with its sessions.

M3 accepts a narrower compromise for remaining adapter-published catalog
projections and catalog-driven lifecycle history. Only the successful committer
publishes those derived rows. Publication can arrive out of order or fail
permanently, leaving gaps or inconsistent projections. Followers still apply
catalog effects, but do not duplicate publication. Catalog-backed conversion is
separate work, rather than an additional delivery-recovery protocol here. This
exception does not relax user-data, protection or current-execution ownership.

### Deployment and process lifetimes do not coincide

Promotion, retirement and process death are separate events. Compatible old and
new deployments can coexist without startup fencing one another. Promotion
transfers active authority, including external-sink ownership, while retirement
explicitly ends a deployment's participation. Output protocols must remain safe
when a paused predecessor resumes, rather than assuming that revoked authority
means its process has already stopped.

Compatibility consequently covers catalog and builtin schemas, written plans,
DDL semantics, Persist formats and the relay envelope. A version label alone
cannot establish it. Shared formats cannot advance past the requirements of an
admitted participant, and the system cannot safely readmit an older version by
merely lowering that label afterward.

The environment-scoped pubsub relay crosses deployment boundaries. It survives
controller restart, zero adapters and deployment retirement. One configured
provisioner owns it during overlap, with explicit transfer and retained-service
adoption rather than competing supervisors. Controller-owned processes likewise
need a lifetime independent of the controller process. Whole-environment shutdown
is a separate operation from stopping one controller, and must preserve data and
avoid stopping unrelated environments or external services.

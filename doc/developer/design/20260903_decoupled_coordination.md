# Decoupled coordination

## Outcome and scope

The catalog is the authority for maintained storage and compute lifecycle.
Cluster-side components subscribe to catalog changes and enact the desired
state, rather than depending on an adapter to send lifecycle commands. Adapters
write catalog changes and use a separate fast protocol for query execution.

The goal is decoupling, preserving consistency guarantees, performance, and
existing SQL behavior except for the [maintained-creation admission rules](#admission),
including the [fresh sink cutoff](#fresh-sink-cutoff).
Losing an adapter must not disrupt maintained dataflows or other clients. Its own
queries may fail. Transparent query or session failover is out of scope.

Working multi-adapter operation is the destination, not this deliverable.
Boundaries must support independent adapters becoming catalog writers and query
clients without another ownership redesign. Once lifecycle enactment leaves the
adapter, the adapter's DDL and the lifecycle components' publication are two
cooperating catalog writers, and that much concurrency is required. Arbitrary
numbers of adapters and their deployment are not.

Initial implementation and validation target environments initialized under the
new protection rules. Conversion of existing environments, builtin schema migration,
and version-upgrade support for protected environments are deferred until after
demonstrating fresh-environment decoupling. They are not prerequisites for the
[milestones](#milestones).

Enabling the new ownership model for existing environments requires a separate
conversion and rollout decision that preserves their promised results. Conversion
to logical-input protection becomes active only once remaining recovery requirements
are protected across all logical inputs. It must not skip pending results by moving
the recovery timestamp forward. Fresh environments must still survive same-version
restarts, replanning, and ownership handover.

Query-local dataflows that do not go through the catalog, including slow-path
SELECTs, SUBSCRIBEs, and COPY TO, remain on the fast protocol. Their creation,
execution, responses, and cleanup are part of request-scoped execution, not
durable catalog lifecycle.

## Future work: true zero-downtime upgrades

The end state this work must not foreclose is an upgrade in which an environment
of the new version runs beside the current one, hydrates everything on its own
replicas, and takes over only at cutover. Both generations are full participants:
catalog writers, query clients, and lifecycle followers. Cutover transfers
ownership of maintained outputs and external writes to the new generation and
fences the old one. An outside signal from the upgrade orchestrator decides when
that is safe. Nothing in the environment infers it.

The catalog is shared across generations, and some state must be kept per
generation. Items, clusters, protection, and what a user has declared for a
cluster, whether its managed configuration and strategy or the replicas of an
unmanaged cluster, live in the catalog once, for all generations. Replicas are
per generation: each generation keeps the replicas it runs for a cluster, derived
from that shared declaration, together with the state that drives them, such as
hydration, scaling, and reconfiguration. A generation's clients and lifecycle
components use its own replicas. Write ownership of a maintained output and a
generation's client incarnations are per generation as well, and exactly one
generation writes a given output at a time. Other state found to differ between
generations is kept per generation rather than folded into a shared definition.

While generations coexist, the catalog is written in a form every live generation
understands. A newer version introduces no record kinds, builtin schema changes,
or migrations until older generations are fenced, and operates against the older
durable state until then. Persist applies the same discipline to its own state.
This is a contract on how versions are developed, not only on this design.

## Approach

Use catalog implications to derive lifecycle effects from committed catalog
state. Complete the relevant implication paths as needed for this work. Move
responsibility for enacting those effects to cluster-side components that can
recover and follow catalog state independently of the originating adapter.
Controllers can remain useful abstractions. Their exact placement and factoring
are implementation choices.

Catalog authority covers creation, changes, compaction permission, and deletion
of maintained objects. It describes desired state, not a command history that
requires the initiating adapter to replay it. A recovering subscriber must be
able to establish the required state and continue following changes.

The fast protocol supports independent query clients without giving a connecting
client ownership of the cluster's maintained lifecycle. Responses, cancellation,
and disconnect cleanup belong to the relevant client. This is a semantic split,
not simply another listener for the same controller protocol.

## Boundary contracts

### Compaction permission

Store explicit compaction bounds for maintained collections in the catalog.
Storage and compute must not compact those collections beyond committed
permission. Applied compaction may lag, retaining extra history. Bounds advance
monotonically within a collection's lifetime. Their representation and granularity
remain implementation choices. A record is justified by information that cannot
be derived from existing catalog state or durable progress, not by uniformity
across object types.

Introducing or strengthening a maintained read requirement and advancing the
affected bounds must be coordinated at the catalog transaction boundary. A
transaction must not commit a requirement incompatible with already authorized
compaction, or authorize compaction that invalidates a committed requirement.
Protection must cover the interval between catalog commit and cluster application,
independently of the creating adapter's lifetime.

This places maintained read requirements and permission to discard history under
the same durable authority. Cluster-side components have an explicit limit to
enforce and recover, without treating a process's local accounting as the
authority to advance beyond it. The cost is ongoing catalog traffic and processing
proportional to changing bounds and publication cadence, accepted over the
[delegated alternative](#delegated-compaction-advancement). Coalescing or
rate-limiting advancement may retain extra history, but must not delay protection
until after it is needed. Choose cadence and batching from measured catalog load,
DDL latency, and retention cost. Publication work scales with what changed, not
with catalog size.

### Applying committed permission

Any component may apply committed compaction permission for the identified
collection lifetime. Applying permission does not require exclusive ownership or
leadership. Concurrent or delayed application must not regress compaction or
bypass other valid read protection. Permission cannot be reused for a different
collection lifetime or an unrelated use of a shared shard.

Coordination belongs at the boundary that authorizes compaction, admits read
requirements, and reclaims client protection, not at the act of applying
already-committed permission. Proposing bounds need not require a leader either,
provided catalog transactions validate proposals against authoritative requirements.

### Logical recovery dependencies

Maintained recovery protection covers all logical collection inputs, including
those eliminated by optimization. Unmaterialized views do not form recovery
boundaries. Persisted inputs do: reading an upstream MV protects its output,
while that MV's maintenance separately protects its own inputs. Indexes remain
replaceable access paths, not the authority for recovery dependencies.

Protection follows the history needed for installation and recovery, rather than
pinning creation-time history forever. For MVs it can advance with durable output
progress and cease when no further input reads are needed for recovery. Execution
holds may retain additional history.

We accept extra retention on inputs the running plan does not read, especially
for slow or suspended consumers, to preserve reconstruction from catalog SQL
independently of optimizer choices.

### Readability and compaction

A reader must secure protection before relying on a timestamp. Observing a
frontier in a catalog snapshot is not itself a read hold. While protection is
valid, compute and persist compaction must respect it, including through
dependencies, installation, and ownership handover. One client cannot release
another's protection.

Installation `as_of` selection respects committed permission, with the replacement
semantics for indexes described in [Index reconstruction](#index-reconstruction).
An MV's initial storage visibility boundary remains a distinct concept. The
representation and accounting mechanisms for individual read requirements remain
implementation choices.

Propagation to persist critical since handles must respect all valid read
requirements. Those handles are the durable backstop, not a substitute for
multi-client accounting.

Readability is enforced where a read happens. A cluster refuses a read below a
collection's since rather than serving it, and persist does the same for shard
reads. Query-local dataflows keep their imports readable through the cluster's
own accounting until they are removed. Reclaiming a client's protection therefore
needs no fence propagated to compute: a reclaimed client's late read is either
served correctly or refused, and it cannot acquire new protection.

Recovery must establish actual readability and restore valid read requirements
before further advancement is authorized. Reconstructed plans must use inputs
readable at the protected timestamps, rather than assume equivalent access paths
have equivalent history.

### Client read protection

Client protection is scoped to a client incarnation and the collections and
frontiers it requires. A client's read protection remains valid across restarts
or replacement of the components enforcing compaction. Durable client requirements
participate in compaction accounting alongside maintained requirements. A durable
requirement on an index protects the index's inputs at that frontier, since
arrangements do not survive compute restart and must be reconstructible.

Clients may aggregate query and transaction needs locally under established
protection. Ordinary query execution must not require a durable write for each
hold change. Establishing protection and reclaiming it must be coordinated with
compaction so that neither a new requirement nor a stale client can rely on
discarded history.

Abandoned client protection must be reclaimable without releasing other clients'
requirements. Maintained requirements belong to maintained objects, not their
creating clients. Query execution and transient dataflows remain ephemeral and
require their own readiness and execution protection. This does not introduce
transparent session or query failover.

### Admission

Admission of maintained read requirements respects committed compaction permission
for all logical inputs, rather than relying on lagging physical compaction.
Automatically selected creation timestamps must be compatible with all those
inputs. Explicit historical refresh requests are rejected if any logical input
cannot support them, even when optimization removes that input.

### Visibility and execution

Catalog commit, cluster application, and query readiness are different events.
Queries must not fail spuriously or execute against the wrong object state
because the fast protocol overtakes catalog application. Preserve existing
behavior for concurrent DDL, transactions, cancellation, and object drops.

Query-client connections must not replace one another's desired state or reset
maintained dataflows. Lifecycle ownership and permission to perform external
writes must remain safe across restarts and handover, independently of query
connection lifetime.

## Selected implementation decisions

These choices resolve implementation questions raised by the requirements above.
They constrain the relevant paths without prescribing their internal mechanisms.

### Fresh sink cutoff

Fresh sinks select their automatic cutoff against committed input permission,
including with `SNAPSHOT = false`. They cannot rely on history merely because
physical compaction lags. Existing sinks retain pending output through alteration
and recovery. Their requirements advance with durable output progress, not merely
with input compaction permission.

### Index reconstruction

An index's compaction bound is its published since, not a historical reconstruction
guarantee. A fresh index has no bound until first publication. Recovery installs at
the least readable frontier, capped by committed permission where permission is at
or above readability. Where permission is below readability, no durable requirement
depends on the gap, because durable protection of an index protects its inputs. The
index is replaced at readability and its bound follows through publication.
Installation never waits for a catalog write. Live readers of an existing trace
remain protected by execution holds.

### Read-only prewarming

Preserve prewarming by following committed catalog permission independently of the
SQL savepoint. Read-only bootstrap follows [Index reconstruction](#index-reconstruction)
without waiting for the writer. A local savepoint write grants no compaction
permission, and there is no startup-specific writer protocol.

### Lifecycle placement

The controller bundle may run as one independent process that follows the
catalog, enacts maintained state, and publishes protection. Those three
responsibilities are its interface. It does not serve controller state to adapters
and does not gate their catalog writes, so it can later dissolve into per-cluster
followers without another redesign. Collection lifecycle and compaction belong to
it. DDL and table appends are request-scoped and stay with adapters.

### Query client

An adapter reads through a query client that owns that client's read
requirements. It learns storage frontiers from persist and compute frontiers from
the fast protocol, where frontier reporting is best effort. It issues peeks and
query-local dataflows and receives their responses. Its protection is the
[durable client protection](#client-read-protection) from the start. No remote
controller access API or volatile hold forwarding is introduced as a bridge.

### Cooperating catalog writers

The following rules apply to protected environments. Unprotected environments
retain local-capability-driven compaction and epoch-fenced normal writer opens,
including their existing migration behavior.

The deployment generation is the catalog fence. Components of the active
generation write without fencing each other, and promotion fences the whole old
generation. Persist compare-and-append is the commit authority: metadata-only
writes such as protection and heartbeats retry on contention, while DDL refreshes
and revalidates and reports a planning conflict when structural changes
invalidated it rather than merging. Each writer follows the durable stream it
commits to. Within a generation, safety comes from validation at commit, not from
per-process epochs.

Persist critical since handles follow the committed bound only. Every valid read
requirement is in that bound, so applying it is monotone and needs no per-process
opaque. Local hold accounting does not drive critical handles. A prewarming
process that needs client protection writes under the active generation, since a
writer opened under its own pending generation would fence the leader before
promotion. Where an enactment proves unsafe under two same-generation lifecycle
instances, that case gets a narrow fence of its own, not a general epoch.

Administrative edits use cooperative compare-and-append in either mode, without
exclusive admission or promotion. Client heartbeats and recent publication provide
an advisory live-environment check. `catalog-debug` refuses a live mutation with a
reason unless `--force` is supplied. A serving writer that cannot apply a committed
foreign change halts and rebuilds from durable state. This is the general rule for
foreign writes, not an administrative exception.

## Alternatives

### Plan-specific recovery protection

Protecting only physical inputs ties retention to optimizer output. Controller
dependency holds implicitly do this, with their aggregate effect preserved in
persist sinces. Bootstrap rebuilds holds from physical plans and durable storage
frontiers, but cannot restore discarded history for an input introduced by
replanning.

A plan-specific design needs a recovery contract beyond an input-ID set, such as
durable logical recovery expressions with upgrade compatibility. We choose
logical-input protection rather than making optimization decisions authoritative.

### Delegated compaction advancement

The catalog could define maintained requirements and retention policies while
components account for client and maintained reads and advance
compaction directly. Persist critical handles would provide the durable storage
backstop, without publishing advancing bounds to the catalog. This avoids ongoing
frontier-publication traffic and its dependence on catalog write availability.

Delegation still requires coordination when durable read requirements are
introduced or strengthened. Their admission must be tied to protection held by
those components. Reclaiming abandoned precommit protection must exclude a late
commit that relies on it. Observing an object's absence in a catalog snapshot is
not sufficient. Both approaches must recover valid holds and prevent compaction
based on incomplete or superseded read requirements.

We choose explicit bounds for the catalog-local permission boundary. Delegation
reduces ongoing catalog traffic but shifts coordination into maintained-DDL
admission and cleanup. We accept the publication and retention costs of explicit
bounds rather than this delegated admission and reclamation protocol.

### Volatile client protection

The components enforcing compaction could account for client requirements only in
memory. This avoids durable client-metadata traffic, but losing that accounting
requires either invalidating clients' protection or reconstructing all valid
requirements before compaction advances. An aggregate frontier alone does not say
which clients still need it. We choose durable client protection to avoid tying
otherwise-live clients' protection to those components' lifetimes, accepting the
metadata and reclamation costs.

## Implementation and verification

Existing CI, nightly, and performance suites are the broad acceptance signal.
Ensure coverage actually exercises the changed boundaries, particularly
independent query clients, catalog/query ordering, and read protection during
compaction and recovery. Add targeted tests or experiments where existing
coverage leaves uncertainty. Measure catalog traffic and retained-history cost
as well as query performance.

### Milestones

These are outcome checkpoints, not a rigid implementation sequence or one-commit
tasks. Work can cross their boundaries when necessary.

#### 1. Catalog-backed recovery protection

Maintained requirements and compaction permission are coordinated through the
catalog across maintained storage and compute object types. Use an MV as the
first complete example: logical-input protection is established before installation
depends on it, survives uncached recovery, preserves a pending first refresh, and
advances with durable output progress rather than retaining creation-time history
forever.

Derive requirements from existing catalog definitions and durable progress where
possible. Separate records per object type are not prescribed. Preserve existing
recovery semantics without introducing creation-time history guarantees for indexes
or metric sinks.

Evidence includes actual compaction and an input eliminated by optimization,
fresh builtin initialization, and same-version recovery including MVs reading
system-catalog collections. Measure publication and retained-history costs as the
real path becomes available. This milestone can use the current single-writer
arrangement.

#### 2. Independent maintained lifecycle

Lifecycle components establish and follow maintained state from the catalog in a
process that is not the adapter, without sequencer installation closures or
creator-local plans. Creation, changes, deletion, compaction, and protection
publication continue across adapter loss and recovery. Adapter DDL and lifecycle
publication commit as cooperating catalog writers. The adapter reads through the
query client with durable protection, and a cluster accepts its lifecycle
connection and query connections at once without one replacing the other's state.

Demonstrate: stop the adapter while maintained dataflows, sources, sinks, and
compaction continue, then restart it and resume queries. Include same-batch
dependencies and concurrent or delayed application of committed permission. One
adapter and one query client suffice.

#### 3. Independent query clients

Several query clients use the fast protocol without acquiring ownership of
maintained lifecycle. Catalog application and query readiness remain correctly
ordered. Responses, cancellation, query-local dataflows, and disconnect cleanup are
isolated between clients.

Demonstrate independent clients without a multi-adapter deployment. Cover one
client advancing or losing its protection while another retains an older
timestamp, recovery of the components enforcing compaction, and an expired client
returning. Together, these milestones complete the fresh-environment decoupling
outcome, subject to the correctness and performance acceptance criteria above.

Implementation history is in the [log](20260903_decoupled_coordination_log.md).
Workflow and current steering are in the
[implementer prompt](20260903_decoupled_coordination_prompt.md).

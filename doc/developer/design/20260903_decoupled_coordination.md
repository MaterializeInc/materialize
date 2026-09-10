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
clients without another ownership redesign. Enabling concurrent catalog writers
and deploying multiple adapters are not required here.

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

Committed permission bounds a dataflow's installation `as_of` from above. Installing
a maintained collection at an `as_of` beyond its committed permission discards
history without authorization, so `as_of` selection treats permission as a hard
constraint rather than waiting for permission to catch up. An MV's initial storage
visibility boundary remains a distinct concept. The representation and accounting
mechanisms for individual read requirements remain implementation choices.

Propagation to persist critical since handles must respect all valid read
requirements. Those handles are the durable backstop, not a substitute for
multi-client accounting.

Recovery must establish actual readability and restore valid read requirements
before further advancement is authorized. Reconstructed plans must use inputs
readable at the protected timestamps, rather than assume equivalent access paths
have equivalent history.

### Client read protection

Client protection is scoped to a client incarnation and the collections and
frontiers it requires. A client's read protection remains valid across restarts
or replacement of the components enforcing compaction. Durable client requirements
participate in compaction accounting alongside maintained requirements.

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

A saved index compaction bound is not a historical reconstruction guarantee.
Recovery selects installation frontiers from actual readability and all valid read
requirements, within committed permission. Reconciliation that discards existing
history requires committed permission, even when performed as part of reconstruction.

### Read-only prewarming

Preserve prewarming by following committed catalog permission independently of the
SQL savepoint. Read-only bootstrap installs within that permission and does not
wait for the writer to advance it. A local savepoint write grants no compaction
permission, and there is no startup-specific writer protocol or reconstruction
exemption.

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

#### 2. Catalog-driven maintained lifecycle

Cluster-side lifecycle components establish and follow maintained state from the
catalog without sequencer installation closures or the originating adapter.
Creation, changes, deletion, and compaction work across recovery. Losing the
adapter does not interrupt maintained work.

Demonstrate a production subscriber applying committed changes without
creator-local plans, including same-batch dependencies. Include concurrent and
delayed application of committed compaction permission.

#### 3. Independent query execution

Query clients use the fast protocol without acquiring ownership of maintained
lifecycle. Catalog application and query readiness remain correctly ordered.
Responses, cancellation, query-local dataflows, and disconnect cleanup are
isolated between clients.

Demonstrate independent clients without requiring concurrent catalog writers or
a multi-adapter deployment. Cover one client advancing or losing its protection
while another retains an older timestamp, recovery of the components enforcing
compaction, and an expired client returning. Together, these milestones complete the
fresh-environment decoupling outcome, subject to the correctness and performance
acceptance criteria above.

Implementation history is in the [log](20260903_decoupled_coordination_log.md).
Workflow and current steering are in the
[implementer prompt](20260903_decoupled_coordination_prompt.md).

# Decoupled coordination

## Outcome and scope

The catalog is the authority for maintained storage and compute lifecycle.
Cluster-side components subscribe to catalog changes and enact the desired
state, rather than depending on an adapter to send lifecycle commands. Adapters
write catalog changes and use a separate fast protocol for query execution.

The goal is decoupling, preserving consistency guarantees, performance, and
existing SQL behavior except for the [maintained-creation admission rules](#admission),
including the [fresh sink cutoff](#fresh-sink-cutoff), and the plan-selection and
introspection behavior of [written plans](#written-plans).
Losing an adapter must not disrupt maintained dataflows or other clients. Its own
queries may fail. Transparent query or session failover is out of scope.
For this deliverable, table and webhook ticking and shard finalization remain
adapter-owned and may pause while no adapter is live.

Active and prewarming deployments coexist as independent catalog participants.
Their adapters and lifecycle components are cooperating catalog writers, with
authority appropriate to each deployment. Arbitrary concurrently serving
adapters remain a destination, not this deliverable. The boundaries must
support them without another ownership redesign.

Initial implementation and validation target environments initialized under the
new protection rules. Native prewarming and handover between compatible
participating versions are in scope. Conversion of existing environments and
interoperation with binaries that lack this deployment model remain separate
rollout work, not prerequisites for the [milestones](#milestones).

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

## Deployment coexistence and native prewarming

Each deployment's replica set and read-only/output-write authority are durable
catalog state. Both active and prewarming deployments use native
catalog-following replicas throughout their lifetime. The pending deployment
hydrates its own replicas while the active deployment serves. Promotion changes
durable authority without switching execution models or discarding the state
warmed for takeover. An outside signal from the upgrade orchestrator authorizes
promotion. Neither joining the catalog nor observing hydration grants that
authority.

User objects, clusters, and declared cluster configuration live once in the
shared catalog. Actual replicas and deployment-specific hydration, scaling and
reconfiguration state are scoped to their deployment. Clients route to their
own deployment's replicas, and observations identify that scope. There is no
private durable SQL catalog per deployment. Written plans retain their
[build ownership](#written-plans).

Catalog participation is distinct from output-write authority. A read-only
deployment can publish its replicas, plans and protection without gaining the
right to write shared user data or maintained outputs. Promotion transfers
output authority and fences the retired deployment at the catalog and output
commit boundaries. Delayed followers must not permit both generations to write
the same output. Restart recovers the committed authority rather than inferring
it from process startup or connection order.

Protection accounts for all live deployments. Promotion preserves the warmed
deployment's requirements. Retirement or abandonment releases only that
deployment's resources under the existing protection rules, not another's holds
or shared user objects.

While versions coexist, shared catalog and Persist state must remain readable
and writable by every live participant. Writers preserve each other's state and
invariants, not merely accept each other's encodings. Incompatible records,
builtin schema changes and migrations wait until the affected older generations
are fenced. This is a contract for participating versions, not a requirement
that unmodified pre-feature binaries understand the deployment model.

## Approach

Use catalog implications to derive lifecycle effects from committed catalog
state. Complete the relevant implication paths as needed for this work. Move
responsibility for enacting those effects to cluster-side components that can
recover and follow catalog state independently of the originating adapter.
Controllers can remain useful abstractions. Internal factoring is an implementation
choice within the agreed [lifecycle placement](#lifecycle-placement).

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

This places maintained read requirements, retention policies, and permission to
discard history under the same durable authority. Cluster-side components have an
explicit limit to enforce and recover, without treating a process's local
accounting as the authority to advance beyond it. The cost is ongoing catalog
traffic and processing proportional to changing bounds and publication cadence,
accepted over the
[delegated alternative](#delegated-compaction-advancement). Coalescing or
rate-limiting advancement may retain extra history, but must not delay protection
until after it is needed. Choose cadence and batching from measured catalog load,
DDL latency, and retention cost. Publication work scales with what changed, not
with catalog size.

### Object-owned retention

A maintained object's retention policy constrains authorized compaction of its
retained state and the logical inputs needed to reconstruct that history,
independently of adapter, query-client, or replica lifetimes. This includes
indexes with no replicas. Client and execution holds impose additional
constraints. Reclaiming an incarnation does not remove the object's retention
requirement.

An index's object-owned requirement applies its retention policy to durable
progress of its logical inputs, with or without replicas. Live replicas also
protect their running indexes' retention windows relative to execution progress.
That history must remain admissible for new reads. Protecting only unfinished
execution is insufficient. Lagging replicas may retain extra history. Zero replicas
must neither release the object-owned retention requirement nor freeze its
advancement from input progress.

If progress cannot be established, retain the existing protection. With no
components running, advancement may stop conservatively. Recovery must preserve
history still required by the policy. This neither pins creation-time history
forever nor restores history already discarded before protection was established.

Replica-local compute logs remain volatile. Their retention policies, including
those of indexes over them, apply within the replica incarnation and do not
promise reconstruction after loss of the replica. This work does not persist
those logs. Persisted system-catalog collections retain the normal guarantees.

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
A publisher proposes bounds only from a state in which it has applied every
committed change up to its transaction's base, and recovery restores maintained
state before publication or reclamation resumes.

### Logical recovery dependencies

Maintained recovery protection covers all logical collection inputs, including
those eliminated by optimization. Unmaterialized views do not form recovery
boundaries. Persisted inputs do: reading an upstream MV protects its output,
while that MV's maintenance separately protects its own inputs. Indexes remain
replaceable access paths, not the authority for recovery dependencies.

[Selected written plans](#written-plans) also carry catalog-owned protection for
actual imports, including other indexes, independently of adapter or replica
lifetimes.

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

Installation `as_of` selection respects committed permission, with the index
contract described in
[Index creation and reconstruction](#index-creation-and-reconstruction). An MV's
initial storage visibility boundary remains distinct. Representation and
accounting mechanisms are implementation choices.

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
for logical inputs and actual plan imports, not lagging physical compaction.
Automatically selected creation timestamps must be compatible with those inputs.
Explicit historical refresh requests are rejected if any logical input cannot
support them, even when optimization removes that input.

### Visibility and execution

Catalog commit, cluster application, and query readiness are different events.
Queries must not fail spuriously or execute against the wrong object state
because the fast protocol overtakes catalog application. Preserve existing
behavior for concurrent DDL, transactions, cancellation, and object drops.
The [catalog-ordering appendix](#appendix-catalog-freshness-and-execution-ordering)
describes the corresponding requirement for independently following clients and
replicas.

The catalog determines index candidates and transaction eligibility.
Installation or hydration must not make an index appear or disappear from that
logical view. Read acquisition establishes a justified protected frontier
before fixing the transaction's timestamp and time domain, without requiring
index installation. SELECT and nonexecuting EXPLAIN share this protection
machinery and preserve their existing transaction effects. EXPLAIN can return
with zero replicas, while actual execution checks import readability and waits
for required progress. With no intervening DDL, physical readiness alone cannot
break repeated logical reads.

Catalog certification and readiness waits remain cancellable and preserve
statement-specific timeout semantics. They do not impose a common deadline on
previously unbounded session controls, transaction controls or DDL. Submitted
writes still require a definitive result before reporting cancellation or
timeout.

Query-client connections must not replace one another's desired state or reset
maintained dataflows. Lifecycle ownership and permission to perform external
writes must remain safe across restarts and handover, independently of query
connection lifetime.

### Observability

Metrics follow the responsibilities they describe. Retire accounting for removed
controller queues, transports and command paths rather than recreating those
components for metric parity. Meaningful observations, including public lag
reporting, frontiers, hydration and cleanup, remain at their owning boundaries.
Preserve autonomous curated-metric emission during adapter absence. Missing
observations are not zero values or execution prerequisites.

## Selected implementation decisions

These choices resolve implementation questions raised by the requirements above.
They constrain the relevant paths without prescribing their internal mechanisms.

### Fresh sink cutoff

Fresh sinks select their automatic cutoff against committed input permission,
including with `SNAPSHOT = false`. They cannot rely on history merely because
physical compaction lags. Existing sinks retain pending output through alteration
and recovery. Their requirements advance with durable output progress, not merely
with input compaction permission.

### Index creation and reconstruction

The index-creation catalog transaction commits the selected plan, an admitted
initial `as_of`, and protection on logical inputs and actual plan imports.
This establishes the initial index compaction bound and a frontier against which
clients can acquire holds before replica installation, including with zero
replicas. The frontier is justified by the protected inputs, not a placeholder
`MIN`. No extra pre-installation commit or replica acknowledgment is required.

Installation and reconstruction honor committed frontiers and valid read
requirements, including through imported indexes. Protection advances with
[object-owned retention](#object-owned-retention) and recovery needs rather than
permanently pinning the initial `as_of`. A past bound is not a promise to
reconstruct history no longer required. Live execution holds continue to protect
existing traces.

### Read-only prewarming

Prewarming uses
[deployment-scoped replicas](#deployment-coexistence-and-native-prewarming).
They follow committed definitions, written plans and compaction permission for
their deployment. Read-only bootstrap follows
[Index creation and reconstruction](#index-creation-and-reconstruction) without
depending on the serving adapter to grant permission. A SQL savepoint or
adapter-local read-only setting grants neither compaction permission nor
output-write authority.

### Lifecycle placement

Following and enactment run in clusterd, at the replica, for compute and
storage alike. Each replica follows the catalog for its cluster and deployment
and reconciles itself: it installs from written plans or source and sink
definitions, applies committed bounds, and proposes bounds from its own
progress. There is no lifecycle connection: the fast protocol is the only
protocol, and nothing sends maintained installation commands. Which replica
serves a request and how replicated responses are merged belong to the query
client. Environment-wide storage accounting dissolves along the way: critical
since handles follow committed bounds, table registration is adapter-owned, and
shard finalization applies committed retirement permission idempotently.
Adapters perform finalization for this deliverable. Creating replica processes
stays with envd for now. DDL and table appends are request-scoped and stay with
adapters.

A replica's execution reads and live-index retention windows use incarnation-scoped
client protection. A slow or hydrating replica keeps the input history it needs
until its protection is released or reclaimed. These requirements are additional
to maintained recovery requirements and object-owned retention, which do not
expire with the replica.

### Written plans

The writer that commits a maintained object also writes its optimized plan and
notices, per build. Lifecycle components install what is written and do not plan.
EXPLAIN and `mz_notices` read and hold that written state. DDL that removes
something a written plan imports rewrites the affected plans of the writer's own
build in the same transaction without reinstalling the running dataflows. Other
builds rewrite theirs when they observe the change, and a plan whose import is
gone is not installable until then. No build writes another's plans. A new index
does not change existing plans. A new generation's adapters write plans for their
version before that generation's components install.

Selecting or rewriting a plan admits its import protection in the same catalog
transaction. Protection follows the selected plans and their required frontiers
across live builds and deployments, while running executions retain their own
holds. Replacing or retiring a selection must preserve valid protection.
This does not make cross-build plan repair a synchronous DROP barrier.

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

Catalog writes validate deployment membership and the operation's authority at
commit. Active and prewarming generations participate under their own
identities without fencing one another by joining. Promotion revokes the
retired generation's write authority without invalidating the warmed
generation's protection. Persist compare-and-append is the commit authority:
metadata-only writes such as protection and heartbeats retry on contention,
while DDL refreshes and revalidates and reports a planning conflict when
structural changes invalidated it rather than merging. Each writer follows the
durable stream it commits to. Cooperating writers do not fence one another with
per-process epochs.

Persist critical since handles follow the committed bound only. Every valid
read requirement is in that bound, so applying it is monotone and needs no
per-process opaque. Local hold accounting does not drive critical handles.
Prewarming client protection belongs to its own deployment and participates in
the shared bound. Where concurrent enactment is unsafe, enforce authority at
the affected output's write boundary rather than treating catalog membership as
exclusive ownership.

## Alternatives

### Plan-specific recovery protection

Protecting only physical inputs ties retention to optimizer output. Controller
dependency holds implicitly do this, with their aggregate effect preserved in
persist sinces. Bootstrap rebuilds holds from physical plans and durable storage
frontiers, but cannot restore discarded history for an input introduced by
replanning.

A plan-specific design needs a recovery contract beyond an input-ID set, such as
durable logical recovery expressions with upgrade compatibility. We choose
logical-input protection for recovery, supplemented by protection of selected
plans' actual imports. Physical dependencies alone do not define recovery.

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
recovery semantics and policy-based retention without pinning creation-time history
for indexes or metric sinks.

Evidence includes actual compaction and an input eliminated by optimization,
fresh builtin initialization, and same-version recovery including MVs reading
system-catalog collections. Measure publication and retained-history costs as the
real path becomes available. This milestone can use the current single-writer
arrangement.

#### 2. Independent maintained lifecycle

Replicas establish and follow their cluster's maintained state from the catalog,
without sequencer installation closures, creator-local plans, or a controller
sending installation commands. Enactment of committed creations, changes, and
deletions, together with compaction and protection publication, continues across
adapter loss and recovery, subject to the adapter-owned work in
[Outcome and scope](#outcome-and-scope).
Adapter DDL and replica publication commit as cooperating catalog
writers. The adapter reads through the query client with durable protection,
straight to replicas. Compute is the first working slice within this milestone,
storage follows on the same path, and neither is complete without the other.

Demonstrate: stop the adapter while maintained dataflows, sources, sinks, and
compaction continue, explicitly allowing table- and webhook-fed work to pause.
Restart a replica during the outage and show that it reconstructs and
progresses without the adapter, rather than relying on a surviving sibling's
progress. Then restart the adapter and resume queries. Include a multi-replica
cluster with one replica still hydrating, same-batch dependencies, and
concurrent or delayed application of committed permission. One adapter and one
query client suffice for this outage demonstration. Verify policy-based
retention independently of live client or replica grants, including their
reclamation and subsequent reconstruction.

Native deployment handover is also part of this milestone. Prewarm a second
deployment on its catalog-owned replicas and show that externally authorized
promotion retains warmed execution while transferring write authority safely.
Compatible participating versions must coexist as catalog writers without
premature shared-state migration. This requires active/prewarming overlap, not
arbitrary concurrently serving adapters or a general upgrade-version matrix.

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

## Appendix: catalog freshness and execution ordering

Strict serializable queries must observe catalog changes completed before they
start, across adapters, and execution must not overtake required replica-side
application. This includes configuration, not just object definitions.

### Catalog writes

All catalog writers, including protection, heartbeat and compaction publishers,
share the `EpochMilliseconds` oracle's allocation, completion and
future-timestamp discipline. Retries and rebasing must preserve it before the
write becomes durable. A commit at C is acknowledged only after oracle
completion covers C, directly or through a barrier at least C.

This coordination cost is accepted. Batching remains an implementation choice.
Replicas write independently, not through the adapter or table-write worker.

Inherited oracle/catalog progress and its necessary monotonic advancement retain
log-and-proceed behavior. A future timestamp already inherited from that state
must not block bootstrap or heartbeats. Genuinely new future jumps remain subject
to precommit protection, including when advancing an empty catalog upper. This
does not add clock synchronization or new clock-recovery guarantees.

Clock fixtures may explicitly enable a fixture-owned shared mock clock for
catalog/oracle allocation and checks, supporting clock changes and restarts.
Source clocks, Persist lease clocks and scheduling timers remain unchanged.
The fixture clock is not a production clock service or a timestamp-policy bypass.

### Planning and catalog freshness

A current-data strict serializable statement selecting its own timestamp T on
`EpochMilliseconds` uses an immutable catalog snapshot validated for that query
at T. Establish freshness within the operation's real-time interval, reusing
the data-read oracle call where valid, and certify a complete catalog prefix
through T. Local revisions or object existence alone do not prove this.

Validate the definitions, permissions and configuration the query uses. Refresh
and replan on relevant changes. If relevant snapshot state is newer than T,
select a compatible timestamp through the oracle before execution.
Catalog-derived errors also need freshness, even before the data timeline is
known.

For a data timestamp fixed by a transaction or `AS OF`, keep fresh catalog
visibility per statement without moving that timestamp. Incompatibilities
follow existing conflict/error behavior, not historical name resolution or
permissions. Other isolation levels and data timelines retain their data
timestamp rules, with catalog freshness established separately on
`EpochMilliseconds`. No historical-catalog service is required.

### Replica admission

Requests carry a catalog position covering their validated definitions and
configuration, scoped to the catalog history and deployment fence.
Progress-only publications neither invalidate planning nor impose
definition-application waits. Their protection constraints remain binding.
A reconstructed snapshot may conservatively require one initial prefix catch-up.

The receiver waits for required state and configuration to be applied, not
merely read, then checks readiness and read protection for the imports. A newer
receiver does not rewind: stable object identities and existing
concurrent-DDL/readability checks determine whether it can execute the plan or
must report an error. Waiting is cancellable and may expedite the receiver's
own catch-up, without blocking that work or giving clients lifecycle authority.

No all-replica DDL barrier, unrelated hydration wait or per-query durable
record is required. Existing asynchronous MV replacement and startup-only
parameter semantics remain unchanged. Token and wakeup mechanics are
implementation choices.

Resume from the [current handoff](20260903_decoupled_coordination_log.md) and
[implementer prompt](20260903_decoupled_coordination_prompt.md). The
[archived log](20260903_decoupled_coordination_log_archive.md) is historical
context, not required reading or current steering.

# Decoupled coordination

## Outcome and scope

The catalog is the authority for maintained storage and compute lifecycle.
Cluster-side components subscribe to catalog changes and enact the desired
state, rather than depending on an adapter to send lifecycle commands. Adapters
write catalog changes and use a separate fast protocol for query execution.

The goal is decoupling, preserving consistency guarantees, performance, and
existing SQL behavior except for the [maintained-creation admission rules](#admission),
including the [fresh sink cutoff](#fresh-sink-cutoff), and the plan-selection and
introspection behavior of [written plans](#written-plans). M3 adds the
[adapter lifecycle](#adapters-and-process-management-m3) and
[multi-producer introspection ownership](#shared-maintenance-and-introspection).
Losing an adapter must not disrupt maintained dataflows or other clients. Its own
queries may fail. Transparent query or session failover is out of scope.
Table and webhook ticking and shard finalization remain adapter-owned and may
pause while no eligible adapter is live.

Active and prewarming deployments coexist as independent catalog participants.
Their adapters and lifecycle components are cooperating catalog writers, with
authority appropriate to each deployment. M3 separates process management into
`controllerd` and SQL serving into independently managed `adapterd` instances.
Multiple adapters serve the same deployment without owning maintained lifecycle
or depending on a designated serving adapter for shared management.

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

Native warm handover with replica-targeted MVs on managed clusters is outside
these milestones. Reject that combination before promotion, leaving the active
deployment intact, without retargeting or dropping the MVs. Preserve single-deployment
behavior and handover support for pins to explicitly declared replicas on
unmanaged clusters. The broader
[replica-targeted MV semantics](#future-work-replica-targeted-mv-semantics)
are deferred.

Query-local dataflows that do not go through the catalog, including slow-path
SELECTs, SUBSCRIBEs, and COPY TO, remain on the fast protocol. Their creation,
execution, responses, and cleanup are part of request-scoped execution, not
durable catalog lifecycle.

## Deployment coexistence and native prewarming

Each deployment's adapter and replica realizations and read-only/output-write
authority are durable catalog state. Identity is scoped by deployment, not binary
version. Active and prewarming deployments use native catalog-following replicas
throughout their lifetime. The pending deployment hydrates its own replicas while
the active deployment serves. Promotion changes
durable authority without switching execution models or discarding the state
warmed for takeover. An outside signal from the upgrade orchestrator authorizes
promotion. Neither joining the catalog nor observing hydration grants that
authority.

User objects, clusters, declared configuration and explicit adapter and replica
declarations live once in the shared catalog. There is no private durable SQL
catalog per deployment. M2 needs one deployment-owned adapter realization. Named
adapter declarations, SQL management and multiple serving adapters belong to M3.
Written plans retain their [build ownership](#written-plans).

`ReplicaId` identifies a logical replica, not a deployment-local process. Shared
declarations and deliberate carryover preserve that ID across deployments,
including in public replica IDs. Runtime identity includes the deployment.
Equal names do not establish correspondence. Independently created replicas and
DROP/recreate get fresh IDs, even when names match. This does not promise ID
stability across a logical replica replacement.

Deployments may have different replica sets. For example, both can realize
`u12`, while only the prewarming deployment creates `u13` for a hydration burst.
This does not require a shared declaration or a corresponding replica in every
deployment for each policy-created replica. Actual configuration, hydration,
burst state and reconfiguration progress are deployment-local. Shared policy
does not let one deployment's readiness satisfy another's or end its burst.
Clients route to their own deployment's replicas. Cross-deployment observations
and cleanup distinguish realizations by deployment and replica ID. Retiring a
realization must not drop a shared declaration or its dependents. These identity
and membership contracts are part of M2, independent of deferred MV pinning.

For these milestones, `mz_cluster_replicas` remains a shared, materializable
relation showing the catalog's active deployment, even through a prewarming
adapter. Routing, reconciliation and prewarming readiness use their own
deployment's inventory, not this public filter. Raw catalog access retains all
deployments. [Query-relative public visibility](#future-work-deployment-relative-catalog-views)
is deferred.

Catalog participation is distinct from output-write authority. A read-only
deployment can publish its replicas, plans and protection without gaining the
right to write shared user data or maintained outputs. Promotion commits write
enablement in the catalog. Restart recovers that state rather than inferring it
from process startup or connection order.

Compatible deployments may write the same Persist output concurrently, including
late writes from a retired deployment. Persist compare-and-append and each
collection's write protocol must preserve correctness at every readable
timestamp, not merely converge eventually. Promotion need not wait for old
Persist writers to stop or for every output to complete a handover.

Catalog fencing, read protection and external-sink transaction/version checks
remain required.

Protection accounts for all admitted deployments. Promotion preserves the warmed
deployment's requirements. Durable retirement revokes the retiring deployment's
catalog authority and releases its query and execution protection without waiting
for physical process death. Shared declarations, maintained requirements and other
deployments' protection remain intact. Physical cleanup may follow asynchronously.
Cancelling an abandoned prewarm uses explicit deployment retirement, not expiry.

### Version compatibility

Catalog admission and retirement determine the versions that shared catalog and
Persist state must support. Reuse deployment membership with the version
information needed for that decision, not a separate liveness registry. Writers
preserve all admitted participants' encodings and shared collection semantics.
Unsupported participants are not admitted.

Shared Persist shards use a supported state-format target distinct from the
actual binary identity. This covers first initialization, even through read-only
metadata access, as well as subsequent writes and maintenance. Bootstrap and
restart must honor the catalog's policy before such access. A newer initializer
must not make a shard unreadable to an admitted older participant. An older
version label must not conceal incompatible state or behavior.

Formats may advance once durable catalog retirement removes the older
compatibility requirement. Admission and advancement must be ordered so no
admitted participant loses compatibility. Do not readmit an incompatible older
version by lowering the label on already-written state. Retired zombies may
still complete compatible operations or fail on unsupported state. Advancement
does not wait for physical process death.

Incompatible records, builtin schema changes and migrations still wait until
the affected older generations are fenced. This is a contract for participating
versions, not a requirement that unmodified pre-feature binaries understand the
deployment model.

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

Periodic publication combines changed client and maintained requirements with
newly permitted bounds in one transaction, calculating permission from the
resulting candidate. On-demand grants must not postpone maintenance, and
unavailable bound proposals must not block safe requirement advancement.

### Object-owned retention

A maintained object's retention policy constrains authorized compaction of its
retained state and the logical inputs needed to reconstruct that history,
independently of adapter, query-client, or replica lifetimes. This includes
indexes with no replicas. Client and execution holds impose additional
constraints. Retiring a realization does not remove the object's retention
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
requirements, and retires client protection, not at the act of applying
already-committed permission. Proposing bounds need not require a leader either,
provided catalog transactions validate proposals against authoritative requirements.
A publisher proposes bounds only from a state in which it has applied every
committed change up to its transaction's base, and recovery restores maintained
state before publication or retirement resumes.

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
own accounting until they are removed. Retiring a client's protection therefore
needs no fence propagated to compute: a retired client's late read is either
served correctly or refused, and it cannot acquire new protection.

Recovery must establish actual readability and restore valid read requirements
before further advancement is authorized. Reconstructed plans must use inputs
readable at the protected timestamps, rather than assume equivalent access paths
have equivalent history.

### Client read protection

Query and execution protection belongs to a deployment-qualified adapter or
replica realization and the collections and frontiers it requires. It does not
expire through silence or missed heartbeats. A dead but still-declared owner may
retain history indefinitely until replacement or removal. Process supervision
owns recovery, not a generic catalog-client liveness registry.

A realization has one current execution. Restart replaces that execution, not the
logical declaration. Replacement retires obsolete transient requirements while
securing the protection its successor needs. Superseded executions cannot publish
protection or release successor grants. They stop rather than register themselves
again. This is local to the realization, not a fence on cooperating catalog writers.

DROP, scale-down and deployment retirement release the affected realizations'
protection. Removing a shared declaration removes all its realizations. Authority
revocation and protection release are coordinated at the catalog transaction
boundary, so stale publications cannot restore retired protection. Retirement may
fail the old execution's queries. It does not require draining or transparent
session failover.

Protection survives replacement of the components enforcing compaction. Durable
client requirements participate alongside maintained requirements. A requirement
on an index protects its inputs at that frontier, since arrangements do not
survive compute restart and must be reconstructible.

Clients may aggregate query and transaction needs locally under established
protection. Ordinary query execution must not require a durable write for each
hold change. Establishing protection and retiring it must be coordinated with
compaction so that neither a new requirement nor a stale client can rely on
discarded history.

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

### Kafka placement and recovery

Kafka sinks run on one selected replica in the active deployment. Restart recovers
on that replica. Moving to another replica follows catalog membership changes,
not missed heartbeats or client-incarnation age. Prewarming replicas must not
compete for output ownership or block the active deployment's selection.

Producer opening and reopening check current catalog authority for the deployment,
realization and execution. Admission is a point-in-time check, not a timed lease.
Kafka's per-sink transactional identity, producer fencing, committed-progress
recovery and sink-version checks preserve output correctness. An already-approved
old attempt may fence its replacement after a pause, causing a transient restart,
but cannot obtain fresh admission after retirement. No guarantee of
interruption-free takeover or wait for old process death is required.

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
M3 places process management in controllerd, not database progress or shared
maintenance. DDL and table appends stay with adapters, alongside
[cooperative maintenance](#shared-maintenance-and-introspection).

A replica's execution reads and live-index retention windows use its
[realization-owned protection](#client-read-protection). A slow, hydrating or
stopped replica retains that history until release, replacement or removal.
These requirements are additional to maintained recovery requirements and
object-owned retention, which do not end with the replica.

### Adapters and process management (M3)

Split environmentd into adapterd for SQL serving and controllerd for catalog-driven
process management. One controllerd manages each deployment identity, not each
distinct binary version. Within a deployment, controllerd, adapterd and clusterd
use the same release. Old and new deployments have their own controllers,
adapters and replicas. Controllerd uses the local-process/Kubernetes backends to
reconcile its deployment's committed inventory with matching launch configuration.

Adapterd runs the existing cluster-controller policy, including replica
declaration realization, burst scaling, reconfiguration and ON REFRESH scheduling.
Adapters use deployment-local observations and commit decisions with catalog
precondition checks before provisioning is authorized. Concurrent attempts
reobserve after conflict, without a designated policy adapter. Missing
observations do not establish readiness or justify scaling to zero.

Controllerd owns deployment initialization and process management, not replica-set
policy, SQL coordination, maintained-dataflow installation or shared maintenance.
Adapters discover and query replicas without lifecycle RPCs or provisioning
privileges. Losing or restarting controllerd leaves healthy children running.
Provisioning and recovery of failed processes may wait for its return, but
running SQL, maintained execution and cooperative maintenance do not depend on it.

The external launcher/operator launches controllers, authorizes promotion,
manages routing and completes physical teardown after durable deployment
retirement, even if the retired controller is gone. Starting a deployment's
controller before its children does not require upgrading the old deployment's
controller in place. Controller startup alone neither promotes a deployment nor
authorizes incompatible catalog or Persist-format advancement.

Initialization establishes a joinable deployment, including shared catalog and
table infrastructure. A fresh environment gets a default adapter declaration so
the first SQL endpoint needs no SQL bootstrap. That default is initialization-only
and is not recreated after explicit removal. An adapter joins its specified
deployment without repeating initialization, resetting shared tables or promoting
itself. Build-owned planning remains with adapters under
[written plans](#written-plans), without a permanent planner leader.

SQL supports `CREATE ADAPTER adapter_name (SIZE '<size>')`, `DROP ADAPTER` and
`ALTER ADAPTER ... RENAME TO ...`. SQL adapter management requires a superuser.
Creation acknowledges the catalog commit, not process readiness. Catalog
relations list declarations, requested sizes and deployment-qualified endpoints,
readiness and provisioning errors. Local sizes specify CPU and memory allocations
using the backend's resource controls, with
enforcement limitations explicit. Dropping the serving adapter or the last
declared adapter is allowed. Self-drop may disconnect the issuing session before
acknowledgement. Zero adapters is a valid state, not a reason to reinitialize the
environment. With no adapters running, replica-set policy may pause while
controllerd continues maintaining the committed process inventory.

Adapter declarations have stable IDs independent of names. Rename does not change
process identity or require a restart. Realizations are deployment-qualified.
Each realization has its own execution and protection. Replacing one adapter
does not replace a peer. Temporary objects record enough durable execution
ownership for targeted cleanup when temp DDL occurs, without a catalog record
for every connection.

Controller-published endpoint and status describe the realization, separately
from user intent. The endpoint is a local pgwire address or a backend-appropriate
routable address. An assigned endpoint does not establish readiness, and observed
readiness does not guarantee a future connection.

Cancellation is endpoint-local in M3. Shared activity exposes adapter ownership,
and identifiers must be unambiguous within their scope. Query-local state and
cleanup cannot collide across adapters. Connection and concurrency admission
limits are per-adapter, while catalog object quotas remain shared. Cross-adapter
cancellation routing and distributed connection quotas are outside M3.

Controllerd exposes pretty-printed JSON over HTTP for inspection and visualization:
its desired adapterd and clusterd inventory used by reconciliation, including
processes not yet running, their identities, deployments, resources, endpoints
and observed status. This is a view of desired state, not a second authority or a
dump of credential-bearing launch arguments. Keep desired-state calculation
separate from enactment. A passive exporter consumed by a Kubernetes controller
is future work, not required for M3.

Controllerd also exposes an administrative HTTP API to list, create and drop
adapter declarations, including when no adapter is running. Creation accepts the
same name and size as SQL. Listing includes deployment-qualified endpoints and
observed status. Mutations use the same catalog operations and validation as SQL,
respecting deployment authority. Success acknowledges the catalog commit, not
process readiness or completed teardown. This is catalog administration, not a
SQL proxy or a direct process-command path.

`GET /api/collections` lists catalog-known Persist-backed collections as
pretty-printed JSON, including system collections. Each entry has its catalog
collection `id`, qualified `name` where available, and the backing shard's observed
`since` frontier. Preserve unnamed entries with a null name. Report the frontier
as an array, or null when unavailable, not the catalog's compaction permission.
This is an observation, not a read hold or an atomic snapshot across shards.
Optional `id` and `name` URL parameters use case-sensitive SQL LIKE patterns:
`%` matches any sequence, `_` one character, and backslash escapes literals.
Both filters must match when supplied together. Apply filters before observing
shards. The endpoint works without a serving adapter and adds no protection,
compaction or background-maintenance responsibilities to controllerd. Include
browser and curl examples with correct URL encoding in the local startup recipe.

The HTTP listener accepts requests without in-process authentication or caller
authorization. Access control belongs to the external system exposing the port.
Anyone with direct access can manage adapters. This does not bypass catalog
validation or deployment fencing. M3 adds no controller authentication layer.

### Shared maintenance and introspection

No controllerd or designated serving adapter owns shared database progress.
Any authorized adapter may advance idle table or webhook uppers. Coalesce toward
required progress and skip empty advancement already accomplished by another
writer, including after conflict. Table operations retain txn-WAL and oracle
ordering. Legitimate multiwriter contention is not evidence of a failed process,
and an empty tick's no-op rule does not acknowledge an uncommitted user write.

Any authorized adapter may finish committed shard retirement. Physical
finalization requires catalog-certified eligibility and definitive txn-WAL
Forget, including application of preceding writes. Local adoption or detachment
of handles does not retire shared registration. Concurrent attempts must be safe
without a singleton finalizer.

Introspection follows the producer of each fact:

| Facts | Owner |
| --- | --- |
| Replica compute/storage observations | The replica, not duplicate adapter relay collectors |
| Sessions, queries and webhook contributions | The adapter execution |
| Catalog-derived facts | Catalog-derived relations, using raw-catalog views where possible |
| Provisioning and process observations | The deployment's controllerd |
| Shared reductions and sampling | Maintained SQL where appropriate, or concurrency-safe cooperating adapter work |

Joining an adapter neither duplicates shared facts nor retracts peer rows.
Current-state publications distinguish realization and execution so late
predecessor writes cannot become successor state. After replacement, current
observations may be unknown until the successor publishes. Published cumulative
counters and retained history survive replacement and retirement under their
existing retention semantics, rather than following active-row cleanup.
Preserve public relation contracts through owner-qualified internal data where
possible.

For remaining adapter-published catalog projections, M3 permits originator-only
publication: only the successful committer publishes its commit's derived rows.
Followers still apply catalog state and required effects, but do not republish
those rows. Fresh initialization seeds them. Joining and restarting adapters
neither reset shared tables nor replay full positive snapshots.

This publication is neither atomic with DDL nor ordered across originators.
Concurrent publications can arrive out of catalog order, and publication failure
can leave missing or inconsistent rows indefinitely, including unmatched
retractions. This accepted M3 limitation is confined to these catalog projections,
not user-data writes, protection or runtime/session ownership. Catalog-backed
conversion is separate work. M3 adds no publication watermark or recovery protocol.

Catalog-driven Paused/Dropped history, including synthetic per-replica Paused
markers, is also published only by the successful committer. Followers and
join/restart reconstruction do not manufacture these historical transitions.
M3 accepts missing events after publisher failure and unordered publication across
commits for this history only. Current object existence and zero-replica placement
come from the catalog, independently of history delivery. Transactionally recorded
lifecycle events, potentially through the audit log, are a longer-term direction.

Physical Persist progress and usage require actual observations, not catalog
compaction permission. Sampling and pruning retain their concurrency checks.
Reuse already-safe collectors, including hydration history, rather than rewriting
all introspection. Expensive scans consult shared completed-work progress and
stagger attempts. Occasional redundant measurement is acceptable, but duplicate
logical publication or double retraction is not. This requires neither
exactly-once task execution nor a general background-job or leader-election system.

### Persist pubsub

An environment-scoped pubsubd serves all adapters and replicas across deployments,
at an endpoint managed by the external launcher/operator. It outlives individual
deployments. Reuse the existing relay and remote clients, with one shared fanout
service rather than unrelated servers that split subscribers.

Pubsub is a best-effort accelerator for durably committed Persist diffs, not a
durable authority. Loss and reconnect use the existing durable fallback. Outages
may increase latency and backend traffic but must not prevent progress or gate
serving on relay availability.

The relay envelope must interoperate across the supported coexistence versions.
Payload encoding remains governed by the catalog's
[Persist-format policy](#version-compatibility), not the relay's binary version.
The launcher can upgrade pubsubd ahead of new SQL children while serving existing
clients. Verify supported old/new clients against the relay across that rollout.
Neither a separate catalog-compatibility system nor pubsub HA machinery is
required for M3.

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
retired generation's catalog-write authority without invalidating the warmed
generation's protection. Persist compare-and-append is the commit authority:
metadata-only writes such as protection and compaction retry on contention,
while DDL refreshes and revalidates and reports a planning conflict when
structural changes invalidated it rather than merging. Each writer follows the
durable stream it commits to. Cooperating writers do not fence one another with
environment-wide process epochs. Supersession of a realization's own execution
follows [client read protection](#client-read-protection).

Conflict recovery applies committed diffs incrementally. Catch-up and transaction
repreparation scale with changes and affected dependencies, rather than rebuilding
or decoding the whole catalog on each retry. Retry against refreshed, validated
state, including recomputing compaction proposals when their inputs change.

Catalog upper movement is not itself a planning conflict. Client-hold downgrades,
maintained-requirement frontier advancement and compaction-bound updates leave
definitions and selected plans unchanged. Retain valid DDL preparation and input
holds across these conflicts, recomputing affected admission and cleanup decisions
against the refreshed prefix rather than replaying stale diffs. Committed catch-up
and its effects must survive even if the candidate fails.

For ordinary MV creation, frontier movement may change the admissible initial
`as_of` and corresponding protection, not the optimized plan. Definition, selected
plan, birth and protection commit consistently. The catalog commit timestamp is
distinct from the MV's `as_of`, and fixed timestamps, including required REFRESH
times, cannot slide to make a retry succeed. Structural changes still require
revalidation, replanning or a conflict, and loss of execution authority is terminal.

Stagger periodic publications across participants, including at startup. Conflicts
permit bounded immediate retry work, followed by randomized backoff. Retry delays
and repeated attempts must not monopolize the coordinator: pending operations
leave it available for other requests, cancellation and maintenance. Where retry
suspension releases local DDL admission, retain preparation and reacquire and
revalidate before resuming. Preserve completion ownership and resolve submitted
writes before releasing admission. Scheduling and task placement remain
implementation choices. New protection still commits before use, and delayed
advancement retains committed protection.

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
compaction work continue, explicitly allowing table- and webhook-fed work to pause.
Compaction respects the stopped adapter's durable holds until replacement or
explicit retirement. Silence must not release them.
Restart a replica during the outage and show that it reconstructs and
progresses without the adapter, rather than relying on a surviving sibling's
progress. Then restart the adapter and resume queries. Include a multi-replica
cluster with one replica still hydrating, same-batch dependencies, and
concurrent or delayed application of committed permission. One adapter and one
query client suffice for this outage demonstration. Verify policy-based
retention independently of live client or replica grants, including their
explicit retirement and subsequent reconstruction. Verify same-deployment
replacement and deployment retirement without heartbeat expiry, preserving other
owners' protection and safe Kafka recovery. Adapt existing coverage at these
boundaries rather than adding a separate acceptance campaign.

Native deployment handover is also part of this milestone. Prewarm a second
deployment on its catalog-owned replicas and show that externally authorized
promotion retains warmed execution and enables writes safely under the existing
concurrent-writer protocols. Compatible participating versions must coexist as
catalog writers without premature shared-state migration. This requires
active/prewarming overlap, not arbitrary concurrently serving adapters or a
general upgrade-version matrix.

#### 3. Independent adapters and process management

Deliver [adapterd and controllerd](#adapters-and-process-management-m3), with
environment-scoped [pubsubd](#persist-pubsub) and multiple adapters serving SQL,
including DDL and writes, against shared replicas.
Preserve cross-adapter catalog freshness, write ordering and independent client
protection. Responses, cancellation, query-local dataflows and disconnect cleanup
remain isolated. No serving adapter owns shared process management.

Provide `bin/controllerd --optimized` as the local development entry point.
With documented metadata-store prerequisites in place, it builds the required
binaries, configures the local environment, starts or connects to external pubsub,
and launches controllerd. Controllerd provisions the catalog-declared adapters
and replicas, including the default adapter on fresh initialization. Include a
short recipe for startup, SQL/HTTP access and stopping/restarting without resetting
state. Exercise the local demo through this entry point, not only a private harness.

Build through these slices, preserving working outcomes as the split progresses:

1. **One default adapter.** Establish stable adapter identity and separate
   initialization from join. Extract deployment-scoped process management and
   environment-scoped pubsub, preserving M2 behavior.
2. **Correct independent adapters.** Isolate executions, protection and cleanup.
   Remove shared bootstrap resets and repair conflicting introspection writers.
   Make table writes, maintenance and replica-set policy cooperate without a
   privileged adapter. Keep policy in adapterd and process enactment in controllerd.
3. **SQL/HTTP-managed lifecycle.** Expose SQL creation, listing, rename and drop,
   plus controllerd HTTP listing, creation and drop, with resource sizes,
   endpoints and readiness through the same catalog and reconciliation path.
   Include the filtered durable-collection inspection endpoint.
4. **Deployment coexistence and backends.** Exercise old/new deployment
   controllers with shared pubsub, externally authorized promotion and retired
   deployment cleanup. Complete local-process and Kubernetes integration through
   their existing backends and external lifecycle owner.

Demonstrate SQL creation, listing, rename and drop of adapters, endpoint discovery
and the HTTP inventory. Use two adapters concurrently, kill one, and show
controllerd restoring it while the other continues serving against the same
replicas. Creation and recovery must not repeat shared initialization or reset
peer state. The local demo uses declared CPU/memory sizes, not manually launched
extra adapters. Also drop all adapters, list the empty adapter inventory through
controllerd HTTP, create an adapter through that API, and resume SQL against the
existing environment without reinitialization.

Exercise existing replica policy with concurrent adapters, preserving catalog
conflict checks and deployment-local readiness. Also demonstrate controller
restart without disrupting healthy children and pubsub outage/recovery with
adapters using remote pubsub. Preserve native warm handover under the split.
Extend existing boundary coverage rather than creating a separate acceptance
framework or general upgrade-version matrix.

Retain the independent-client proof: one client advances or loses protection while
another retains an older timestamp, the compaction enforcer recovers, and an
explicitly retired execution returns safely. Together, these milestones complete
the fresh-environment decoupling outcome, subject to the correctness and performance
acceptance criteria above.

## Appendix: catalog freshness and execution ordering

Strict serializable queries must observe catalog changes completed before they
start, across adapters, and execution must not overtake required replica-side
application. This includes configuration, not just object definitions.

### Catalog writes

All catalog writers, including protection and compaction publishers,
share the `EpochMilliseconds` oracle's allocation, completion and
future-timestamp discipline. Retries and rebasing must preserve it before the
write becomes durable. A commit at C is acknowledged only after oracle
completion covers C, directly or through a barrier at least C.

This coordination cost is accepted. Batching remains an implementation choice.
Replicas write independently, not through the adapter or table-write worker.

Inherited oracle/catalog progress and its necessary monotonic advancement retain
log-and-proceed behavior. A future timestamp already inherited from that state
must not block bootstrap or catalog progress. Genuinely new future jumps remain
subject to precommit protection, including when advancing an empty catalog upper.
This does not add clock synchronization or new clock-recovery guarantees.

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

## Future work: deployment-relative catalog views

Outside these milestones, public deployment-owned metadata should describe the
querying adapter's deployment, including during prewarming. Raw catalog access
and explicitly unfiltered relations should retain all deployments. Scope uses
immutable launch deployment identity, not binary version or the catalog's active
generation. Shared SQL objects remain shared.

A materializable all-deployments inventory with ordinary query-time view filters
is a candidate, following the `pg_catalog` all-databases pattern. Its filter
must not specialize a shared maintained output to one writer's deployment.
Before adopting it, settle materialized-view, index and SUBSCRIBE semantics,
including existing dependents and builtin migration. Current expression
preparation rejects context-dependent functions in maintained queries. Explicit
scope for durable queries and request-scoped binding for subscriptions are
options, not approved changes to existing SQL behavior.

## Future work: replica-targeted MV semantics

Settle whether MV pins should be restricted to shared replica declarations,
including how cluster conversions affect them, before extending managed-pinning
support. Policy-created replicas need not have counterparts across
deployments. The milestone exclusion does not change which pins can be created
within a single deployment or defer the core replica identity contract.

Resume from the [current handoff](20260903_decoupled_coordination_log.md) and
[implementer prompt](20260903_decoupled_coordination_prompt.md). The
[archived log](20260903_decoupled_coordination_log_archive.md) is historical
context, not required reading or current steering.

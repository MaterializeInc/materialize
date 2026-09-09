# Decoupled coordination

## Outcome and scope

The catalog is the authority for maintained storage and compute lifecycle.
Cluster-side components subscribe to catalog changes and enact the desired
state, rather than depending on an adapter to send lifecycle commands. Adapters
write catalog changes and use a separate fast protocol for query execution.

The goal is decoupling, preserving consistency guarantees, performance, and
existing SQL behavior except for the [maintained-creation admission rules](#admission-and-conversion).
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
conversion and rollout decision that preserves their promised results. Fresh
environments must still survive same-version restarts, replanning, and ownership
handover.

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

## Catalog-backed compaction bounds

Store explicit compaction bounds for maintained collections in the catalog.
Storage and compute must not compact those collections beyond committed
permission. Applied compaction may lag, retaining extra history. Bounds advance
monotonically within a collection's lifetime. Their representation and granularity
remain implementation choices.

Introducing or strengthening a maintained read requirement and advancing the
affected bounds must be coordinated at the catalog transaction boundary. A
transaction must not commit a requirement incompatible with already authorized
compaction, or authorize compaction that invalidates a committed requirement.
Protection must cover the interval between catalog commit and cluster application,
independently of the creating adapter's lifetime.

This places maintained read requirements and permission to discard history under
the same durable authority. Cluster-side components have an explicit limit to
enforce and recover, without treating a process's local accounting as the
authority to advance beyond it.

The cost is ongoing catalog traffic proportional to the number of changing bounds
and their publication cadence, together with catalog processing and subscriber
work. Publication competes with DDL, and further advancement depends on catalog
write availability. Delaying publication retains more history, with storage and
compute resource costs even when the metadata bandwidth is modest.

Avoid coupling ordinary query throughput to catalog writes for every hold change.
Coalescing or rate-limiting bound advancement may retain extra history, but must
not delay protection until after it is needed. Choose cadence and batching from
measured catalog load, DDL latency, and retention cost.

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

## Boundary contracts

### Readability and compaction

A reader must secure protection before relying on a timestamp. Observing a
frontier in a catalog snapshot is not itself a read hold. While protection is
valid, compute and persist compaction must respect it, including through
dependencies, installation, and ownership handover. One client cannot release
another's protection.

Keep compaction bounds distinct from a dataflow's installation `as_of` and an
MV's initial storage visibility boundary unless the implementation establishes
how they fit. The representation and accounting mechanisms for individual read
requirements remain implementation choices.

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

### Admission and conversion

Admission of maintained read requirements respects committed compaction permission
for all logical inputs, rather than relying on lagging physical compaction.
Automatically selected creation timestamps must be compatible with all those
inputs. Explicit historical refresh requests are rejected if any logical input
cannot support them, even when optimization removes that input.

Existing objects retain their promised results. Their conversion to logical-input
protection becomes active only once their remaining recovery requirements are
protected across all logical inputs. Conversion must not skip pending results by
moving the recovery timestamp forward. Conversion is deferred under the
[initial scope](#outcome-and-scope).

### Visibility and execution

Catalog commit, cluster application, and query readiness are different events.
Queries must not fail spuriously or execute against the wrong object state
because the fast protocol overtakes catalog application. Preserve existing
behavior for concurrent DDL, transactions, cancellation, and object drops.

Query-client connections must not replace one another's desired state or reset
maintained dataflows. Lifecycle ownership and permission to perform external
writes must remain safe across restarts and handover, independently of query
connection lifetime.

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

Implementers choose the smallest coherent mechanism and the order of work.
This document does not prescribe catalog fields, leases, protocol messages,
planning placement, or process topology. An incremental implementation is fine,
but an intermediate step is not completion of the outcome above.

Flag implementation discoveries, tradeoffs, and scope growth to Aljoscha. Pause
affected work when guidance is needed, especially when correctness, user-visible
behavior, or an agreed boundary would change. Do not silently narrow capability
or expand the design to resolve a difficulty. Seek independent review where
the risk warrants it.

Existing CI, nightly, and performance suites are the broad acceptance signal.
Ensure coverage actually exercises the changed boundaries, particularly
independent query clients, catalog/query ordering, and read protection during
compaction and recovery. Add targeted tests or experiments where existing
coverage leaves uncertainty. Measure catalog traffic and retained-history cost
as well as query performance. Record what ran, failures, and remaining gaps.

### Milestones

These are outcome checkpoints, not a rigid implementation sequence or one-commit
tasks. Work can cross their boundaries when necessary. The implementation log
records the active milestone and progress toward it.

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

Evidence includes actual compaction and an input eliminated by optimization.
Measure publication and retained-history costs as the real path becomes available.
This milestone can use the current single-writer arrangement.

Include fresh builtin initialization and same-version recovery, including MVs
reading system-catalog collections.

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

### Starting points

- [Catalog implications](../../../src/adapter/src/coord/catalog_implications.rs)
  derive effects from committed changes, including sink creation, index
  installation, and MV storage registration. MV and metric-sink compute
  installation remain sequencer-side.
- [Compute protocol](../../../src/compute-client/src/protocol/command.rs) mixes
  lifecycle and query commands. [Transport](../../../src/service/src/transport.rs)
  replaces the active client on a new connection.
- [StorageCollections](../../../src/storage-client/src/storage_collections.rs)
  owns storage capability accounting and critical since handles. The fixed
  critical-reader identity and epoch fencing support handover, not independent
  owners aggregating their local holds.

## Implementation log (append-only)

Append dated entries with findings, decisions or pending questions, and the next
useful step. Validation status lives in the PR, not here. Clearly distinguish an
implementer's proposal from a decision reviewed with Aljoscha. Do not rewrite
earlier entries. Keep the main text concise and update its agreed boundaries
only following review.

### 2026-09-03: Scope agreed with Aljoscha

Multi-adapter operation is the end goal, not the immediate deliverable.
Query-local dataflows remain on the fast protocol. Adapter loss must not harm
other clients or maintained dataflows, but transparent failover is not required.
Completing relevant catalog implication paths is within scope.

### 2026-09-03: Sink creation from committed implications

Sink additions now install exports through catalog implications.

Controllers still live in the adapter. Direct committed-update tests and
same-batch MV/sink creation remain gaps. Index parsing leaves plans absent and
the expression cache has no runtime read API.

### 2026-09-03: Reusable index reconstruction

Extracted bootstrap's uncached index planning and notice rendering without
changing cache policy, ordering, or installation. Next: runtime expression-cache
reads and index-add implications using reconstruction on cache misses. Cache
validity must account for committed dependencies, and installation must follow
same-batch prerequisites.

### 2026-09-03: Runtime index implications

Index additions now acquire cached or reconstructed plans and install through
implications. Runtime cache reads are best-effort and validated against committed
dependencies and compute availability. Review identified stale session notices
after cache rejection, addressed by filtering dropped dependencies.

No existing harness can exercise cluster/table/index creation in one committed
batch without the sequencer. Same-batch MV/index creation still depends on moving
MV storage creation into implications.

### 2026-09-03: System-boundary verification

Agreed with Aljoscha to proceed with system-level coverage, without a test-only
coordinator command. Added cache-disabled SQL creation and restart coverage for
index use, EXPLAIN, notices, and drop cleanup. The no-sequencer same-batch runtime
case remains uncovered until a production catalog subscriber exists. External
catalog writers fence the adapter today, and the existing read-only catalog
harness does not apply controller effects.

### 2026-09-03: MV storage registration from committed implications

MV additions register storage before dependent sinks/indexes, then initialize
read policies through the deferred batch. Runtime and bootstrap share descriptor
construction, including replacement ownership and the initial storage frontier.
Compute installation remains sequencer-side.

### 2026-09-03: Reusable MV reconstruction

Extracted bootstrap MV reconstruction without changing cache policy or timestamp
selection. Extended cache-disabled restart coverage to MV results, EXPLAIN, and
continued maintenance.

Finding: a cache miss during runtime MV reconstruction can choose imports outside
the creator's holds, and acquiring fresh holds after commit cannot recover history
needed for the committed first refresh. Input protection must be established
before moving compute installation.

### 2026-09-03: Pending first-refresh recovery coverage

Added cache-disabled restart coverage for an unexecuted first refresh after its
input changes.

Code inspection ruled out merely broadening creator holds: timestamp selection
joins every hold, changing historical MV readability, while
[`sufficient_collections`](../../../src/adapter/src/coord/indexes.rs) stops at
available indexes and does not cover sibling indexes exposed by same-batch drops.
Compute holds do protect actual transitive dependencies. Production MV installation
paused pending a decision between lifecycle-owned protection and a temporary
protected-plan reconstruction bridge.

### 2026-09-03: Lifecycle protection prioritized with Aljoscha

Proceed with lifecycle-owned protection rather than a temporary adapter bridge.

The durable writer is already shared behind a mutex, so an independent lifecycle
component need not introduce concurrent catalog writers. Existing hold accounting
and epoch fencing are reusable, but neither recovers pending maintained read
requirements from catalog state. Open question before choosing a schema: does
catalog authority require committed frontier bounds, or can advancement be
delegated to a fenced lifecycle owner under catalog-derived policies?

### 2026-09-03: Delegation failure-scenario exploration

Worked cases require atomic ordering of creation commit versus hold reclamation,
physical fencing, and a recovery barrier before compaction resumes. Snapshot absence
and catalog fencing alone are insufficient. Small abstract interleaving models
checked these orderings, not production behavior. Delegation shifts coordination
to read-requiring DDL and owner handover rather than ongoing frontier publication.
The shared writer mutex only serializes one in-process handle, not independent
writers or subscriber delivery.

### 2026-09-03: Catalog-bound traffic estimate

For compact records advancing once per minute, a planning budget of 300-500 bytes
per retraction/insertion pair gives 0.5-0.83 MB/s at 100,000 changing bounds.
This estimates uncompressed logical updates, not measured persist traffic. Current
structured JSONB encoding retains whole-record JSON text. Whole-item rewrites,
batch sizes, catalog CPU/DDL latency, persist maintenance, subscriber fanout, and
retained history need measurement.

### 2026-09-03: Explicit bounds agreed with Aljoscha

Choose catalog-backed compaction bounds and document delegated advancement as an
alternative. Schema, granularity, batching, and publication cadence remain open.
The one-minute traffic estimate is not a chosen cadence.

### 2026-09-04: Admission and recovery scope exploration

The proposed validation seam is durable `Transaction` state before
[`into_parts`](../../../src/catalog/src/durable/transaction.rs) extracts the batch.
MV creation protects physical imports but persists only the storage visibility
timestamp. Requirement identity must account for recovery and access-path changes,
not just creation admission. Protecting all logical dependencies risks retaining
unneeded history and rejecting historical creation on unused inputs. Protecting
selected imports requires a recoverable path through replanning and index drops.

### 2026-09-04: Recovery cases traced

Existing dependency holds retain dropped indexes and their storage inputs. Live
replanning lacks timestamp-aware access-path selection, while bootstrap can rebuild
indexes with different installation frontiers. Transitive storage protection handles
access-path loss but does not guarantee reconstruction from SQL: [optimizer goldens](../../../test/sqllogictest/transform/union_cancel.slt)
show whole logical inputs eliminated from nonconstant results. A reconstructed plan
can require their discarded history. The [expression cache](../../../src/catalog/src/expr_cache.rs)
explicitly has no cross-build representation compatibility contract.

Retaining a logical recovery computation over protected storage inputs would add
durable expression compatibility and dependency-version obligations. An IDs-only
alternative using empty-input substitution has unestablished error and
semantic-assumption contracts.

### 2026-09-04: Logical-input protection agreed with Aljoscha

Choose logical-input protection based on code inspection, accepting conservative
retention without making optimized expressions authoritative. The catalog already
preserves [logical dependencies for drop safety](../../../test/sqllogictest/materialized_views.slt),
but installed holds and [bootstrap recovery constraints](../../../src/compute-client/src/as_of_selection.rs)
follow physical imports. Bootstrap freezes recovered storage sinces, not missing
history. Historical creation admission and
conversion of existing objects are the next design questions.

### 2026-09-04: Admission and conversion boundaries agreed with Aljoscha

Automatic creation timestamps account for all logical inputs. Explicit historical
refresh requests incompatible with their committed compaction permission are
rejected. Existing objects keep their promised results, with conversion active only
once remaining recovery requirements are protected. The conversion mechanism and
rollout policy for objects unable to satisfy that condition remain open.

### 2026-09-08: Logical collection input discovery

Added catalog traversal and tests for logical inputs, preserving collection
versions and stopping at upstream MV outputs without selecting indexes. Review
found that resolved collection IDs also include functions, addressed by filtering
name references to relations while retaining raw-HIR reads. This is discovery only,
with no runtime protection or admission change.

Next useful step: derive maintained requirements from their query definitions
and enforce admission together with bound advancement at the durable transaction
boundary. Conversion policy remains open.

### 2026-09-08: Fresh-environment milestones agreed with Aljoscha

Focus initial implementation on fresh environments and defer existing-environment
conversion and rollout. Preserve the rejected alternatives as decision context.
Milestone 1 is active. Sink/index implications, MV storage registration, reusable
reconstruction, and logical-input discovery are implemented. Admission and
catalog-backed protection remain unwired, and independent lifecycle following
and query clients remain unimplemented.

Next useful step: connect maintained requirements, admission, and bound enforcement
toward the first protected MV creation and recovery example. Conversion is not a
blocker for this work.

Implementation finding: the [runtime index cache-miss path](../../../src/adapter/src/coord/catalog_implications.rs)
optimizes synchronously after commit, following precommit optimization. Account
for coordinator blocking and the postcommit failure boundary before extending
that pattern to MVs, without making a broad cache redesign a prerequisite.

### 2026-09-08: Storage-side compaction permission

Storage consumes explicit bounds independently of retention policies and execution
holds, including initialization, shared-shard versions, and drops. Recovery must
use protected readability rather than the policy frontier. Initialization must
not reuse a handle after a failed fencing compare refreshes its owner token.

Milestone 1 remains active. There is no durable producer for these bounds yet.
Next useful step: persist bounds and maintained requirements, enforce their
compatibility at the catalog transaction boundary, and supply committed bounds
to storage before relying on them for MV installation and recovery.

### 2026-09-08: Logical-input MV admission

MV creation accounts for logical inputs in refresh preparation and timestamp
selection, including eliminated references and planner-introduced reads. Query
references are kept separate from statement dependencies because a replacement's
`FOR` target is not an input read. Existing early holds are preserved when adding
inputs discovered during planning.

This constrains live admission using held readability, not committed permission.
Milestone 1 still needs a durable producer for bounds and maintained requirements,
transaction-boundary compatibility checks, and committed delivery to storage.

### 2026-09-08: Catalog integration prioritized with Aljoscha

Milestone 1's next target is the catalog authority connecting admission to storage:
maintained requirements and compaction permission are checked together in the
catalog transaction, and committed permission reaches storage. This can take
several commits. If blocked, identify the concrete obstacle to this integration.

The decisive check must reject an incompatible maintained requirement even when
the history remains physically readable. Creator-held readability is not durable
protection for logical inputs eliminated from the installed plan.

Account for mixed drop/create batches: [storage creation](../../../src/storage-client/src/storage_collections.rs)
requires metadata for every still-governed collection, but [catalog implications](../../../src/adapter/src/coord/catalog_implications.rs)
create collections before processing drops whose metadata is already removed.
Resolve this mismatch as part of integration, preserving valid read protection.

### 2026-09-08: Mixed-batch storage protection

Storage creation accepts committed removal of collection metadata while retaining
the installed bound until the drop is applied. Still-cataloged collections must
continue supplying bounds. This preserves create-before-drop implication ordering
and shared-shard protection without another lifecycle API.

Milestone 1 remains active. Next: persist requirements and bounds, check their
compatibility in the catalog transaction, and deliver committed bounds to storage.

### 2026-09-08: Durable protection and committed delivery

Storage-backed bounds and maintained requirements have separate durable records.
Final transaction state checks their compatibility and bound monotonicity, with
recoverable adapter admission and committed delivery to storage. Governance begins
at collection birth, not adoption of a live ungoverned collection.

Milestone 1 remains active. SQL creation does not yet produce these records. Next:
wire initial bounds and MV logical-input requirements into creation, accounting for
registration timestamps and actual readability when reusing shards. Recovery must
consume the requirements, and durable output progress must advance them and permit
bound publication before this becomes the complete protected-MV path.

### 2026-09-08: Durable client protection agreed with Aljoscha

Choose client-incarnation-scoped durable protection that survives replacement of
the components enforcing compaction, without durable query execution or session
failover. Schema, grouping, and reclamation mechanics remain implementation choices.
No separate component called a lifecycle owner is prescribed.

Milestone 1 remains active. Next: complete the protected-MV production path.
Integration cases identified in review include table commit before initialization,
read-only bootstrap racing a drop, and intermediate bound updates removed by a
same-transaction drop. Independent-client protection is part of milestone 3, not
a prerequisite for the first maintained-recovery example.

### 2026-09-08: Non-exclusive compaction application agreed with Aljoscha

Applying committed permission does not require a fenced or exclusive executor.
Separate this responsibility from authorization and read-protection accounting.
Milestone 1 remains active. Existing controller fencing cannot simply be removed
while advancement still depends on private hold accounting.

### 2026-09-08: Production integration scope question

Table initialization must respect committed birth permission independently of
later transaction-WAL registration. Secondary MV visibility must not advance the
shared shard's physical since. Source exports and sinks also inherit storage
dependencies, so zero is not a generally valid initial bound.

The protected-MV path reaches builtin initialization and the catalog's own shard.
Pending question for Aljoscha: include builtin schema evolution in milestone 1,
or leave it with milestone 2's lifecycle changes? The implementer's recommendation
is to include system-catalog inputs rather than silently use legacy protection.
Record production is in progress. Progress publication and the production
compaction/restart demonstration remain unwired.

### 2026-09-08: Builtin milestone scope agreed with Aljoscha

Include fresh builtin initialization and same-version recovery in milestone 1.
Defer builtin schema migration and protected-environment version-upgrade support
to milestone 2, separately from conversion of unprotected environments. Builtin
shard replacement reuses GlobalIds while discarding contents, so its interaction
with protected dependent history needs an explicit lifetime/recovery treatment.

### 2026-09-09: Catalog-backed MV recovery path

Milestone 1 is complete for fresh environments and same-version recovery.
Creation commits logical requirements with collection permission, including builtin
inputs. Publication follows durable output progress and respects early creation
holds. Finite persist downgrades retain pending delivery through rate limits and
primary handoff. Read-only bootstrap relies on leased readability, not stale
snapshot permission.

Publication cadence is configurable. Planning-cache freshness and DDL conflict
tracking have distinct revisions, so cadence changes do not abort open DDL.
Next: milestone 2's catalog-driven maintained lifecycle and builtin schema
migration. MV compute installation still uses the sequencer closure.

### 2026-09-09: Maintained-protection coverage agreed with Aljoscha

Milestone 1 remains active. The protected-MV path is an implementation checkpoint,
not completion across maintained object types. Source/sink recovery protection
still depends on installed controller accounting, and compute compaction has no
catalog permission cap. Current recovery ordering protects the single-owner path,
but does not establish complete catalog-local authorization.

Retain the explicit-bounds decision and its accepted catalog traffic cost rather
than reopen delegated admission and reclamation. Next: complete source/sink
requirements at the catalog authorization boundary and maintained compute permission.
Milestone 2 remains the independent lifecycle subscriber and builtin migration.

### 2026-09-09: Migration follow-on scope agreed with Aljoscha

Builtin schema migration and protected-environment version upgrades join
existing-environment conversion as follow-on work after demonstrating
fresh-environment decoupling, not milestone 2 requirements. Fresh builtin
initialization, same-version recovery, and ownership handover remain in scope.
Milestone 1 remains active. Next: complete maintained protection, then move
lifecycle execution to the independent catalog subscriber in milestone 2.

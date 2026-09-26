# Decoupled coordination: implementation log

Historical decisions, findings, and handoffs. The
[design](20260903_decoupled_coordination.md) owns the agreed contracts, and the
[implementer prompt](20260903_decoupled_coordination_prompt.md) owns current steering
and the append-only handoff instructions.

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

### 2026-09-09: Publication batching and remaining scope questions

Publication stages changed requirements and bounds as one catalog operation,
avoiding per-record scans of accumulated transaction updates. End-to-end
publication, DDL latency, and retention measurement remain necessary.

Pending guidance from Aljoscha: using committed permission for a fresh sink's
automatic cutoff can omit changes that lagging physical compaction still retains,
including with `SNAPSHOT = false`. The implementer's proposal is to apply the
agreed creation-admission rule to that cutoff while preserving pending output
through alteration and recovery. A second proposal is to cover catalog metric
sinks in milestone 1 and address the ownership of curated, transient-ID,
per-replica metric dataflows with milestone 2's subscriber.

Milestone 1 remains active. Next: resolve these boundaries and complete source/sink
authorization and maintained compute permission.

### 2026-09-09: Maintained-protection integration checkpoint

The metric-sink discussion clarified that protection belongs to their inputs and
execution holds. Their process-local output needs no compaction bound or historical
recovery requirement. The curated definition list is empty.

The local implementation connects source/sink requirements to their definitions and
durable progress, including a committed remap floor carried into ingestion setup.
Catalog comparison uses frozen read-only reconstruction rather than changing
publication settings. Index bound ownership and controller accounting are present,
but index publication, committed delivery, and pre-install authorization are unwired.

Pending decision: may fenced index reconstruction establish readability later than
the saved bound once all valid read requirements are protected? Same-replica
reconciliation can compact or replace an existing trace, so it is not universally
a fresh installation. No permission bypass is implemented.

Milestone 1 remains active. Next: resolve that boundary, connect the index path,
and complete production recovery and publication-cost evidence. An adjacent issue
remains separate: `alter_export` retains the old descriptor after an input change,
which a subsequent connection alteration can consult.

### 2026-09-09: Recovery authority and sink admission agreed with Aljoscha

Preserve committed compaction permission without making saved index bounds a
historical reconstruction guarantee. Recovery must preserve all valid read
requirements and authorize destructive reconciliation. Preserve read-only
prewarming by bringing forward the necessary catalog-following work, rather than
introducing a startup-specific writer protocol or a reconstruction exemption.

Fresh sinks select their automatic cutoff against committed input permission,
including with `SNAPSHOT = false`. Existing sinks retain pending output through
alteration and recovery. Their requirements advance with durable output progress,
not merely with input compaction permission.

Milestone 1 remains active. Next: connect maintained compute permission and
recovery authorization, then establish production recovery and publication-cost
evidence across maintained object types.

### 2026-09-09: Maintained compute permission and recovery evidence

Index bounds publish with storage progress and reach compute before installation.
Writable recovery commits the selected installation frontier. Read-only prewarming
follows committed bounds through an independent durable catalog reader beside its
SQL savepoint, and waits for permission rather than installing beyond it. A dropped
sibling index no longer blocks delivery to surviving lifetimes.

Findings: physical history release for sources is bounded by crash-surviving
persist reader leases and a fixed ingestion resumption hold, not only by catalog
permission. Consistency checking needed an exact committed prefix because
publication removes any quiet catalog window. Catalog history growth at a
one-second cadence made `mz_catalog_raw` observers slow even at 100 collections.

Open question for Aljoscha: a same-version replan can select an index `as_of`
beyond the writer's bound when a suspended cluster's warmup hold caps publication.
Read-only bootstrap then waits before installing any dataflow. This is a liveness
tradeoff of strict permission, not a safety gap, and needs a decision on whether
prewarming should proceed per cluster or keep waiting.

Milestone 1 evidence: protected recovery demo and read-only index proof pass on
published binaries. Representative publication measurement is in progress.

### 2026-09-09: Design consolidation and steering agreed with Aljoscha

Keep the architectural choices. Selected implementation decisions belong in their
own design section, historical handoffs in this sibling log, and current steering
in the prompt.

Milestone 1 remains active. A blocked cluster should not prevent unrelated,
otherwise-ready work from prewarming. The affected cluster's eventual progress
and promotion-readiness behavior remain open. Next: resolve those questions and
concurrent-drop behavior, finish publication evidence including real subscriber
costs, and advance the production maintained-lifecycle subscriber.

### 2026-09-09: Permission as an installation constraint agreed with Aljoscha

Committed permission is a hard upper constraint on installation `as_of`, applied
in as-of selection. Installation never waits for the writer to advance permission.
This supersedes the partial-prewarming direction in the previous entry: no cluster
blocks, so per-cluster isolation and the affected cluster's promotion readiness
need no separate treatment.

Review of the branch against the design found the protection contract implemented
across maintained types, with machinery beyond the design in three places: index
bound birth records threaded through eleven transaction mutation paths with
whole-catalog scans in validation, test-only surface on the durable catalog trait
and coordinator command enum for the consistency checker, and publication work
proportional to catalog size. The design now states that a record must carry
non-derivable information and that publication scales with what changed.

Milestone 1 remains active. Next: remove the wait loop and index birth records,
trim the checker surface, make publication proportional to changed records, then
record measurements and squash the 09-09 history before milestone 2.

### 2026-09-09: Index installation feasibility and checker boundary

Removing index birth records leaves first-installation authority unresolved. If
absence means MIN, an index over an already-compacted input cannot install. A
saved index bound can also precede actual readability when replanning introduces
an input outside the installed plan's protection. A hard upper constraint cannot
make those frontiers compatible. Index cleanup is paused for a decision on initial
permission and the outcome of infeasible read-only reconstruction.

The consistency checker needs independent reconstruction at an aligned immutable
catalog version under ongoing publication. Moving that comparison into the existing
diagnostic check is a proposal, not an agreed decision. It removes the external
prefix certificate but adds server-side reconstruction cost. Next: settle these
two boundaries, then resume index simplification and changed-record publication.

### 2026-09-09: Index permission and checker boundary agreed with Aljoscha

An index's bound is its published since. A fresh index has no bound until first
publication, so first installation has no cap. When a saved bound is below actual
readability, nothing durable depends on the gap, because a durable requirement on
an index protects its inputs. The index is replaced at readability and its bound
follows through publication. Installation never waits for a catalog write. This
resolves both halves of the previous entry's question without a pre-install commit.

The consistency checker's durable-to-memory reconstruction moves behind the
existing environmentd diagnostic check, off the coordinator thread, using an
ordinary read-only open synced to the snapshot's upper. The dedicated command,
header, and frozen catalog mode are removed once no test needs them.

Milestone 1 remains active. Next: resume index simplification under these rules,
then the checker move and changed-record publication.

### 2026-09-09: Measurement accounting and index-fixture scope

Persist's shard diff-byte counter measures consensus state metadata, not catalog
row payload. Separate committed packed-row counters classify protection and other
catalog traffic without adding durable records or another catalog mode.

A large identical-index population on one view produces quadratic notice work,
retained notices, and dependency checking. Cache-miss implication reconstruction
also repeats optimization on the coordinator. Proposed scope split, awaiting
Aljoscha: use independent constant-view inputs for publication measurements, retain
the duplicate-index stress case, and address the broader planning/notice cost with
lifecycle work. Do not suppress slow-message diagnostics to make that case pass.

Milestone 1 remains active. Next: resolve that scope split and finish payload-aware
CI measurements.

### 2026-09-09: Bounded performance scope agreed with Aljoscha

Milestone 1 measures 100 and 1,000 generated objects. Larger-scale performance
work is deferred. Keep the shared-view index topology and diagnostics, rather
than adopting the independent-view proposal. The workflow still accepts explicit
larger cohorts. Next: finish payload-aware comparisons at the agreed sizes.

### 2026-09-09: Maintained-lifecycle handoff

The next ownership boundary is MV compute installation from committed state and a
production maintained-lifecycle subscriber. Creator-local plans, locally installed
collection assumptions, and sequencer-supplied MV requirements remain transitional.
Keep the bounded performance scope, baseline catalog cloning costs, and deferred
identical-index scaling work explicit rather than folding a general redesign into
the ownership transition.

### 2026-09-10: Lifecycle extraction cost question

Controller servicing, protection publication, table registration, and managed-cluster
reconciliation still depend on the coordinator. Moving installation into implications
alone does not establish adapter-process independence. A full catalog follower must
also preserve transaction boundaries rather than pass several transactions' parsed
updates into implications as one unconsolidated batch.

Pending guidance from Aljoscha: the proposed extraction keeps the controller bundle
in an independently running lifecycle service, with single-adapter controller access
initially. This brings the required cross-process access boundary forward, but not
milestone 3's independent clients or durable client protection. The service extraction
is paused on its implementation cost. Writer-owned MV admission and catalog-owned
storage metadata preparation are bounded prerequisites that can proceed independently.

The writer derives complete MV requirements from query references and committed input
permission. A sequencer-selected frontier is optional, with birth promises checked
against final transaction state. Query references are reconstructed from SQL and kept
separate from replacement-target dependencies. Storage preparation uses final catalog
membership, including all collection versions, rather than installed collections.
Shared live aliases prevent shard retirement, while orphan mappings do not.

### 2026-09-10: Lifecycle placement and query client agreed with Aljoscha

The controller bundle moves into one independent process whose interface is
catalog following, enactment, and publication. It does not serve controller state
to adapters or gate their catalog writes, so it can later dissolve into per-cluster
followers. The adapter gets no remote controller API. It reads through a query
client that owns its read requirements, learns storage frontiers from persist and
compute frontiers from the fast protocol, and carries durable client protection
from the start. That protection is pulled forward from milestone 3 so no bridge is
built twice. Table appends and DDL stay with the adapter.

Adapter DDL and lifecycle publication become two cooperating catalog writers.
This is required concurrency once publication leaves the adapter, distinct from
arbitrary numbers of adapters, which remain out of scope. The cluster side must
accept a lifecycle connection and query connections at once.

Milestone 2 is re-cut as independent maintained lifecycle with one adapter and
one query client. Milestone 3 becomes multiplicity and isolation of query clients.
Next: build the query client in-process, then the connection split, cooperating
writers, and the process move, in that order.

### 2026-09-10: Stable MV read bindings and client-protection proposal

MV collection versions now participate in SQL name resolution independently of
table schema evolution. Replacement changes the catalog item ID while preserving
the referenced collection version, so downstream recovery requirements and SQL
reconstruction retain the same input identity. Session-catalog GlobalId lookups
also preserve the requested MV version. The internal state-level lookup still
defaults MV aliases to Latest, an adjacent inconsistency left outside this fix.

Pending approval before building protection: a never-reused client incarnation
with a heartbeat sequence, plus per-incarnation collection requirements with
frontiers and input lifetime bindings. The proposed reclaimer observes an unchanged
heartbeat for five minutes on its monotonic clock, then compares and closes that
incarnation atomically with removing its requirements. Renewal is proposed every
minute. A restarted observer waits a full observation window. Closed incarnations
cannot be revived, and closure must fence late query execution before compaction
passes the reclaimed protection. This is a proposal, not an agreed decision.

### 2026-09-10: Client protection records and reclamation agreed with Aljoscha

Two records: a client incarnation with a heartbeat, and a requirement keyed by
incarnation and collection with a protected frontier. Clients aggregate locally,
commit before use, and batch advances. Heartbeat every minute, bumped in the same
transaction as requirement publication so there is one client write path. A
reclaimer that observes an unchanged heartbeat for five minutes on its monotonic
clock removes the incarnation and its requirements with a compare on the
heartbeat. A restarted reclaimer waits a full window. Closure is permanent.

Rejected from the proposal: copying shard identities or input bindings into client
records, and a compute-side incarnation fence. Storage metadata stays the one
owner of the id-to-shard mapping and remains unfinalized while any requirement
references the collection. An index's inputs are derived at validation time.
Readability is enforced where the read happens, so a reclaimed client's late read
is served correctly or refused, and compute learns nothing about incarnations.

Milestone 2 remains active. Next: build the query client in-process on this basis.

### 2026-09-10: Query-client integration and connection review

The uncommitted implementation adds the two client records, catalog-derived index
input requirements, retained storage metadata, local grant aggregation, and
heartbeat reclamation. The query client observes Persist frontiers and actual
compute read frontiers. Request timestamp selection and fast peeks are being
integrated with that client. Query-local creation and replicated sink-response
merging are implemented but not yet connected to SQL slow SELECT, SUBSCRIBE, or
COPY TO execution.

Independent connection review found five concrete issues: missing transitive
producer retention, query servicing blocked by incomplete lifecycle initialization,
unresolved pre-admission cancellation, starvation between query connections, and
unbounded response-routing history. Fixes and regressions are in the worktree.
Routing uses ordered connection-open and retirement events, not catalog incarnation
knowledge. Scheduling guards follow maintained import chains as well as direct
query imports.

The adapter and compute test targets compile. The normal compute test command was
killed twice during code generation, with kernel OOM evidence. A compute-only
test profile override is being tried without changing assertions. These checks
do not establish SQL integration or milestone acceptance. Query dataflow time
dependence, timeline hold issuers, and instrumentation still need integration
attention before removing the controller bundle from the adapter.

Proposed to Aljoscha, awaiting approval: cooperating handles join the active
deployment fence, with promotion still fencing the old deployment. Persist
compare-and-append remains the commit authority. Writers refresh and revalidate
on contention, retry metadata-only conflicts, and reject invalidated planned DDL
rather than merge stale definitions. Each writer follows committed state directly.
Read-only SQL savepoints require a live client-protection writer. No cooperating
writer implementation has been started on this proposal.

The reduced-optimization run exposed invalid arrangement fixtures, then a real
maintained-alias retention bug. Corrected fixture lowering and producer-token
retention yielded 13 query tests and 16 peek-sweep tests passing. Review found
the same retention condition in iterative alias exports, which is now corrected
too. The combined rerun passes formatting, whitespace, adapter test-target
compilation, all 13 query tests, and all 16 peek-sweep tests.

Query-client review also found a replica-publication race in the terminal
unreadable decision and creation sent before imported indexes were observed.
Selection now compares desired and ready counts from the same selection pass,
and query creation waits for readable imports while retaining creation holds.
Their wire-level regression coverage remains outstanding. Baseline comparison
rejects target-local upper selection as a required fix: existing timestamp
selection uses cluster-wide uppers, then honors the target during execution.
Waiting on a slower target preserves that behavior. Replica since/readability
rejection remains a separate concern requiring a concrete baseline comparison,
not a change to freshness semantics merely to satisfy a target-local test.

### 2026-09-10: Cooperating writers approved, committed-only critical since

Aljoscha relayed approval of generation-scoped writers, CAS with metadata retry
and DDL revalidation or planning conflict, independent projections, and no DDL
forwarding. Critical since handles follow committed bounds only, without local
capability authority or an envd process epoch. Prewarming protection joins the
active generation, not its pending deployment generation. Concrete enactment
conflicts between same-generation lifecycle instances must be raised separately.
The designer's decision commit is preserved as parent `75c83e93`. Include the
uncommitted log updates with the implementation commits.

Implementation is underway at the durable join, adapter contention/projection,
and storage critical-since boundaries. Joining must not run bootstrap migrations
or remove live ephemeral objects. A definite CAS loss is recoverable, but a fence
observed after a successful append still requires recovery rather than replay.

The committed-only storage contract rejects writable collections without bounds.
Fresh catalog protection defaults on, and unprotected catalogs receive an explicit
unsupported-mode error rather than conversion or a local-capability fallback.
The forced builtin-MV migration tests deliberately use unprotected mode and are
incompatible with this scope. Their assertions remain intact. SQL query-client
integration, prewarming writer wiring, and system verification remain incomplete.

### 2026-09-10: Resumed after disk recovery

Generation-scoped catalog admission and metadata primitives pass the open and
read-write suites. Committed-only storage advancement passes the collection suite,
including real leased readability after critical advancement and finalization
after lease release. The unused critical-since epoch argument is removed through
controller and environmentd construction.

SQL SUBSCRIBE and COPY now own cancellable query-creation tasks in their active
sink state. Slow SELECT and legacy-sequenced fast peeks route through QueryClient.
Creation waits for readable imports, and transient peeks use only connections
that acknowledged their own dataflow. Coordinator execution bookkeeping is shared
across the query paths. This wiring still needs production system verification.

Prewarming now acquires a joined writer under the active durable generation and
reconstructs a separate committed projection. Its entry point permits only client
creation and publication, leaving the SQL savepoint unchanged. A synchronization
fence closes cached client grants. The adapter projection test also distinguishes
planning-visible peer DDL from client metadata and checks generation promotion.

Independent review identified a manual-debug mutation conflict: debug edits and
deletes relied on epoch fencing, which same-generation admission does not provide.
Raised to Aljoscha for an explicit offline or promotion policy, with offline-only
recommended. The fencing test remains unchanged. Promotion cleanup and crashed
ephemeral-owner recovery also retain explicit limitations, not a general epoch.

Remaining integration includes runtime timeline hold issuers, maintained DDL
readability assumptions, query time dependence and instrumentation, then the
independent lifecycle process and production demonstration. The worktree and log
updates are uncommitted, with the designer's parent commit preserved.

### 2026-09-10: Protected-mode scope and administrative writes clarified

Aljoscha relayed the designer's correction: committed-bound-only critical since
and cooperative writer admission apply to protected environments. Unprotected
environments retain capability-driven compaction, epoch-fenced normal opens, and
their migration behavior. The unsupported-mode error and changed default are
reverted. Prewarming acquires a protection writer only in protected mode. The
builtin-MV migration tests remain unchanged, not skipped or weakened.

Administrative edits use cooperative CAS without exclusive admission or promotion
in either mode. Registered client heartbeats and recent catalog publication provide
an advisory liveness check. Edits and deletes refuse with a reason unless forced.
The check is repeated against each CAS snapshot after contention. Heartbeats are
counters, so unreclaimed clients are conservatively treated as live. Publication
uses a five-minute recency window, conservative under timestamp compaction.
`--force` bypasses only this check, not CAS or deployment fencing.

Writers that cannot decode, apply, or enact committed foreign changes halt for
durable-state recovery rather than continue with a stale or partial projection.
This applies to all foreign writes, not only administration. Tests cover ordinary
legacy epoch fencing separately from non-fencing administrative mutations and
protected-generation cooperation.

Review found valid foreign changes that bypassed projection errors. Runtime identity
validation now reports recovery for post-bootstrap changes to the protection latch,
transaction WAL identity, or bootstrap-only settings. It preserves generation/epoch
fence precedence and permits initialization and effective no-ops. Joined serving
projections mark bootstrap completion after reconstruction. Global system-parameter
effects now run from committed implications, including the timeline timer, instead
of a separate input-Op path. Subprocess coverage distinguishes process termination
from an ordinary test panic for invalid SQL and a protection-mode change.

### 2026-09-10: Query-owned timeline windows

Protected timeline windows use client grants after installation, deferring indexes
without observed readable replicas. Bootstrap uses the same acquisition path,
without requiring initial index-bound publication. Pending acquisition defers
during another client publication and rechecks collection membership after it.
Unprotected timelines keep controller holds. Next: exercise this in draft PR CI,
then finish maintained installation and remaining query-side controller dependencies
before moving the lifecycle process.

Logging sources have no storage access path. Planning must include their
catalog-owned indexes before query-wire observations arrive, while execution
still waits for actual trace readiness. Unpublished protected logging indexes
retain their MIN initialization permission. Historical-read admission and retained
storage projection agreement remain separate follow-ups.

### 2026-09-10: Milestone 2 pause handoff

Temporary SQL visibility is not durable lifetime membership. Track committed Item
global IDs and versions even when their owning session is absent locally, so a
joined projection cannot retire a live foreign temporary table on client release.

Next: fix historical-read acquisition. A cached client grant at G covers reads from
G onward but does not make older retained history unavailable. Use query timestamp
constraints and current permission to expand coverage when needed, preserving local
reuse and ordinary nonblocking reads rather than republishing MIN per query.
Restarted index permission, final shard retirement, and targeted SUBSCRIBE behavior
remain separate investigations. Maintained installation and the lifecycle process
move are still outstanding. Preserve the designer commits and keep this checkpoint
as WIP while continuing the draft PR loop.

### 2026-09-12: Table-time ownership question

Empty adapter group commits advance transaction-WAL time for registered tables.
Moving only the controller bundle leaves that progress dependent on adapter life,
so maintained source/table joins can stall after adapter loss. Proposed to Aljoscha:
lifecycle advances WAL time without row writes or membership changes, while the
adapter retains DDL, registration/forgetting, and row appends. This is not yet an
agreed decision. Pause that extraction boundary pending guidance and continue the
in-process query and maintained-installation work.

Unpublished indexes can have durable client grants. Recovery must not choose a
later soft timestamp preference just because no index bound has been published.
The design's least-readable reconstruction rule also covers grants admitted after
the recovering process's snapshot, without adding a grant subscriber or startup
catalog write.

A pending MV replacement's creation frontier is independent of the target's shared
output progress. Runtime installation respects that distinction. Restarting with
the target still suspended remains a separate investigation: bootstrap still
applies the shared output's recovery constraint to the pending replacement.

### 2026-09-12: Adapter-driven table time agreed with Aljoscha

Aljoscha relayed the designer's decision: transaction-WAL ticking stays with
adapters. Assume a live adapter for table-time progress. The single-adapter
milestone 2 demonstration explicitly accepts paused table-fed dataflows and shows
source-fed dataflows and compaction continuing. This resolves the extraction
question without adding another WAL writer. Use “lifecycle components” or “the
process running the controller bundle,” not “lifecycle process” or “lifecycle
service.” Next: resume the remaining in-process prerequisites and bundle extraction.

### 2026-09-12: Adapter-owned table writer and replacement recovery

The adapter owns the existing WAL worker and its append/register/forget FIFO.
Registration uses committed descriptions and shard metadata, not controller
inventory. Read-only backfill remains limited to migrated system tables and does
not tick the WAL. The bundle retains transaction reading and collection lifetime
management.

Pending replacement recovery distinguishes durable creation protection from
unprotected input holds that can advance with the target. Sealed-target pruning
retains its existing semantics. Compaction publication cannot release abandoned
client grants, independently of its cadence. Next: finish the remaining query-side
controller dependencies and extract the controller bundle.

Read accounting can retire before execution cleanup. Table/source cleanup now
validates the controller's own inventory, and table cleanup follows adapter
forgetting without a deferred channel. Retained aliases whose live primary belongs
to a later bootstrap batch wait for that batch, with shared-shard permission still
enforced across all aliases.

Remaining request ownership includes webhook batching, idle progress and statistics,
and storage-side oneshot COPY execution. Metadata-driven Persist reads need no
controller state. Storage query connections must coexist with maintained execution,
not replace its connection or replay completed COPY requests.

Remote COPY staging uses fresh, request-owned storage query connections, with row
commitment still in the adapter. Query admission, retirement, and all-worker
completion use the storage sequencer. Completion must retain tokens until every
worker finishes, or local release can stall a sibling. COPY completion carries its
ingestion UUID so a late canceled result cannot consume a newer request's context.
Connection lifetime bounds replay/cancellation state for the per-COPY client.

Asked Aljoscha whether webhook idle progress must continue without the sole adapter,
or may pause alongside adapter-driven table time. Webhooks have a separate idle
driver coupled to HTTP batching and statistics. The proposal is to preserve idle
progress in lifecycle components and separate adapter-owned HTTP execution, but
that boundary and statistics ownership are not yet settled.

### 2026-09-12: adapter-owned webhook ticking

Aljoscha chose the same live-adapter assumption for webhook ticking as table time.
Keep HTTP batching and idle advancement together with the adapter. The adapter-loss
demonstration can let webhook-fed work pause and must show autonomous source-fed
maintenance and compaction continuing.

Adapter-owned storage execution includes statement histories and webhook statistics.
The existing raw statistics relation is partitioned by replica ID, with NULL rows
owned by the adapter. Even an empty restored inventory must reconcile persisted
rows. Catalog shard preparation is independent of controller inventory, while
physical finalization acknowledgments remain with the finalizer.

Maintained creation admits logical input history, not candidate index readiness.
Query progress observes Persist and compute directly. Compute write observations
must include the installed as-of lower bound, and fresh notification subscriptions
must not replay old changes. Remaining extraction includes selected-plan/notice
publication, startup protection ordering, and separating maintained enactment from
the request-serving event loop. Naming does not require an extra wrapper.

### 2026-09-14: Selected-plan introspection and request handles

Protected index/MV plan explanations read selected immutable plans with a fixed
catalog snapshot. Runtime notice publication follows committed own-build selections,
independently of installation. Protected request plumbing no longer carries legacy
storage handles, and MV creation and rewrites share replica eligibility.

Pending Aljoscha's decision: DROP INDEX currently names still-running physical
dependents, which selected plans cannot describe after a rewrite. The proposal is
to preserve that meaning through best-effort compute query-protocol observations,
not controller-state RPC. That path remains unchanged.

Next: selected-plan-only installation with independent waiting, bootstrap ordering,
and the lifecycle ownership split. Same-generation orchestration, lifecycle
connection admission, and introspection reconciliation need narrow enactment fencing.

### 2026-09-14: Fixed-plan protection gap

Logical maintained requirements do not protect physical indexes in a selected
plan. An uninstalled or rewritten selection can lose required index history after
its writer's client protection is reclaimed, even while logical storage remains
readable. Removing installation fallback is paused on this gap.

Proposed to Aljoscha: derive object-owned protection for selected physical imports
from immutable plans alongside logical requirements, accepting retention for
selected plans that are not running. No new durable record is proposed. Admission,
selection changes, and compaction authorization must enforce this atomically.

Also proposed, not yet agreed: publish changed client aggregates in coalesced
maintenance batches while retaining one-minute idle heartbeat renewal and the
five-minute reclamation grace. Sharing heartbeat cadence retains obsolete live
client grants for up to a minute. Earlier aggregate publication increases catalog
traffic and needs a deliberate retention/traffic choice.

### 2026-09-14: Designer clarification relayed by Aljoscha

No durable physical-import protection. Logical inputs remain the only maintained
requirement inputs. Publishers apply every catalog change through their transaction
base before sampling bounds and apply/resample after CAS failure. Recovery installs
before publication or reclamation, with writer client protection through commit.
The rewrite scenario above does not establish a full-recovery failure: index
reconstruction can recover the selected access path from protected logical inputs.
Next: verify that reconstruction and application/publication ordering.

DROP INDEX names only the objects whose plans the writer rewrote. Client aggregate
advancement uses the existing coalesced publication cadence with heartbeat bumps
in that transaction, while heartbeat-only renewal is idle maintenance. Inspect and
remove unnecessary birth-time protection for new sources.

### 2026-09-14: Pending-installation liveness question

The in-progress runtime installer queues unavailable selections/imports and revisits
physical dependencies. Dropping an uninstalled export must consume its pending
marker before physical-drop filtering, and reclamation must recheck pending work
after CAS contention applies peer DDL.

Asked Aljoscha to clarify unrelated maintenance: the full-application rule pauses
all bound publication and reclamation while any installation is pending. Controller
servicing, sources, sinks, and unrelated catalog effects continue. No scoped
publication exception is proposed or implemented without that clarification.

### 2026-09-14: Literal publication barrier agreed

Aljoscha relayed the designer's decision: any pending installation defers all bound
publication and client reclamation. Execution and other catalog effects continue.
Within one build, write-before-select and atomic import rewrites make pending work
transient. Failures retry with backoff and must be logged, counted, and surfaced.
Do not build scoped exceptions. Actual stalls may motivate a separate decision to
scope the rule using committed catalog dependents.

### 2026-09-14: Adapter-loss demonstration drives process extraction

Aljoscha relayed the designer's steering: define the mzcompose adapter-loss workflow
now and move lifecycle ownership into its own process before exhausting the
controller-read tail. Diagnostics without query-client observations may be unknown.
The demo must observe source-fed MV execution, sinks, and compaction while the
adapter is stopped, explicitly allow table/webhook-fed work to pause, and verify
queries resume after restart. Design and prompt bodies stay untouched.

The cluster `adapter-loss` workflow expects separate `--coordination-role=adapter`
and `--coordination-role=lifecycle` services. Those entrypoints are not wired yet.
`Catalog::open_committed` supplies non-initializing reconstruction for the joined
lifecycle process. Next: branch before listener binding and component construction,
keep adapter writers absent from lifecycle and controllers absent from adapter,
then split bootstrap and loop inputs by ownership. Query-backed introspection and
curated metrics must keep an explicit planning/writing owner, not be silently skipped.

### 2026-09-14: Replica placement supersedes process extraction, consolidated handoff

Aljoscha and the designer decided that compute and storage clusterd replicas follow
and enact their cluster's catalog. There will be no separate lifecycle process or
lifecycle connection. The preceding extraction plan is superseded. Keep the query
client, cooperating writers, written plans, client protection, connection split,
and adapter-owned table/webhook writers. Replica orchestration stays in envd.
Design and prompt bodies are designer-owned. This log replaces the handover file.

Implementation checkpoint retained from the handover:
- Immutable expression entries in `src/catalog/src/expr_cache.rs` are keyed by
  build, export GlobalId and UUID. Catalog `WrittenPlan` selections use the catalog
  CAS. Import validation uses final candidate state, not an operation prefix.
- Writer create stages select plans atomically. `prepare_written_plan_rewrites`
  uses post-DDL state, index replan ordering and protection through commit. It
  handles replacement versions and writer-owned notice changes. CREATE INDEX
  does not rewrite existing plans. Build ownership remains the caller's duty.
- Protected runtime installation consumes selected bytes with dependency retries
  and the global publication/reclamation barrier. Mixed bootstrap still plans.
  Missing bytes can still error in writer notice refresh and bootstrap.
- Metadata-only retries must apply foreign changes and resample proposals after
  changed execution constraints. Request diagnostics use real query-client
  observations or unknown. `Catalog::open_committed` reconstructs without bootstrap
  DDL or planning. WAL writer startup is separate from controller construction.
- Relevant boundary fixtures are catalog `written-plans`, adapter MV/selection
  tests, `restart/selected-plan-explain`, and environmentd
  `test_peer_index_pending_installation`. Remaining scenarios include same-batch
  dependencies and aliases, replacement reconstruction, failed CAS and delayed
  permission application. Preserve pending historical outputs and feature behavior.

Next is a compute replica follower alongside the controller, then replacement of
controller installation by replica reconciliation. Replica execution reads need
incarnation-scoped client protection before removing cross-replica hold accounting.
Direct query routing/merging follows, then storage enactment on the same follower
path. Bring shard finalization and unsafe concurrent sink external writes to
Aljoscha before implementing their rule. Do not retain duplicate storage enactment.

The `cluster/adapter-loss` workflow now uses surviving clusterds, including a
two-replica compute cluster, rather than separate envd roles. It requires Kafka
execution and direct Persist history compaction during absence. Controlled slow
hydration, same-batch dependencies and delayed/concurrent permission scenarios
remain to be added. Table/webhook work may pause. Compute alone is not milestone
completion. Cross-build repair, upgrades, prewarming-owned selections and retired
build cleanup remain deferred.

### 2026-09-14: Storage ownership decisions from Aljoscha and the designer

Finalization applies committed permission after collection metadata retirement,
which excludes surviving alias and requirement references. Duplicate finalization
attempts are harmless. For M2 the adapter drains it, deferred during adapter loss.
There is no replica-side finalization drain.

Kafka sinks have one active replica, selected as the lowest live incarnation among
the cluster's replicas. Stable per-sink transactional IDs provide handover and ALTER
fencing when a new producer opens. DROP stops when the follower observes it, with
the existing command-delivery lag allowance. No catalog sink lease. Remove the
ordered catalog-epoch lifecycle admission, not generation-scoped cooperative
admission or incarnation-scoped client protection.

### 2026-09-15: Shared catalog-following path

Aljoscha relayed the designer’s direction to replace snapshot-derived follower
state with a committed read-only Catalog, its update stream, and shared
parse-and-absorb catalog implications. Delete the parallel derivation. Replica
enactment must acquire incarnation-scoped import protection before choosing an
as_of and installing through its worker path, then apply bounds before proposing
them. The controller remains the installer until that path can replace it.

### 2026-09-20: Convergence steering and remaining storage questions

Aljoscha approved documentation cleanup and steering toward the shared-catalog
integration, replica enactment, and adapter-loss proof, including reconstruction
of a replica during adapter absence. Mechanical moves and behavior changes may
land separately. Writer-owned planning and replica-side enactment remain agreed
boundaries.

Query-client findings: completed transient-export observations survive until
disconnect and are cloned during planning. Acquisition retries can reuse a grant
made incompatible by peer publication. Resolve at those boundaries without adding
another protection mechanism.

Before storage cutover, settle the takeover latency implied by incarnation
reclamation and Iceberg's retry of overlapping same-version batches. These remain
open questions, not approval of a faster closure rule or a sink lease. Kafka
producer retries must re-evaluate current eligibility and committed definitions.

### 2026-09-20: Shared native catalog path

Native loading, update application and state-owned transactions now live in
mz-catalog. Adapter keeps serving/session behavior and DDL admission. Clusterd
uses the same committed loader and implications, with initialization shard
identities captured from that prefix rather than repeated snapshot lookups.

Next is replica-owned worker sequencing and aggregated progress, then storage
enactment. Cold recovery must establish dependent holds before activating index
read policies. Pending incremental installations need local import holds across
retries. The adapter-loss target now restarts a compute replica after compaction,
stops its surviving sibling, and requires a fresh source-fed result without SQL
ingress. Sink takeover latency and Iceberg overlap remain pre-cutover decisions.

### 2026-09-20: Native execution protection boundary

Native ingress is independent of query connections and aggregates global-worker
progress. Incremental selection takes protected live-index frontiers alongside
the pending DAG. The remaining integration is incarnation-scoped protection,
local dependency holds, and bounds publication before replacing the controller.

Execution-input progress and index retention are distinct needs. Dropping the
controller's input holds would change historical reads through retaining indexes.
Proposed, awaiting Aljoscha: preserve the live index's retention window with
incarnation-scoped logical-input protection, without promising durable index
reconstruction at that window. Sink cutover questions remain open.

### 2026-09-20: Object-owned retention agreed with Aljoscha

Retention policies remain authoritative with no adapters, query clients, or
replicas, including after incarnation reclamation. This supersedes the live-only
index-retention proposal. Replica grants cover additional execution needs, not
the object's policy. Recovery cannot skip history the policy still requires.
Retention advances with the relevant upper rather than pinning creation history.

Next: integrate this constraint before replica cutover and verify it without
incarnation grants, through shutdown and reconstruction. Derive from catalog
definitions and durable progress where possible. The progress basis for an absent
index must be established before choosing any additional durable state.

### 2026-09-21: Zero-replica retention advancement clarified with Aljoscha

Zero-replica indexes retain policy-driven advancement from input progress, using
the existing paused-cluster behavior as the baseline. Losing replicas neither
releases the policy nor freezes its frontier. Verify advancement without query
or replica grants. Upper semantics for live, lagging indexes and the retention
contract for replica-local compute logs remain separate open questions.

### 2026-09-21: Index retention upper and compute-log scope agreed with Aljoscha

The object-owned index requirement follows durable logical-input progress with or
without replicas. Replica grants additionally protect running indexes' retention
windows relative to execution progress, preserving admission of historical reads
even when inputs run ahead. Extra retention for lagging replicas is accepted.

Compute logs and indexes over them keep replica-local history semantics. This work
does not persist those logs or recover their history after replica loss. Persisted
system-catalog collections remain under the normal guarantees. These decisions
resolve both pending retention questions. Next: integrate and verify both retention
constraints before replica cutover, without assuming additional durable state.

### 2026-09-21: Derived retention at the native catalog boundary

Object retention needs no additional index record. The native commit path can
derive it from final definitions, durable logical-input uppers, and the transaction
base's readable floor. Proposals cannot supply their own floor. First index-bound
publication records installed readability rather than advancing it.

Progress sampling has bounded fanout. Failure retains committed permission by
aborting publication for retry. Log-dependent indexes retain local semantics,
including mixed views, without exempting persisted inputs' own policies. Next is
replica grant integration for execution and live retention windows, plus recovery
and zero-replica advancement through the runtime path.

### 2026-09-21: Execution protection outlives export retirement

Queries and maintained importers can keep a producer running after its catalog
export is dropped. Their read timestamps need not protect the producer's pending
input snapshot. Replica execution grants must follow actual input completion,
not catalog absence or retirement of the readable export. Passive input probes
can continue reporting without extending execution lifetime. Legacy reconnection
must silence retired IDs at the nonce boundary, before initialization is received.
Next: bind execution grants to these signals and add execution-relative live windows.

### 2026-09-21: Replica enactment integration boundaries

Local index imports must remain pinned across installation retries, before applying
new committed bounds. Ordered creation transfers execution protection to trace
readers. First index permission must respect every committed window, not join a
proposing replica's later installation frontier over another replica's requirement.

Questions brought to Aljoscha: whether five-minute Kafka takeover is acceptable,
whether curated non-catalog metric observers may be adapter-owned diagnostic
queries, and whether replica incarnations should carry their catalog replica ID.
The identity tag is a proposal: current heartbeat/grant records cannot distinguish
eligible sink replicas from readers. Iceberg's overlapping same-version retry still
needs a commit-boundary decision. These do not block committed-bound application.
Next: complete adapter compute cutover, then storage enactment on the same follower.

Slow catalog I/O can outlive local renewal evidence. Installation now requires
recent evidence measured from the start of a successful publication, not its
acknowledgement. Staleness pauses new installation without killing pinned existing
execution or preventing renewal. Observed reclamation still terminates the follower.
Current own-build DROP transactions repair pending consumers' selections, so late
old plan bytes are not authority to install against a dropped index.

### 2026-09-14: Cutover decisions approved by Aljoscha

Fresh-environment M2 accepts roughly five-minute abandoned-incarnation Kafka
takeover, including same-generation replica restarts. This is provisional, not a
production failover target. Read-protection grace stays unchanged, without another
sink lease or handoff protocol. Replica incarnations carry an immutable catalog
replica ID, ordinary query incarnations remain untagged, and membership is derived
from the catalog. The tag alone grants neither ownership nor fencing.

Curated metric sinks must keep emitting during adapter loss. Planning can remain
writer-owned, execution is replica-owned, and existing definitions and written
plans should be reused. Bring disproportionate cost rather than adding a parallel
protocol or weakening autonomous observability.

Iceberg must check version and progress preconditions at every actual commit and
internal rebase against atomically protected metadata, with explicit initial-
snapshot semantics. Overlap stops prepared-file publication and reconstructs from
committed progress through sink recovery. Uncertain commits require a progress
reload before republishing and must not trigger deletion of possibly committed
files. Completion without duplicates, not just overlap rejection, is the outcome.
Next: integrate these decisions and complete the adapter-loss/reconstruction proof.

Storage output uppers and bookkeeping DROP acknowledgements do not complete
execution reads. Native attempts retain protection across async startup and actual
reader teardown, including remap reads and upsert rehydration. Source enactment
shares the compute follower's incarnation and publisher. Desired installation waits
for all-worker acknowledgement, not first input data. Canceled attempts keep their
read protection through teardown.

Two proposals remain with Aljoscha: a Kafka pre-open admission roundtrip over the
native endpoint after potentially long initialization I/O, and failing closed when
all Materialize progress snapshots have expired behind an Iceberg compaction
snapshot. No additional lease is proposed. Curated metrics still need replica-owned
written selections. Their transient execution scopes are isolated from queries,
but compute logging must also distinguish exports with colliding scoped IDs.

### 2026-09-14: Curated replica ownership

Curated admission uses replica/name ownership on existing per-build written
selections, without SQL catalog items or another plan store. The writer prepares
new replicas' selections in their creating transaction. Native execution preserves
the distinction between live flag changes and bootstrap reconciliation.

Maintained and query transient IDs have separate execution scopes. Logging must
key export instances by dataflow identity as well as export ID. Collector labels
select a stable worker, so an ordered drop and replacement cannot race registry
ownership across workers. Next: finish verification, native sink admission, and the
coupled production cutover. The Kafka pre-open and expired-progress questions above
remain unresolved.

### 2026-09-14: Sink admission and missing progress decisions

Aljoscha approved attempt-scoped Kafka pre-open admission through the existing
replica endpoint after slow initialization. It rechecks current eligibility,
definition and protection, without replacing Kafka fencing or reusing approvals
across retries and replacements. Read grace and provisional takeover delay stay
unchanged.

Iceberg must fail closed when prior table history remains without Materialize
progress. Genuine initialization stays valid. This is a bounded correctness fix,
not metadata repair. Next: complete native sink enactment and genuine status and
statistics observations, then coupled cutover and the adapter-loss demonstration.
Curated progress must remain nonterminal and advance across successive samples.

Native storage retains passive inventory and introspection writers in the adapter,
not installation holds, command history or lifecycle transports. Query observers
subscribe after aggregate readiness and restore current status. Counter aggregation
continues without observers, but reported deltas are not reset until one subscribes.
This does not promise complete outage history across replica loss. A health stall
does not undo source hydration; a new attempt or observer snapshot re-establishes it.

Bounds honor the configured publication interval independently of protection
renewal and reclamation. An abandoned query incarnation can pin input compaction
through the same five-minute grace as any other client. Read-only provisioning
omits native follower arguments and retains legacy prewarming. The legacy sink-drop
path's stale shard-mapping rows remain outside the native inventory cleanup change.

### 2026-09-14: Planning and metadata-contention boundaries

EXPLAIN's declared index candidates are separate from the observed collections
used for timestamp and read protection. A broader proposal to keep transaction
access paths pinned to held indexes would also change the documented error after
CREATE INDEX during a transaction. That behavior change awaits Aljoscha's decision.

Empty DropObjects operations need no plan repair. Prepared DDL can survive
metadata-only contention without repeating plan loading, provided its planning
revision and client incarnation remain valid and commit validation runs against
the refreshed prefix. Structural changes retain the planning-conflict policy.
Next: verify this boundary against the unchanged bounded workload and finish the
native outage demonstration. Native acceptance precedes upstream integration in
a separate review boundary unless an upstream change blocks that acceptance.

Native index frontier diagnostics use actual observations from current query
connections, independently of permission publication. Unknown observations are
omitted, not reported as completed frontiers. Disconnect and DROP retract them.
This reporting owns no installation or read capabilities. Persisted collections'
global frontier reporting remains storage-owned.

Unused unmaterialized views have no plan owners and cannot invalidate written
imports or notices. Drops with dependents still take the full repair path. This
eligibility distinction does not add a plan cache or broaden notice optimization.

Runtime-immutable WAL identity is read from a fenced durable snapshot without
requiring the SQL projection to consume unrelated replica updates. Those updates
remain queued for normal application. Temporary comments follow their items'
local SQL visibility, independently of durable membership and reclamation. The
same-generation restart cleanup expectation remains a separate open question.

### 2026-09-14: Bootstrap and acceptance boundaries

Protected same-generation restart preserves potentially live foreign temporary
owners. SQL namespace isolation is separate from durable inventory. Graceful close
cleans up its owner, and promotion reclaims crashed-owner items, comments and
storage mappings with finalization. Crashed-owner resources may remain until
promotion. Unprotected cleanup is unchanged, without a session-liveness framework.

Bootstrap must tolerate autonomous metadata publication throughout opening, not
just at the final WAL lookup. Identity creation precedes runtime freezing. Pure
catalog replay does not commit, while genuine initialization writes keep
refresh, validation and fencing. Absorbed builtin rows belong in the initial reset
vector until that reset completes, not in the live introspection writer.

Retained catalog history needs attribution to actual Persist readers before cost
acceptance. Keep reader maps from the existing boundary inspections. A controlled
nonempty prepared-rewrite regression needs visibility into revision reuse and live
holds. Approval for a small test observation hook is pending. Broader transaction
access-path behavior remains deferred.

### 2026-09-14: Scoped proof and existing retention costs

The prepared-rewrite regression may use an environment-scoped Rust observation
hook, inactive unless explicitly configured. It reports plain IDs, revisions and
active hold minima without acquiring, cloning or extending protection or changing
retry decisions. No SQL surface, durable state or general observer framework.

The outage fixture may supply bounded ordinary input to give Persist compaction
work independently of per-record inspection overhead. Keep the physical assertion,
cutoff, grace, timeout and exact outputs. A universal scheduling theorem is not a
landing requirement. Ordinary lease-refresh lag is an accepted existing M2 cost,
not a new latency guarantee. Report observed retention costs and the unproven
steady-state plateau without adding a longer campaign as an acceptance gate.

### 2026-09-14: Sibling development binaries and historical-index verification

Within the single-build scope, replica reconstruction configuration carries the
writer's explicit written-plan namespace. The writer keeps its distinct
development-build namespace and expression-cache isolation. Replica selections,
plan bytes and ownership checks use that supplied namespace, with matching
semantic versions, including prerelease, required before loading plans. The
provisioner must pair compatible sibling binaries. Neither version equality nor
successful decoding proves development-build compatibility. Reconstruction does
not need an adapter connection, arbitrary selection fallback, cross-build repair
or a new fingerprinting framework. Release-build behavior is unchanged.

After two unmasked zero-replica history advances, the outage fixture may extend
only the historical index's retention before starting its first replica. The
original historical timestamp and reference read remain protected through
hydration and comparison. This separates rolling retention from cold-start
verification without changing physical cutoffs, grace, deadlines or exact
outputs. The final historical query must execute through the reconstructed index,
not merely explain a declared index candidate. Broader transaction behavior
remains outside this change.

### 2026-09-14: Isolated retention input and frozen M2 acceptance scope

The zero-replica retention input and index are established before the adapter
outage on a shard without readers from the compute replicas deliberately killed
by the recovery checks. The shared-input outage and recovery checks remain intact.
This isolates retention advancement from valid abandoned Persist reader leases,
without changing production leases, reclamation, scheduling or proof deadlines.

The CI135583 failure reproduced with its prebuilt images. At the failed deadline,
a killed compute replica's input reader still had a valid 15-minute lease. The
reader subsequently expired normally and Persist since advanced without
intervention. This is fixture interference, not evidence of a production defect.

M2 acceptance scope is frozen. The targeted DDL regressions and bounded throughput
checks have sufficient passing evidence. Finish the agreed native outcomes, fix
concrete CI regressions and continue the agreed upstream integration. Preserve
behavioral contracts while simplifying incidental fixture arrangements. Do not
add scenarios, stronger guarantees, observability or a lease-expiry campaign.
Escalate correctness problems, user-visible changes or disproportionate cost.

The complete native workflow passes locally with CI135583 binaries and this
fixture correction. It proves physical compaction past cutoff `1790241677000`,
adapter-absent compute recovery with fresh value 1220, source/sink recovery with
1230, and two unmasked retention advances. Historical timestamp `1790242378001`
is read through the reconstructed index with equal reference rows, one additional
pgwire fast-path execution and no Persist-fast-path execution. All three resumed
MV/sink outputs match exactly: 132 values from 0 through 1310 in steps of 10.
The EXPLAIN matcher accepts both `ReadIndex` and fast-path `Indexed` rendering.
Syntax, Black, Ruff and whitespace checks pass. Host memory pressure delayed
startup, but the workflow's proof deadlines and assertions were unchanged.
This closes the native runtime proof, not the remaining CI regressions.

### 2026-09-14: Replacement tests follow asynchronous application semantics

Replacement application is not linearizable at the dataflow level. Queries may
return old data after ALTER returns, even under strict serializable isolation,
as documented in `test/testdrive/replacement-materialized-views.td` and the test
migration in a67e138b92 (#35091). The observed old value followed by convergence
does not demonstrate a production correctness defect. The expression-cache tests
use bounded eventual-output checks while retaining dependency-drop, cache-disabled
application and restart coverage. No enacted-catalog observation or synchronous
APPLY completion contract is added. Failure to converge or recover remains a bug.

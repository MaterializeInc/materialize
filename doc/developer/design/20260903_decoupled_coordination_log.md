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

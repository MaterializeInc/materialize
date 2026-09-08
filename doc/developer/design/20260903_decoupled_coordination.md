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
enforce and recover, without treating a live owner's local accounting as the
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
how they fit. The representation and accounting of individual read requirements
remain open.

Propagation to persist critical since handles must respect all valid read
requirements. Those handles are the durable backstop, not a substitute for
multi-client accounting. Stale owners must not advance compaction or destroy
data based on incomplete local knowledge. Abandoned client holds must be
reclaimable without allowing that client to resume using invalid protection.

Recovery must establish actual readability and restore valid read requirements
before further advancement is authorized. Reconstructed plans must use inputs
readable at the protected timestamps, rather than assume equivalent access paths
have equivalent history.

### Admission and conversion

Admission of maintained read requirements respects committed compaction permission
for all logical inputs, rather than relying on lagging physical compaction.
Automatically selected creation timestamps must be compatible with all those
inputs. Explicit historical refresh requests are rejected if any logical input
cannot support them, even when optimization removes that input.

Existing objects retain their promised results. Their conversion to logical-input
protection becomes active only once their remaining recovery requirements are
protected across all logical inputs. Conversion must not skip pending results by
moving the recovery timestamp forward. The conversion mechanism and rollout policy
for objects that cannot yet satisfy this condition remain open.

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

The catalog could define maintained requirements and retention policies while a
fenced lifecycle owner accounts for client and maintained reads and advances
compaction directly. Persist critical handles would provide the durable storage
backstop, without publishing advancing bounds to the catalog. This avoids ongoing
frontier-publication traffic and its dependence on catalog write availability.

Delegation still requires coordination when durable read requirements are
introduced or strengthened. Their admission must be tied to owner-held protection.
Reclaiming abandoned precommit protection must exclude a late commit that relies
on it. Observing an object's absence in a catalog snapshot is not sufficient.
Recovery of valid holds and enforcement against stale owners remain necessary
under either approach.

We choose explicit bounds for the catalog-local permission boundary. Delegation
reduces ongoing catalog traffic but shifts coordination into maintained-DDL
admission and cleanup. We accept the publication and retention costs of explicit
bounds rather than this owner-backed admission and reclamation protocol.

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

### Starting points

- [Catalog implications](../../../src/adapter/src/coord/catalog_implications.rs)
  derive effects from committed changes. Index and MV creation also have
  sequencer-side installation paths to account for.
- [Compute protocol](../../../src/compute-client/src/protocol/command.rs) mixes
  lifecycle and query commands. [Transport](../../../src/service/src/transport.rs)
  replaces the active client on a new connection.
- [StorageCollections](../../../src/storage-client/src/storage_collections.rs)
  owns storage capability accounting and critical since handles. The fixed
  critical-reader identity and epoch fencing support handover, not independent
  owners aggregating their local holds.

## Implementation log (append-only)

Append dated entries with findings and evidence, decisions or pending questions,
validation results, and the next useful step. Clearly distinguish an
implementer's proposal from a decision reviewed with Aljoscha. Do not rewrite
earlier entries. Keep the main text concise and update its agreed boundaries
only following review.

### 2026-09-03: Scope agreed with Aljoscha

Multi-adapter operation is the end goal, not the immediate deliverable.
Query-local dataflows remain on the fast protocol. Adapter loss must not harm
other clients or maintained dataflows, but transparent failover is not required.
Completing relevant catalog implication paths is within scope. Implementation
has not started and runtime validation has not been performed.

### 2026-09-03: Sink creation from committed implications

Sink additions now install exports through catalog implications. Independent
review found no blocking issue. [CI build 133862](https://buildkite.com/materialize/test/builds/133862)
passed both Clippy jobs and formatting before a documentation push superseded it.
Latest CI and runtime validation remain pending. Local builds were blocked by
missing artifacts and tools. Aljoscha confirmed CI as the default test loop.

Controllers still live in the adapter. Direct committed-update tests and
same-batch MV/sink creation remain gaps. Next proposed slice: reuse index plan
reconstruction from bootstrap, preserving precommit optimization as a cache.
Index parsing leaves plans absent and the expression cache has no runtime read
API. No new boundary decision was made.

### 2026-09-03: Reusable index reconstruction

Extracted bootstrap's uncached index planning and notice rendering without
changing cache policy, ordering, or installation. Independent review found no
issue. Local adapter `cargo check` and Rust formatting passed. Full formatting
and lint failed on missing tools and a Python-doctest dependency build. Runtime
validation and this commit's [PR CI](https://github.com/MaterializeInc/materialize/pull/38696/checks)
remain pending.

Next proposed slice: runtime expression-cache reads and index-add implications
using reconstruction on cache misses. Cache validity must account for committed
dependencies, and installation must follow same-batch prerequisites. Add direct
committed-update coverage without sequencer plans. No boundary decision changed.

### 2026-09-03: Runtime index implications, verification in progress

Index additions now acquire cached or reconstructed plans and install through
implications. Runtime cache reads are best-effort and validated against committed
dependencies and compute availability. Review identified stale session notices
after cache rejection, addressed by filtering dropped dependencies.

Adapter and cache-test compilation passed. Runtime tests remain pending. The
extraction's Clippy fix passed both [CI Clippy jobs](https://buildkite.com/materialize/test/builds/133870).
Full validation of this follow-up is pending. Proposed test-only adapter catalog
transaction request awaits Aljoscha's approval, because no existing harness can
exercise cluster/table/index creation in one committed batch. Same-batch MV/index
creation still depends on moving MV storage creation into implications.

### 2026-09-03: System-boundary verification

Agreed with Aljoscha to proceed with system-level coverage, without a test-only
coordinator command. Added cache-disabled SQL creation and restart coverage for
index use, EXPLAIN, notices, and drop cleanup. Python formatting and Ruff passed,
runtime validation remains in [PR CI](https://github.com/MaterializeInc/materialize/pull/38696/checks).
The no-sequencer same-batch runtime case remains uncovered until a production
catalog subscriber exists. External catalog writers fence the adapter today,
and the existing read-only catalog harness does not apply controller effects.

### 2026-09-03: MV storage registration from committed implications

MV additions register storage before dependent sinks/indexes, then initialize
read policies through the deferred batch. Runtime and bootstrap share descriptor
construction, including replacement ownership and the initial storage frontier.
Compute installation remains sequencer-side. No boundary decision changed.

Local adapter compilation and Rust/Python formatting passed. Full formatting and
lint are blocked by missing tools and a Python-doctest OpenSSL dependency build.
Independent review found no issue. This slice's [PR CI](https://github.com/MaterializeInc/materialize/pull/38696/checks)
is pending. Prior regular [CI build 133879](https://buildkite.com/materialize/test/builds/133879)
passed. Next useful step: catalog-driven MV compute planning and installation,
preserving refresh timestamp selection and input protection. Same-batch runtime
coverage still awaits a production subscriber.

### 2026-09-03: Reusable MV reconstruction

Extracted bootstrap MV reconstruction without changing cache policy or timestamp
selection. Extended cache-disabled restart coverage to MV results, EXPLAIN, and
continued maintenance. Independent review found no issue. Adapter compilation
and Rust/Python formatting passed.
Full formatting and lint remain blocked by missing tools and an OpenSSL dependency
build. Prior [CI build 133882](https://buildkite.com/materialize/test/builds/133882)
passed. This slice's [PR CI](https://github.com/MaterializeInc/materialize/pull/38696/checks)
and runtime coverage are pending.

Next useful step: establish input protection for runtime MV reconstruction before
moving compute installation. A cache miss can choose imports outside the creator's
holds, and acquiring fresh holds after commit cannot recover history needed for
the committed first refresh. No protection mechanism or boundary change was agreed.

### 2026-09-03: Pending first-refresh recovery coverage

Added cache-disabled restart coverage for an unexecuted first refresh after its
input changes. Python formatting and Ruff passed. Full formatting/lint remain
blocked by missing tools and an OpenSSL dependency build. Prior
[CI build 133887](https://buildkite.com/materialize/test/builds/133887) passed.
This slice's [PR CI](https://github.com/MaterializeInc/materialize/pull/38696/checks)
and runtime verification are pending.

Code inspection ruled out merely broadening creator holds: timestamp selection
joins every hold, changing historical MV readability, while
[`sufficient_collections`](../../../src/adapter/src/coord/indexes.rs) stops at
available indexes and does not cover sibling indexes exposed by same-batch drops.
Compute holds do protect actual transitive dependencies. Production MV installation
remains paused. Next proposed step: agree whether to establish lifecycle-owned
protection now or build a temporary protected-plan reconstruction bridge. No new
mechanism or boundary change was agreed.

### 2026-09-03: Lifecycle protection prioritized with Aljoscha

Proceed with lifecycle-owned protection rather than a temporary adapter bridge.
Both restart jobs in [CI build 133889](https://buildkite.com/materialize/test/builds/133889)
passed, including the pending-first-refresh regression. Overall CI remains pending.

The durable writer is already shared behind a mutex, so an independent lifecycle
component need not introduce concurrent catalog writers. Existing hold accounting
and epoch fencing are reusable, but neither recovers pending maintained read
requirements from catalog state. Before choosing a schema, clarify whether catalog
authority requires committed frontier bounds or can delegate advancement to a
fenced lifecycle owner under catalog-derived policies. The latter is a proposal,
not an agreed interpretation of the boundary. No production changes made.

### 2026-09-03: Delegation failure-scenario exploration

[CI build 133889](https://buildkite.com/materialize/test/builds/133889) passed.
Worked cases require atomic ordering of creation commit versus hold reclamation,
physical fencing, and a recovery barrier before compaction resumes. Snapshot absence
and catalog fencing alone are insufficient. Small abstract interleaving models
checked these orderings, not production behavior. The delegation proposal shifts
coordination to read-requiring DDL and owner handover rather than ongoing frontier
publication. Admission/revocation and recovery of valid client holds remain open.
The shared writer mutex only serializes one in-process handle, not independent
writers or subscriber delivery. No boundary decision or production change made.

### 2026-09-03: Catalog-bound traffic estimate

Aljoscha leans toward explicit bounds. For compact records advancing once per
minute, a planning budget of 300–500 bytes per retraction/insertion pair gives
0.5–0.83 MB/s at 100,000 changing bounds. This estimates uncompressed logical
updates, not measured persist traffic. Current structured JSONB encoding retains
whole-record JSON text. Whole-item rewrites, batch sizes, catalog CPU/DDL latency,
persist maintenance, subscriber fanout, and retained history need measurement.
No schema or publication cadence was agreed.

### 2026-09-03: Explicit bounds agreed with Aljoscha

Choose catalog-backed compaction bounds and document delegated advancement as an
alternative. Schema, granularity, batching, and publication cadence remain open.
The one-minute traffic estimate is not a chosen cadence. Next useful step: define
the catalog transaction boundary for maintained read requirements and bound
advancement. This is a design decision, with no production implementation change.
Documentation checks passed. Full formatting and lint remain blocked by missing
tools and the Python-doctest OpenSSL build.

### 2026-09-04: Admission and recovery scope exploration

The proposed validation seam is durable `Transaction` state before
[`into_parts`](../../../src/catalog/src/durable/transaction.rs) extracts the batch.
MV creation protects physical imports but persists only the storage visibility
timestamp. Requirement identity must account for recovery and access-path changes,
not just creation admission. Protecting all logical dependencies risks retaining
unneeded history and rejecting historical creation on unused inputs. Protecting
selected imports requires a recoverable path through replanning and index drops.
This choice remains open, with schema work paused for discussion with Aljoscha.

No production changes or local tests. Existing [CI build 133937](https://buildkite.com/materialize/test/builds/133937)
remains pending, with no reported failures at inspection.
Next useful step: agree the recovery protection scope before adding durable records.

### 2026-09-04: Recovery cases traced

Existing dependency holds retain dropped indexes and their storage inputs. Live
replanning lacks timestamp-aware access-path selection, while bootstrap can rebuild
indexes with different installation frontiers. Transitive storage protection handles
access-path loss but does not guarantee reconstruction from SQL: [optimizer goldens](../../../test/sqllogictest/transform/union_cancel.slt)
show whole logical inputs eliminated from nonconstant results. A reconstructed plan
can require their discarded history. The [expression cache](../../../src/catalog/src/expr_cache.rs)
explicitly has no cross-build representation compatibility contract.

Pending proposal: retain a logical recovery computation over protected storage inputs,
without pinning physical indexes. This adds durable expression compatibility and
dependency-version obligations, not just frontier metadata. Independent scrutiny of
an IDs-only alternative using empty-input substitution found unestablished error and
semantic-assumption contracts. No approach was agreed. No production changes or runtime
experiments. Next useful step: review the recovery representation cost with Aljoscha.

### 2026-09-04: Logical-input protection comparison

Aljoscha leans toward protecting all logical inputs. The catalog already preserves
[logical dependencies for drop safety](../../../test/sqllogictest/materialized_views.slt),
but installed holds and [bootstrap recovery constraints](../../../src/compute-client/src/as_of_selection.rs)
follow physical imports. Bootstrap freezes recovered storage sinces, not missing
history. Conservative logical storage-input protection would strengthen that contract
while retaining SQL-based reconstruction, avoiding authoritative optimized expressions.
Extra retention, historical creation admission, and conversion of existing objects
need consideration. This remains a proposal. No production changes or runtime tests.

### 2026-09-04: Logical-input protection agreed with Aljoscha

Choose logical-input protection based on code inspection, accepting conservative
retention without making optimized expressions authoritative. Independent review
found no issue. Historical creation admission and conversion of existing objects
are the next design questions, not approved behavior changes.

Document checks passed. Full formatting and lint remain blocked by missing tools
and the Python-doctest OpenSSL dependency build. No production changes, runtime
recovery experiments, or retention measurements were made.

### 2026-09-04: Admission and conversion boundaries agreed with Aljoscha

Automatic creation timestamps account for all logical inputs. Explicit historical
refresh requests incompatible with their committed compaction permission are
rejected. Existing objects keep their promised results, with conversion active only
once remaining recovery requirements are protected. The conversion mechanism and
rollout policy for objects unable to satisfy that condition remain open.

Document checks passed. Full formatting and lint remain blocked by missing tools
and the OpenSSL dependency build. No production changes were made.

### 2026-09-08: Logical collection input discovery

Added catalog traversal and boundary tests for logical inputs, preserving collection
versions and stopping at upstream MV outputs without selecting indexes. Review found
that resolved collection IDs also include functions, addressed by filtering name
references to relations while retaining raw-HIR reads. This is discovery only, with
no runtime protection or admission change.

Adapter library/test compilation and Rust formatting passed. Full formatting and
lint remain blocked by missing tools and the OpenSSL dependency build. Runtime
validation is pending in [PR CI](https://github.com/MaterializeInc/materialize/pull/38696/checks).
Next useful step: derive maintained requirements from their query definitions and
enforce admission together with bound advancement at the durable transaction boundary.
Conversion policy remains open. No design boundary changed.

### 2026-09-08: Test fixture Clippy fix

Both Clippy jobs in [CI build 133949](https://buildkite.com/materialize/test/builds/133949)
rejected seven `unwrap()` calls in the new fixtures. Replaced them with descriptive
`expect()` messages without changing assertions or production behavior. Rust formatting
passed. Updated CI validation remains pending.

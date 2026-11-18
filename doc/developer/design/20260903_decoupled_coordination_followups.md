# Decoupled coordination: architectural follow-ups

This is an audit backlog, not an expansion of Milestone 3. Implementation of
the opportunities below needs separate scope agreement. Findings are based on
source inspection unless runtime evidence is cited.
Measurement reports named in backticks, such as `backend_load`, are kept outside
the repository in `~/.local/share/aj/m3-evidence/reports/`.

The direction is incremental observation and publication by the component that
owns a fact. Reuse existing identities, catalog changes and Persist listeners.
Do not introduce a designated maintenance adapter, another consensus protocol,
or a general sampling framework.

## Startup activation ordering

Source review found a pre-existing join race. `initialize_query_client` refreshes
the catalog before activating the prepared client. A replica created by a peer
can enter through that refresh and start query-dependent introspection too early,
terminating the joining adapter. The previous controller fallback failed too.

A joining adapter must tolerate that catch-up and install each intended
introspection subscription once query execution is ready. Verify a peer-created
replica during the activation handoff. This has not been reproduced or repaired.
The call chain is recorded in
`~/.local/share/aj/m3-evidence/native-only/native-compute-owner-review.md`.

## Serving readiness before local replica provisioning

Two full native DDL runs reached a healthy candidate SQL listener before its
deployment-local catalog replicas existed. Testdrive's initial catalog query
failed with `CLUSTER "mz_catalog_server" has no replicas available`. No candidate
clusterd launch appears before that failure in the captured logs.

The DDL workflow now waits for `ReadyToPromote` before exercising the candidate.
That establishes its test starting condition, not general serving-readiness
acceptance. Decide whether listener readiness should also promise usable builtin
catalog queries before changing the production health contract. This observation
does not establish the separate activation race above.

Evidence: `~/.local/share/aj/m3-evidence/native-only/aj-native-ddl-074f5610/`
and `aj-native-ddl-7422ebf9/`, including exact rerun logs and container inspections.

## Renaming temporary objects referenced by views

The temporary-object lifecycle run rejects `ALTER TABLE temp_t RENAME TO ...`
when a temporary view references the table. The unchanged
`QueryIdentAgg::visit_unresolved_item_name` requires three name components for an
item reference, but temporary references use two (`mz_temp.temp_t`). It reports
an ambiguous rename before committing any change.

This is not repaired by execution ownership. Its lifecycle coverage renames a
leaf temporary view instead, retaining nested dependency cleanup, table-version
and privilege-update checks. A rename-rewriter repair needs separate scope.
Evidence: `~/.local/share/aj/m3-evidence/native-only/native-temporary-runtime-alter-enabled.log`.

## Persist output authority after a non-active deployment restarts

A running former active replica keeps its granted Persist output permission
after promotion. A restarted replica in that still-admitted deployment starts
without it because `ReplicaEnactment::has_output_write_authority` checks the
current active generation. SQL, DDL and table-write admission remains intact.

Clarify whether restart should recover prior output enablement or intentionally
pause those maintained outputs. The design permits overlapping Persist writers
but distinguishes serving admission from maintained output authority. Changing
this behavior needs a policy decision, not a test expectation workaround.
See `src/clusterd/src/catalog_follower/execution.rs` and
`src/clusterd/src/catalog_follower/execution/storage.rs`.

## Kafka sink metric ownership

Each worker creates `KafkaSinkMetrics` with only a sink-ID label. Non-producing
workers then return and drop their handles. Independently created
`DeleteOnDropMetric` handles do not share cleanup ownership, so one worker can
unregister a producing worker's series. Absence of that series is not evidence
that the producer stopped. Current raw status removal also precedes execution
teardown, so it cannot fill this observation gap alone.

Allocating metrics only on the selected producing worker is a proposed narrow
repair. Approval is pending. The distinct-version sink-handover test instead uses
worker-scoped sink statistics, whose owners follow the producing operator's
lifetime. It must not claim withdrawal from rdkafka metric disappearance. See
`native-compatible/verification.md` under the local evidence root and
`src/storage/src/sink/kafka.rs`, `src/ore/src/metrics/delete_on_drop.rs`.

## Retained source-statistics replay

The native scraper retains baseline's change-only publication. Deployment-qualified
rows can legitimately enlarge snapshots and retained update history. A completed
debug readiness diagnostic ended with one dependency chain: historical source
statistics, current statistics, hydration status, and replica display. Their
matching lagging frontiers do not identify excess publication or an expensive
operator. Input progress and operator scheduling measurements are still needed.

Source review also found possible same-deployment reconciliation overlap and no
statistics partition cleanup in the inspected retirement sweeper. Neither is a
measured cause of this run's delay. Check actual publishers and retired rows
before expanding ownership or cleanup scope. Preserve historical visibility and
catalog-authorized retirement. Do not change retention, cadence or readiness to
hide replay cost.

Evidence: `source-statistics-replay-survey.md` and
`readiness-dependency-findings.md` under
`~/.local/share/aj/m3-evidence/native-only/native-ci-recovery/`.

## Singleton CDC-reader ownership across deployments

The full native DDL fixture proves that two admitted deployments repeatedly steal
the same PostgreSQL replication slot and collide on the same MySQL server ID.
Native singleton selection is deployment-local, while the upstream identifiers
come from the shared source definition. Persist output-write permission does not
prevent opening these connections. Legacy read-only prewarming skipped these
connectors. This is persistent contention before process restarts, not merely
OOM recovery. It does not establish data loss.

The approved boundary is a catalog-selected desired reader, enacted by replica
followers and handed over at promotion alongside sink authority. SQL/table writers
remain independent. Candidate compute prewarms from shared Persist source data,
without requiring independent candidate upstream-reader progress. Temporary overlap
is acceptable. Existing takeover/recovery must converge so superseded readers stop
reconnecting. No stop acknowledgement, lease or new coordination protocol is needed.
Verify continued ingestion and eventual cessation of contention, not uninterrupted
connections. The ownership repair is implemented. Selected native PostgreSQL,
MySQL and SQL Server handover runs passed, as recorded in
`~/.local/share/aj/m3-evidence/native-only/native-cdc-runtime-verification.md`.
The unchanged full DDL workload also passed with the current compatible-prototype
26.46/26.47 binaries. See `native-driver-scratch/native-history-compatible-ddl-evidence.tar.gz`
under the same evidence root. This is not arbitrary-release compatibility or
rebuilt-image CI. Broader workloads remain separate checks.

Evidence: `~/.local/share/aj/m3-evidence/native-only/native-cdc-reader-ownership.md`
and the matched actor logs in `aj-native-ddl-6f303bf0/`.

## Backend writes and blob consumption

The backend attribution (`backend_load`)
reconciles CAS/blob calls and bytes against actual events. The
shared frontier repair (`shared_frontiers`)
removed duplicate observation-source reads and reduced total GETs by 28.7%.
The catalog journal and progress fanout remain. A separate retained dependency
MV lacks a common source predicate. Do not redirect its history through the
unretained shared catalog arrangement.

At 10s publication, most CAS excess was empty completeness fanout into 50 durable
advancing catalog outputs. The 60s comparison (`publication_interval`)
reduces that contribution. Frontier observations now lead the remaining CAS gap.
Slower protection publication reduces one source of input progress. General
output coalescing still needs an explicit latency budget.
At the collection-manager boundary, peer progress can inform an idle completeness
deadline without choosing an owner. Preserve the existing maximum lag and all
pending command obligations. Source statistics has two partition timers, not
unconditional empty scraping.

Additional blob bytes are dominated by catalog publication and cross-process
compaction. Most observation producers already batch near their report cadence.
No saving from further batching is established. A cached shared-since shortcut
would need a Persist API that is absent at this revision, so it remains deferred.
The report separates measured attribution from prospective savings.

The fresh logical-component comparison (`logical_load`)
confirms that output completeness, rather than raw catalog CAS count, dominates
extra conditional writes. Source-linked CPU evidence also identifies repeated
immutable written-plan decoding for producer schemas. Cache those facts by
selection/revision while retaining version checks and pending misses. Filtering
irrelevant protection kinds before the shared SQL arrangement is a separate
candidate. It cannot by itself remove empty completeness writes or journal bytes.

The 60s comparison also found legacy frontier progress retained by dynamic SQL
branches. Native-only cleanup removed those alternatives together with their SQL
dependencies. The current comparison below measures remaining overhead, not an
isolated benefit from that removal. Remaining
native observation relays are not alternate execution owners and cannot simply
be stopped while their SQL inputs remain.

### Native-only resource comparison

A fresh debug pair compares native `922ca8ed` with upstream `d9a2c5dc`, at 60s
protection publication. Both passed two five-minute idle windows after five
minutes of warmup, SQL validation, post-window profiles and cleanup. Common
settings and aggregate limits match. No aggregate CPU throttling or memory-limit
events occurred during the windows.

| Metric | Upstream | Native |
|---|---:|---:|
| Materialize CPU, cores | 0.568 | 0.921 |
| PSS, MiB | 1,237 | 2,788 |
| Consensus CAS/s | 78.87 | 97.92 |
| Consensus scans/s | 2.50 | 6.81 |
| Blob GET KiB/s | 16.19 | 86.66 |
| Blob SET KiB/s | 93.62 | 151.26 |

The catalog replica accounts for roughly three quarters of the CPU gap. Native
non-SQL processes retain catalog reconstruction allocations, but sampled heap
paths do not account for every byte of PSS. Scan callers were not traced in this
pair. The older caller distribution must not be presented as current attribution.

This upstream has 47 durable catalog outputs, versus native's 51. The four extra
outputs are adapters, adapter statuses, adapter endpoints and deployment
authority. They account for 5.01 of the 9.87 extra output CAS/s. Shared outputs
add 3.60 committed empty appends/s and 1.27 other CAS/s. No changed-data appends
to these outputs occurred in the measured windows. More frequent completeness
records do not establish faster logical-time progress.

Frontier observations add a net 5.75 CAS/s over upstream's ready-made relations.
No native legacy-frontier writer appears in the ledger. Journal traffic and
frontier observations dominate the blob-byte gap. Backend events reconcile with
the available counters. This is one fresh pair against a newer upstream, not a
causal before/after estimate of the cleanup's benefit.

Detailed attribution, profiles, provenance and limits are in
`~/.local/share/aj/m3-evidence/native-only/native-ci-recovery/native-only-load-report.md`.

### Priorities after native-only cleanup

Keep the 60s publication setting and Persist unchanged. Repeat measurements
before assigning savings to a proposed optimization.

Then evaluate idle completeness coalescing within existing progress-lag bounds,
and revision-keyed caching of immutable plan-derived schemas. Prior evidence
connects these to extra conditional writes and CPU respectively. Neither requires
a new owner or consensus protocol. Preserve pending updates and read obligations.

Separately review whether the three adapter-introspection projections need
independent durable outputs. Preserve public semantics and the authority
projection's durable catalog-since decoupling. Do not restore raw-catalog
dependencies merely to reduce writes. No consolidation is approved here.

For memory, investigate narrower catalog retention in non-SQL processes before
adding shared caches or services. For remaining statistics/hydration relays,
consider direct observation ownership only with their SQL contracts and cleanup
semantics preserved. These are separate proposals, not more legacy paths to
delete. The original-limit debug upsert OOM also reproduces on the pinned upstream
base. Both sampled heaps are dominated by decoded upsert rows and processing
buffers. This does not establish identical retention or exclude native timing
effects. The existing feedback budget does not bound incoming decoded rows.
A general upsert input-memory bound is separate work, not evidence for any of
these catalog changes. See `upsert-oom-investigation/findings.md` in the handoff's
evidence root for the matched runs and interpretation limits.

### Protection publication conflict-rate scaling

The four-index `index-read-protection` fixture runs 18 publishers at a 1s
publication interval. That is 60 times the default cadence. The shared catalog
serializes commits, so overlapping publication attempts can repeatedly invalidate
a slower publisher's prefix even while peers make progress. Bounded retries
preserve correctness but do not guarantee fair progress under this contention.

Keep the 1s variant as a stress test, not an M3 acceptance gate. Do not optimize
candidate cost further for that regime or increase its retry limits. The current
60s debug workflow passed. In 53.6257s of acknowledged unpaused windows, 17 observed
publishers had 19 successful derived commit outcomes and no failed attempts
(0.3543 successes/s). Startup/default-cadence and post-workflow commits are excluded.
These short windows do not establish steady-state capacity, fairness across all
publishers or a measured Persist CAS rate. Per-publisher evidence and limits are
in `current-four-index-60s-audit.md` and `.json` in the handoff's evidence
directory. The earlier 1s runs are not a controlled comparison.

Distinguish attempted, rejected and successful publications, and report success
gaps as well as aggregate throughput. The cadence alone does not establish an
observed CAS rate or a capacity threshold. Targeted DEBUG timings also include
the separately identified tracing bookkeeping overhead.

### Catalog/cache ownership in non-execution processes

Native followers and controllerd reconstruct full SQL catalogs and retain
whole-shard serialized expression stores. This is retained duplication, not a
full reconstruction on every tick. Valid heaps show substantial extra catalog,
encoded-batch and file-buffer allocations in otherwise small replicas. Detached
file-read stacks do not prove which cache retains those buffers.

Consider caller-specific plan-store admission and lazy opening before a larger
lightweight-catalog or selected-revision-store design. Disabling best-effort
expression caching alone does not remove the raw store because fallback opens
remain. Preserve admin validation, committed selection, cross-cluster producer
schemas and catalog-authorized retirement. No measured saving is claimed yet.

Native startup now reads valid Local expressions under the paired writer's build
identity without reconciling the cache. It retains the same store for written
plans. Independent consistency checks still reconstruct without cached expressions.
In the bounded debug frontier fixture, boot to first completed reconciliation fell
from 31–33s to 5.2–5.5s for the two new replicas. The complete frontier file passes
at its original retry budget. These are wall times, not a CPU or memory benchmark.
Evidence: `frontier-cached-evidence.tar.gz` and the earlier
`frontier-testdrive-failure-evidence.tar.gz` under the handoff's
`upsert-oom-investigation/` directory.

Incremental committed replay still uses `LocalExpressionCache::Closed` when
rebuilding changed objects. Reusing cached expressions there is a separate
opportunity. Validate against each transaction's item versions and optimizer
settings, retaining the existing incremental reader rather than adding shard
scans. No incremental-replay cache change or measured saving is claimed here.

Controller and adapter development binaries have distinct expression-cache
namespaces. Controllers now enqueue their local misses after successful catalog
initialization, respecting cache enablement and without waiting for durability.
The matched debug probe observed controller restart at 21.2s before versus 4.5s
after, and all-native restart at 23.9s versus 7.5s. Repaired restarts recorded 259
builtin hits versus zero before. Fresh startup remained about 53s. These are
single-run observations with unchanged limits, not a production benchmark.
Disabled startup left the controller cold on its first enabled restart, which
then warmed the following restart. See `controller-cache-comparison-audit.md`,
`controller-cache-disabled-result.log` and `controller-cache-probes-evidence.tar.gz`.

Replicas still read the paired adapter's namespace, and cache hits still parse
and SQL-plan builtin views. Concurrent cold startup can duplicate optimization
before adapter publication. Retained-state restart evidence shows no repeated
observed adapter builtin optimization events, unlike the earlier controllers.
Do not infer per-item hits from that timing or change namespace isolation to
avoid first-start misses. The adapter's disabled-cache publisher still checks
handle presence rather than best-effort enablement, although native plan storage
retains a handle. That adjacent cleanup is not part of the controller repair.

Current lifecycle, DDL and subscription evidence shows one intentional candidate
adapter restart at promotion, not two. The later clean DDL exit belongs to the
retired predecessor. The process backend adds its fixed five-second relaunch
delay to the expected candidate restart. Exit code 166 is also used for recovery
from errors, so it alone cannot justify bypassing crash-loop backoff. Preserve
per-process identity and reason when measuring this cost. See
`current-promotion-adapter-exits.md`.

## Metadata-only identities after execution retirement

Durable execution retirement can remove the last client grant without the
adapter transaction that normally removes an already-dropped collection's
metadata. Bootstrap must recover those identities until the catalog deletes
their metadata, preserving the committed shared-shard bounds. The classification
repair does not add reclamation. Track eventual cleanup of these abandoned
identities separately, using catalog authorization and incremental changes rather
than a full-inventory sweep or a new ownership protocol.

## Temporary-item cleanup after native retirement

The legacy temporary-item upgrade tests require durable reclamation after their
session is excluded. Native promotion must preserve those sessions, but explicit
retirement currently does not replace that cleanup. `retired_realizations` removes
execution metadata/protection, not Items. The remaining bulk temporary-item
reclamation call is the unprotected catalog-open path in durable `persist.rs`.
Graceful session close remains separate.

Items store their session UUID, while `mz_sessions_raw` reports client incarnation
asynchronously. `coord/retired_rows.rs` can remove retired session observations
without reclaiming their Items, so joining those observations is not a durable
ownership contract. This is the unresolved ownership issue discussed on October 30,
not a new regression from the lifecycle harness changes.

Approved separate repair: retain the session UUID for visibility and store the
existing adapter execution incarnation on temporary Items in the same creation
transaction. Keep normal disconnect cleanup. Adapters reclaim Items only after
catalog-confirmed execution replacement or deployment retirement, preserving
surviving sessions and promotion alone. Do not add a creation transaction,
controllerd maintenance owner or per-connection catalog record. `mz_sessions`
remains observational. Implementation and lifecycle verification remain outstanding.

## Native source-status latency

The native source-failpoint lifecycle passed, but its final successful status-query
attempt reported 673 seconds in a capped debug fixture. The explicit multi-process
cluster sizes were retained. This does not establish responsive SQL, and the delay
has not been attributed to resource limits or any code path. Trace prepare and
execution before changing deadlines or publication settings. Evidence:
`~/.local/share/aj/m3-evidence/native-only/native-source-lifecycle-verification.md`.

## Duplicate progress observation

Fresh scan tracing at 60s identifies additional authority/placement imports and
retained metadata watches, not reintroduced duplicate frontier-observation
sources. MV correction readers have a separate contract. Eighteen logical IDs
share one WAL watch, and all sampled metadata inputs carry policy obligations.
Authority, object-global-IDs and index-columns are concrete arrangement-reuse
candidates, subject to retention boundaries. Raw-catalog overlapping observers
are small candidates, not guaranteed savings. The total scan delta is unstable:
the fresh baseline loses PubSub messages and performs more writer recovery.
Do not infer caller necessity or a fixed native overhead from retry counters.


Persist-internal repairs are deferred. The current
resource comparison (`resource_review`) ranks
caller-side targets instead: frontier SQL/arrangement reuse, a shared query WAL
reader, immutable producer facts, and evidence-driven observation batching.
The measured memory premium includes caches, allocator residency and loaded code.
Small plan-lifetime cleanups alone do not explain or remove it.

The measured scan audit (`replica_scans`)
confirms duplicate empty scans across same-process readers and upper observers.
Distinct consumers do not require independent fallback polling. Keep oracle/
catalog certification separate from background recovery. The catalog helper
itself accounts for little traffic, and currently supplies normal recovery for
its bounded row listener. Removing it alone would rely on incidental operations
or slower lease heartbeats.

Concrete candidates are skipping satisfied pre-rollup refreshes, sharing
same-process shard recovery, injecting an existing full WAL reader into query
snapshots, and removing query compute waits' cache-only timer after verifying
notification coverage. Protected prewarming can reuse existing catalog progress
for its separate protection projection. Readiness reference freshness needs an
explicit contract decision before replacing its fresh target with cached state.

Preserve pre-install/restart coverage, process placement, cancellation, complete
logical inputs and missing-versus-completed progress. Existing MV mint upper
observations and lagging read-back frontiers are different facts. The report
contains measured caller/shard attribution and source-linked evidence under
`~/.local/share/aj/m3-evidence/replica-scan-necessity/`. Implementation needs a
bounded follow-up, not blanket listener removal or a new ownership protocol.

## Recurring full reads

### Retired-row cleanup

The call to `collect_retired_rows` in `src/adapter/src/coord.rs` runs after
read-protection publication. `src/adapter/src/coord/retired_rows.rs` prevents overlapping local
tasks, but each new task scans current storage statuses, sessions and subscriptions.
The status cursor is in `src/storage-client/src/collection_writer.rs`.

Retain observations indexed by incarnation and exact shard identity. Incorporate
both row changes and retirement certificates, then examine affected owners.
Triggering only on retirement is insufficient: late predecessor writes can arrive
afterward. Preserve exact-upper conditional retractions, successor rows and the
txn-WAL path for session/subscription cleanup. Conflicts must refresh observations,
not replay old negatives. This is a strong next bounded optimization candidate.

### Caught-up replica-status analysis

`src/adapter/src/coord/caught_up.rs::analyze_replica_looping` starts a fresh
`snapshot_and_stream` during each enabled readiness check. It scans retained
replica-status history before selecting the relevant deployment and time window.
This is separate from the retained frontier-reference reader.

A task-owned listener could maintain qualifying events and their expiry times.
Preserve the selected observation frontier, deployment filtering, multiplicities,
retractions and expiration without new input. Release the reader when readiness
checks end. Do not silently replace the current selected-cut semantics with
unrestricted latest state. This is another bounded incremental-read candidate.

### Arrangement-size history samples

The `arrangement_sizes_snapshot` task in
`src/adapter/src/coord/message_handler.rs` reads complete size and hydration relations at one oracle timestamp on each configured
sampling interval. Publication checks freshness again.

Retained inputs could assemble samples at a common completed frontier. Consider
the replica-owned introspection opportunity below before adding another adapter
cache. Keep restart freshness and the publication check. History still needs
periodic samples of unchanged values: incremental inputs do not imply
changed-only history output.

### Native storage-metadata resolution

The execution loop in `src/clusterd/src/catalog_follower.rs` follows catalog
changes incrementally but rebuilds its wanted set and calls
`storage_metadata::resolve` each reconciliation turn. That function reconstructs
definitions, resolves schemas and reads retained progress for wanted dependencies
and outputs. Warm progress and immutable-plan hits no longer refresh consensus.

Cache immutable resolution by GlobalId, shard, schema version and selected plan.
Invalidate from existing catalog effects and retry missing prerequisites
separately. Preserve exact table versions, MV writer selection and catalog-prefix
consistency. Physical uppers remain dynamic observations. This is repeated
metadata reconstruction, not a full data-shard snapshot.

### Native compaction alias scans

`src/clusterd/src/catalog_follower/compaction.rs::reconcile` recomputes shard
bounds each successful reconciliation turn. It scans all collection-to-shard
mappings, although only wanted shards receive work.

An incremental shard-to-alias permission index could restrict recomputation to
affected shards. Include every alias of each wanted shard, including aliases
outside the replica's wanted set. Missing bounds prohibit advancement. Keep
pending retries for failed or rate-limited downgrades, even without new catalog
changes. This is not an all-shard cleanup sweep.

## Direct-owner opportunities

These remaining relays consume native observations or committed catalog state.
They are not alternate lifecycle owners. The public frontier views have no
legacy fallback. Remove each relay together with its dependent SQL input only
when a replacement preserves the complete observation contract. Idle upper
advancement does not repair stale observations, but removing it while retaining
SQL dependencies can strand reads.

### Remaining unified compute introspection

Deferred beyond M3. Same-deployment adapters can publish overlapping deployment
partitions. Preserve the current producers until a complete ownership replacement
is designed.

`src/adapter/src/coord/introspection.rs` subscribes to replicas and relays batches
into storage. Reinstallation deletes prior rows by replica ID. Error counts,
hydration times and arrangement sizes remain examples of indirect publication.

Replica execution can publish these facts under existing deployment/incarnation
identities, with separate public reductions. Preserve predecessor suppression,
unknown-after-replacement behavior, cumulative/history contracts and freshness.
Frontier reduction rules do not automatically apply to other metrics.

Hydration also feeds arrangement-history sampling in
`src/adapter/src/coord/message_handler.rs`. That consumer obtains freshness from
the introspection subscription and reads `mz_compute_hydration_times_raw`.
Moving only the public view would leave this dependency behind. Native frontier
observations do not contain hydration durations or operator-level status.

### Storage statistics and webhook contributions

Deferred beyond M3, including overlap between same-deployment adapter scrapers.

`src/storage-controller/src/lib.rs` receives replica statistics, and
`src/storage-controller/src/collection_mgmt.rs` starts scrapers that publish
controller maps.
Replica-produced statistics are candidates for direct execution publication.
Webhook contributions remain adapter-owned and need aggregation, not relocation
to replicas or a designated adapter.

Preserve cumulative counters across restart and retirement, inactive-stat
retention and webhook accounting. The relay boundary is clear. The smallest
complete conversion still needs investigation.

### Maintained physical dependencies and MV refresh state

M3 requires a two-adapter reproduction before deciding scope. If one adapter
retracts another's rows, repair row scoping using the frontier ownership model.
Otherwise retain this as a follow-up. Source inspection alone is not reproduction.

`src/adapter/src/coord/introspection/frontiers.rs` publishes selected physical
imports and refresh observations through differential introspection collections.
The maintained dependency branch of `mz_compute_dependencies` still reads
`mz_maintained_compute_dependencies`. Materialization dependency and lag views
consume it. Subscription physical imports are a separate, valid adapter-owned
branch and must survive any change.

A replacement must expose actual selected imports, not substitute logical object
dependencies. Establish whether committed plan metadata supports a projection
before choosing a mechanism. MV refresh observations combine committed schedules
and initial as-ofs with storage write progress. Preserve unknown progress and
completed schedules. Remove both old sources and their adapter-local publication
state only with complete replacement inputs, not by stopping the frontier timer.

### Catalog-derived projections and lifecycle history

The design retains originator-only projection publication and its scoped M3
inconsistency allowance. Suitable derived facts could become raw-catalog-backed
relations. Transactionally sourced lifecycle events are a separate possible
follow-up. Neither is approval to add projection-recovery protocols, watermarks
or another policy owner during this milestone.

## Separate correctness investigations

### Critical protection at collection birth

Source inspection found a possible ordering gap, not yet reproduced. The native
follower installs and schedules execution before `compaction.reconcile` attaches
critical readers. If the creating adapter stalls before its storage attachment,
a leased Persist reader could advance since beyond the committed permission.
`register_leased_reader(use_critical_since=true)` falls back to trace since when
no critical reader exists. Attaching one later cannot restore discarded history.

Reproduce this ordering before changing physical-readability checks. Establishing
protection belongs at collection birth/execution attachment. A fresh `recent_since`
can detect an existing mismatch but cannot prevent a later race. Existing checks
remain in place. The consensus-read audit is recorded in
`~/.local/share/aj/m3-evidence/consensus-read-removal/readability-review.md`.

### Shared plans for peer-created indexes

A fresh two-version deployment reproduced a post-admission gap: an index created
through generation 0 serves queries there, while generation 1 waits for its own
build's selection. An index created before generation 1 joins works because
bootstrap prepares the missing selection. Evidence:
`~/.local/share/aj/m3-evidence/authority-projection/replacement/`, runs
`run-1793730433` and `run-1793730854`.

The agreed M3 scope assumes both prototype builds understand the same complete
committed-plan encoding. Native deployments share selections without a separate
opt-in, per-build replanning or new coordination protocol. Real-build admission,
object versions, dependencies and read protection remain binding. Upstream
pinned-plan format/version support will replace the compatibility assumption.
This does not claim compatibility between arbitrary releases or migrate old
exact-build selections.

The live regression is `test/controllerd/shared-plans.py`. The default-debug
witness passed with both builds admitted before table/index creation in either
direction. Both served indexed reads and table writes, retaining their SQL
sessions and every process identity. Evidence and limitations:
`~/.local/share/aj/m3-evidence/shared-plans/verification.md`, run `1793800485`.

### Data-shard closure during txn-WAL application

The seeded replacement workload also exposed an adapter panic during concurrent
writes after temporary-table creation and deletion. The stack reaches
`small_caa` through `empty_caa` in `TxnsHandle::apply_le`'s register/forget branch.
This is a data-shard operation despite the panic text saying the txn shard closed.

`empty_caa` and `apply_caa` accepted a closed data shard when the cached
upper showed closure on entry. Their CAS mismatch paths instead panicked on closure.
Independent clients can discover closure at different points. Lawful retirement
can finish while a peer still has an application attempt in flight. Forget
completes preceding application, not every peer's outstanding attempt.

Read-only inspection of all 116 saved shards and the WAL's 221 retained records
identified five closed WAL participants. All were dropped temporary tables with
catalog retirement followed by committed Forget. The WAL and seeded user table
remain open. Two temporary shards reached terminal metadata about 110 ms before
the first panic. This supports the lawful-retirement race, but the logs do not
identify the exact failing shard or reconstruct the in-flight CAS ordering.

Stale new writes require separate handling. `Txn::commit_at` refreshes
membership and protects its check with the WAL CAS, so completed Forget prevents
a new commit. Its previous membership assertion could crash a lagging writer.
Between catalog DROP and Forget, membership
can still permit a staged write. That alone does not establish a consistency
violation for a write concurrent with DROP.

The scoped repair accepts closed data uppers during redundant application and
returns a typed membership error for new writes. The table worker propagates
rejection, and group commit retains transaction contributions until success.
Only transactions touching the rejected target fail, including all their writes
to other tables. Unrelated user transactions and builtin diffs remain pending.
WAL closure remains invalid, including during initialization. Catalog retirement
and Forget acknowledgement gates are unchanged.

`src/txn-wal/src/retirement_tests.rs` uses separate Persist caches to discover
closure during CAS, checks exact surviving contents and listen progress, and
rejects cross-shard transactions containing a forgotten target. Both data-apply
regressions fail with the original closure assertions. Adapter boundary tests
cover missing routes, atomic rejection, continued writes, SQL variable rollback,
selective group-commit recovery and builtin notifications. The original
four-adapter runtime reproduction remains to be rerun.

Retaining transaction contributions adds a row-data copy during consolidation.
Rejected uploaded batches also remain for Persist GC, as with upper-conflict
transactions dropped by their callers. These are separate performance and
reclamation follow-ups, not reasons to weaken atomic rejection or retirement
authorization.

Evidence: `authority-projection/replacement/run-1793731130/initial-g{0,1}.log`
under the evidence root. The failure occurred during INSERT after the first
measurement succeeded. It is separate from authority projection publication.
The read-only investigation is in `txn-retirement/verified-summary.json`, with
catalog/WAL history, per-shard states and reproducible decoder scripts alongside.
Repair checks and remaining failures are recorded in `txn-retirement/repair.md`.

### Startup history pruning

`src/adapter/src/coord.rs::prune_storage_usage_events_on_startup` and adjacent
startup history pruning derive negatives from snapshots and submit ordinary
builtin updates. Their comments rely on startup exclusivity. Concurrent adapters
make process-local startup exclusivity insufficient as a general argument.

Audit the real multiwriter retirement/append ordering before treating these as
harmless one-shot reads. This is a concrete concern, not a reproduced failure.
Do not fold its repair into frontier ownership without scope agreement.

## Snapshots that are not evidence of a recurring rescan bug

- Catalog following retains a listener. `cached_snapshot` clones typed in-memory
  state rather than rereading the Persist shard.
- Txn-WAL initializes a subscription and consumes it incrementally. A per-shard
  `data_snapshot` token is not a WAL-wide scan. Keep the approved GC-validation
  repair separate and do not add an all-shard sweep.
- Statistics restoration and collection-manager retention preparation run at
  task startup, not every maintenance tick.
- Bootstrap configuration reconciliation needs a consistent desired-minus-current
  base. Correct conflict handling matters more than avoiding this one-time read.
- Controller orphan cleanup deliberately lists services before its catalog
  snapshot. A stale cached inventory would violate that ordering.
- User queries and recovery snapshots require their own semantic justification.
  A method named `snapshot` alone is not a finding.

The current frontier repair already retains differential-writer observations,
samples physical storage progress cooperatively, moves replica reports to their
owner, and retains caught-up frontier observations. Do not count those paths as
untouched findings in subsequent audits.

## Nonlocal compute permission staging

The legacy adapter-owned compute path forwards global permission updates to
`compute-client::Controller::apply_compaction_bound`. Unknown IDs enter
`staged_compaction_bounds`; removal occurs when the collection is installed.
A temporary index belonging to another adapter never installs locally, so its
staged permission can remain until process restart. Native replica-owned compute
skips this adapter delivery path and is not affected.

Audit retirement of never-installed permissions before changing this behavior.
Preserve legitimate staging before collection creation. This is an ownership and
lifetime follow-up, not permission to weaken catalog or storage validation.

## Approved M3 scope limits

Expression-cache reclamation uses release-version membership. Executable-specific
cache variants of a live development release can still accumulate. Reclaiming
those variants needs more precise live-build information and is deferred. Written
plans remain authoritative and are not cache garbage.

Sink selection retains the current rule rather than adding a durable placement
record. Unlike upstream's sticky running-sink selection, introducing a lower-ID
eligible replica can change the selected execution. Do not retain placement only
locally: different replicas could then select competing sink writers. Revisit
stickiness separately if this handover cost warrants shared placement metadata.

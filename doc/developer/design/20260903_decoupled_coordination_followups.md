# Decoupled coordination: architectural follow-ups

This is an audit backlog, not an expansion of Milestone 3. Implementation of
the opportunities below needs separate scope agreement. Findings are based on
source inspection unless runtime evidence is cited.

The direction is incremental observation and publication by the component that
owns a fact. Reuse existing identities, catalog changes and Persist listeners.
Do not introduce a designated maintenance adapter, another consensus protocol,
or a general sampling framework.

## Backend writes and blob consumption

The [backend attribution](20260903_decoupled_coordination_backend_load.md)
reconciles CAS/blob calls and bytes against actual events. The
[shared frontier repair](20260903_decoupled_coordination_shared_frontiers.md)
removed duplicate observation-source reads and reduced total GETs by 28.7%.
The catalog journal and progress fanout remain. A separate retained dependency
MV lacks a common source predicate. Do not redirect its history through the
unretained shared catalog arrangement.

At 10s publication, most CAS excess was empty completeness fanout into 50 durable
advancing catalog outputs. The [60s comparison](20260903_decoupled_coordination_publication_interval.md)
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

The [fresh logical-component comparison](20260903_decoupled_coordination_logical_load.md)
confirms that output completeness, rather than raw catalog CAS count, dominates
extra conditional writes. Source-linked CPU evidence also identifies repeated
immutable written-plan decoding for producer schemas. Cache those facts by
selection/revision while retaining version checks and pending misses. Filtering
irrelevant protection kinds before the shared SQL arrangement is a separate
candidate. It cannot by itself remove empty completeness writes or journal bytes.

The 60s comparison also finds legacy frontier progress retained by dynamic SQL
branches. Remove inactive input dependencies before stopping their writers,
otherwise protected-mode results can stall. This is a view/progress design
change, not an unconditional writer-disable patch.

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
[resource comparison](20260903_decoupled_coordination_resource_review.md) ranks
caller-side targets instead: frontier SQL/arrangement reuse, a shared query WAL
reader, immutable producer facts, and evidence-driven observation batching.
The measured memory premium includes caches, allocator residency and loaded code.
Small plan-lifetime cleanups alone do not explain or remove it.

The [measured scan audit](20260903_decoupled_coordination_replica_scans.md)
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

### Remaining unified compute introspection

`src/adapter/src/coord/introspection.rs` subscribes to replicas and relays batches
into storage. Reinstallation deletes prior rows by replica ID. Error counts,
hydration times and arrangement sizes remain examples of indirect publication.

Replica execution can publish these facts under existing deployment/incarnation
identities, with separate public reductions. Preserve predecessor suppression,
unknown-after-replacement behavior, cumulative/history contracts and freshness.
Frontier reduction rules do not automatically apply to other metrics.

### Storage statistics and webhook contributions

`src/storage-controller/src/lib.rs` receives replica statistics, and
`src/storage-controller/src/statistics.rs` periodically publishes controller maps.
Replica-produced statistics are candidates for direct execution publication.
Webhook contributions remain adapter-owned and need aggregation, not relocation
to replicas or a designated adapter.

Preserve cumulative counters across restart and retirement, inactive-stat
retention and webhook accounting. The relay boundary is clear. The smallest
complete conversion still needs investigation.

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

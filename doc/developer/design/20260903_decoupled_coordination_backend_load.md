# Idle backend load: caller and shard attribution

**Most additional CAS calls are empty completeness writes, not retries. The
largest avoidable GET pattern is duplicate frontier-source consumption. Most
additional blob bytes instead come from the read-protection catalog journal and
its distributed compaction. These are different optimization targets.**

No production behavior changed. Temporary instrumentation was removed after
building the candidates. Persist-internal changes remain deferred.

## Matched measurements

This follows the [resource comparison](20260903_decoupled_coordination_resource_review.md).
Native is `604c47268074fcd100910dc9ab4c4720f1ba1662`. Upstream is pinned to
`77ae0a3034efefb22919b6257dd40e5ebc1827bb`, not an unspecified latest main.
Both use debug builds, one frontend, four single-worker replicas, private
PostgreSQL 18 SERIALIZABLE consensus and file-backed blobs. Native protection
publication remains 10 seconds. After creating an indexed one-row table, each
fresh deployment warmed for 300 seconds and ran two 300-second idle windows.

Both disable the metric sink, incremental compaction and columnar accumulable
diffs. Upstream additionally sets `compute_mv_sink_shared_batch_max_bytes=0`,
matching the earlier comparison because that feature is absent from native.
This is a controlled comparison, not upstream with every default enabled.
Builds, measurement runs and the separate plan witness ran sequentially.

| Operation | Main calls/s | Native calls/s | Main KiB/s | Native KiB/s |
|---|---:|---:|---:|---:|
| Consensus CAS | 83.03 | 120.64 | 47.92 | 79.87 |
| Blob GET | 2.28 | 14.37 | 26.97 | 307.40 |
| Blob SET | 4.55 | 9.61 | 95.02 | 281.60 |
| Blob DELETE | 4.30 | 9.38 | 80.59 | 250.07 |

CAS counts include mismatches and errors. Its byte counter counts committed
payload only. Attempted native CAS payload was about 80.54 KiB/s. GET/SET bytes
are successful values transferred through the API. DELETE bytes are the sizes
of removed objects, **not DELETE request payload**. These counters exclude blob
cache hits. They are not physical disk operations or S3 HTTP requests.
Neither run fired a blob hedge or recorded a blob-cache eviction in its windows.

The process split explains where the excess runs:

| Process | Main CAS/s | Native CAS/s | Main GET/s | Native GET/s |
|---|---:|---:|---:|---:|
| environmentd / adapterd | 30.68 | 36.61 | 0.15 | 0.48 |
| Catalog replica | 52.22 | 79.41 | 2.07 | 9.50 |
| Other three replicas combined | 0.14 | 4.49 | 0.06 | 3.87 |
| Separate controllerd | absent | 0.13 | absent | 0.51 |

Pubsubd has no measured backend events and no corresponding Persist counters.
The JSON retains unavailable metrics rather than treating them as measured zero.

Every available completion, success, failure and byte counter reconciles with
the event ledger within its actual scrape interval and measured clock bounds.
Event-derived breakdowns below use nominal scrape midpoints, so their rounded
totals can differ slightly from counter-derived rates. All measured events have
a work classification. Shared critical-reader purpose labels can be ambiguous,
but process, command and target shard remain recorded.

## CAS: progress fanout dominates

Native had about 119.61 committed CAS/s, 1.01 mismatches/s and two backend errors
across both windows. Upstream had about 83.04 committed CAS/s and 0.007
mismatches/s. The two native errors were followed by retries and later successful
appends. The INFO trace does not establish their database error codes.
Both fixtures passed their final SQL checks. This is not a retry storm.

### Fifty durable catalog outputs follow a faster input frontier

Catalog-replica **committed empty appends** rose from 50.08/s to 70.95/s.
Both deployments have 50 catalog MV outputs in the timestamp linkage analysis.
Filtering unchanged rows does not filter completeness progress. Each MV has its
own output shard and must durably advance its upper before serving later reads.

The trace joins an output append's exact upper to the committed raw-catalog
append with that upper. Native's linked output writes split into:

- **53.32/s** carrying an adapter's empty catalog-advance timestamp.
- **17.57/s** carrying a replica's nonempty catalog-publication timestamp.
- **0.04/s** carrying an adapter's nonempty catalog-publication timestamp.

The replica publishers commit changed read requirements and compaction permission
at the existing staggered cadence. Thus a small number of protection commits
produces many empty output writes. This is direct timestamp linkage, not an
estimate from similar rates. It is not a claim that removing those publications
would save exactly 17.57 CAS/s. They are required, and sink feedback can coalesce
different input progress after a scheduling change.

Native already shares ordinary catalog input through `mz_catalog_shared_ind`.
Sharing an input arrangement does **not** combine independent output-shard CASs.
The MV sink's `mint` is event-driven and already combines progress while an
earlier append remains outstanding. Neither sink implementation has a steady-state
time-window knob. The separate witness confirmed the default, non-sync sink.

### Frontend completeness work also increased

Frontend committed empty appends rose from 22.87/s to 29.27/s. Some of this is
classification/ownership change: legacy frontier data writes become empty
advances of the retained legacy relations while native producers publish elsewhere.
Renamed introspection relations must not be counted as entirely new workloads.

Two concrete additional patterns remain:

- The adapter advances each native frontier-observation shard about once per
  second even while independent producers append data. These are registered
  fallback completeness timers. Producer writes reset neither timer.
- Source statistics has **two partition writers**, one for replica-owned rows
  and one for NULL-replica webhook rows. Each has its own one-second progress
  timer, producing about two empty appends/s versus baseline's one. Scrapers
  already suppress empty corrections. This is not doubled scraping frequency.

The current helper correctly skips a target already covered by a shared upper.
However, a tick samples a new `now`, so a recent but earlier producer write does
not satisfy it. Removing fallback advancement outright would strand unchanged
or empty relations. Pending differential commands also cannot be acknowledged
merely because a peer advanced progress.

### Cooperative since application is a smaller cost

The native catalog replica adds about 2.52 committed critical-since CAS/s, or
2.66 attempts/s including mismatches. Adapter and replica can apply the same
catalog-authorized shard permission. The trace contains 481
committed requests over the two windows whose proposed frontier did not exceed
a preceding committed request for that shard and shared critical reader.

This is a candidate set, not measured removable work. An opaque mismatch can
commit without applying the requested bound, and cached visibility can lag.
`read_protection::downgrade_since` already checks `handle.since()`, but that is
handle-local, not the shared physical since. **There is no public
`PersistClient::observed_since` at this revision.** `recent_since` performs reads.
Do not replace these writes with new consensus reads or designate a single owner.

## Blob calls: duplicate consumers and distributed maintenance

| GET work | Main calls/s | Native calls/s | Main KiB/s | Native KiB/s |
|---|---:|---:|---:|---:|
| Compute source consumption | 2.04 | 9.07 | 15.50 | 82.22 |
| Incremental row followers | 0 | 2.08 | 0 | 111.87 |
| Compaction inputs | 0 | 3.05 | 0 | 105.69 |
| Remaining metadata and probes | 0.24 | 0.16 | 11.47 | 7.62 |

The remaining metadata bucket includes rollup reads, GC, acquisition and liveness
probes. A rollup-read label identifies the operation, not every waiting consumer
that might have triggered that metadata refresh. No optimization is inferred
from that small baseline bucket.

### Two frontier indexes independently fetch the same cold parts

The actual installed plans for `mz_frontiers_ind` and
`mz_cluster_replica_frontiers_ind` both directly import the replica and storage
observation sources. The independent plan witness connects their source-reader
IDs to their Timely roots and stored plans.

Within the catalog process, the main native run recorded these repeated GETs of
identical immutable keys by distinct source readers:

| Input | Repeated source GETs | Median completion gap | Maximum gap |
|---|---:|---:|---:|
| Replica observations | 1,787 | 0.30 ms | 11.93 ms |
| Storage observations | 665 | 0.28 ms | 13.34 ms |
| Raw catalog | 290 | 0.09 ms | 2.67 ms |

The first two account for about **4.08 GET/s**, roughly one third of the total
GET excess. Including raw catalog gives about 4.56 repeated source GET/s.
There were no intervening local deletes or cache evictions. BlobMemCache checks
before awaiting the backend and inserts afterward, without coalescing concurrent
misses. The traces show that window being hit. These are not logical fetch
counters that accidentally include cache hits. A further single repeat involved
a source and compaction, not two source consumers.

Raw-catalog duplicates involve three source readers. In the measured windows,
operator roots 3 and 29 each issued 234 source GETs, and root 25 issued 56.
The separate plan witness maps these roots to `mz_catalog_shared_ind`,
`mz_object_dependencies` and `mz_catalog_deployment_authority`, respectively.
The authority source is not exempt from duplicate reads. The stored dependency
plan has several branch filters but no common source filter. The authority plan
has a source predicate, but the trace does not measure which parts it pruned.

The dependency MV is retained metrics. Its direct raw import is deliberate:
blindly redirecting it through the shared arrangement could retain unrelated
catalog history. A common source-level kind predicate is a separate candidate
that does not require sharing its retention boundary.

### Maintenance follows shared data across process caches

Every measured external compaction GET in native read a blob written by another
process. Blob-key joins to the actual SET ledger establish this:

- Replica-observation uploads: 2.08 GET/s and 8.36 KiB/s.
- Replica-observation compaction outputs: 0.79 GET/s and 15.01 KiB/s.
- Raw-catalog compaction outputs: 0.18 GET/s and **82.32 KiB/s**.

An incremental follower consumes original updates. It need not read subsequent
compaction outputs containing the same logical history. Another writer's later
compaction can therefore need those uncached outputs. Upstream's measured
compactions used hot local data and generated no external GETs.

Maintenance placement is opportunistic. Do not interpret a particular replica's
GC or compaction rate as a fixed ownership assignment. Removing an independent
catalog follower, electing a maintenance owner or adding another cache is not
justified by these measurements.

### Bytes are mostly raw-catalog churn, not frontier payloads

Raw catalog accounts for **241.41 of 307.40 KiB/s** of native GET payload and
**119.76 KiB/s** of compaction SET payload. Its direct uploads are only 25.12 KiB/s,
but participants consume the journal and repeatedly rewrite compacted history.
The additional compaction SET bytes are almost entirely this catalog work.
Per-record-kind encoded byte totals were not measured.

| SET work | Main calls/s | Native calls/s | Main KiB/s | Native KiB/s |
|---|---:|---:|---:|---:|
| Data uploads | 2.05 | 5.86 | 15.59 | 50.32 |
| Compaction outputs | 1.86 | 2.84 | 28.72 | 148.54 |
| Rollups | 0.65 | 0.91 | 50.71 | 82.75 |

About 82% of native SET bytes are maintenance outputs. Static catalog results can
also be rewritten without new logical rows: `mz_databases` and `mz_connections`
each had 862 committed empty advances, no data appends, and 124 compaction SETs.
Empty-range insertion can fuel merges containing older nonempty batches.

DELETEs were predominantly GC. Native batch cleanup was 0.18/s and discarded
compaction cleanup 0.008/s. Failed/obsolete work exists, but it does not explain
the larger upload or maintenance rates. Query-snapshot WAL reuse and retirement
snapshot cleanup remain ownership opportunities, but neither generated measured
external snapshot GETs here. Do not credit them with this blob-load reduction.

## Ranked caller-side follow-ups

1. **Share the frontier inputs or common validated intermediates.** Replace
   duplicate source imports with existing index-import machinery. The measured
   opportunity is 4.08 repeated GET/s plus duplicated computation identified by
   the earlier CPU study, not a promised saving. Preserve deployment/incarnation,
   placement, object/shard identity and missing-versus-completed semantics.
   Retain the durable authority barrier and verify recovery/read-hold lifetimes.

2. **Expose a common kind predicate for dependency-source consumption.** The
   generated SQL already filters individual branches to Item, GidMapping and
   ClusterIntrospectionSourceIndex. Make a common restriction available at the
   source and check the resulting physical plan and real part pruning. A source
   predicate alone does not establish fewer external reads, as the authority
   source's remaining GETs illustrate.
   Do not remove dependency edge cases or route retained history through the
   unretained shared arrangement. This is not yet an implemented rewrite.

3. **Credit peer progress in completeness scheduling.** Keep all row owners and
   idle fallbacks, but schedule the next progress-only deadline from already
   committed shared progress. Preserve the one-second idle completeness budget,
   subject to existing scheduling/backend delays, as well as exclusive-upper
   semantics, clock-change behavior and command acknowledgements.
   Simply skipping a tick then sleeping another second can double the lag.
   Differential retries should not keep moving a pure-progress target forward.
   Current per-task rates are attributable load, not guaranteed savings.

4. **Discuss MV output coalescing only with an explicit latency budget.** This
   addresses the largest CAS amplification. Keep original row timestamps and
   publisher cadence. Readers needing an uncommitted upper must wait. Mint cannot
   infer an empty correction merely because no rows have arrived yet. `REFRESH`
   rounds timestamps and is not a transparent substitute for permission-bearing
   outputs. Multi-output plan packing alone cannot eliminate per-shard CASs.

The shared-since shortcut needs a Persist API addition and stays deferred.
Further reductions in journal publication/compaction bytes require a separately
scoped review of protection representation and its actual obligations. There is
no measured zero-tradeoff caller fix for that entire cost. Do not slow protection
publication, discard peer state or split the authoritative journal speculatively.

## Evidence and limits

[Checked-in measurements](20260903_decoupled_coordination_backend_load.json)
contain window and process rates, counter reconciliation, timestamp linkage,
duplicate-key examples, producer joins, cache metrics and binary provenance.
Raw evidence is under `~/.local/share/aj/m3-evidence/backend-load-attribution/`:

- `native-audit-1793996274/` and `main-audit-1793998684/`: logs, scrape timing,
  counters, compressed event ledgers and derived attribution.
- `native-plans-1793999722/`: separate short plan/ownership witness. Its startup
  window is **not** part of the idle cost comparison.
- `derive.py`, `summarize.py`, build manifests, binary hash inventories and the
  three source audits preserve detailed derivation and architectural constraints.
- `misc/benchmarks/native-idle/`: reusable reducer, tests, fixture and reversible
  native/upstream audit patches. Apply controller-metrics.patch for native metrics.

Tracing can perturb scheduling and batching. Call rates reproduce the previous
comparison closely, but byte rates vary with maintenance and cache timing.
These are finite-age file-blob fixtures, not a cloud scaling or network benchmark.
No predicted repair savings were measured, and no production repair was made.

Both supported diagnostic builds and all three fixtures passed. Targeted Python
formatting, lint and reducer tests passed. The full repository formatter still
fails on the pre-existing overlong generic call in `src/catalog/src/catalog.rs`.
The initial native build reused stale upstream workspace artifacts in a shared
target directory. The exact retry reproduced it, then rebuilding workspace
packages fixed it without changing source. Build logs retain that failure.
Broad CI remains non-green and was not rerun. This does not complete M3.

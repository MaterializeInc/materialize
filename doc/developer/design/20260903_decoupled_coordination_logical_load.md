# Idle load after shared frontier inputs

**The branch still uses about twice the idle CPU and memory. Catalog work explains
most of the increase, not controllerd process management or the transaction WAL.**
The catalog replica accounts for 62% of the CPU increase. Empty catalog-output
progress explains most of the extra CAS calls. Protection publications and their
consumption/compaction explain most of the extra blob bytes. Replicas also retain
SQL catalog and expression-store state that upstream keeps out of those processes.

This is a measurement and source audit. No production behavior, Persist code or
publication cadence changed. The [data](20260903_decoupled_coordination_logical_load.json)
contains per-process counters, component/activity accounting, profiles and limits.

## Matched comparison

Fresh debug deployments, one adapter/environmentd, four single-worker replicas,
an indexed one-row table, PostgreSQL 18 SERIALIZABLE consensus and file blobs.
Each warmed for 300s, then ran two 300s idle windows. Builds were not running.
Read-protection publication is 10s. The baseline is pinned **77ae0a3034**, not
current main. Candidate production sources match **a0d87d072a**.

Both use Rust 1.98.1 and the same backend-audit instrumentation. Metric sink,
incremental compaction and columnar accumulable diffs are disabled in both.
Baseline MV shared batches are disabled to match the candidate. These are
matched diagnostic settings, not a claim about default production costs.

| Metric | Upstream | Branch | Branch repeat |
| --- | ---: | ---: | ---: |
| CPU cores | 0.635 | 1.237 | 1.238 |
| PSS MiB | 1353.849 | 2865.299 | 2927.241 |
| Consensus scans/s | 2.938 | 7.583 | 6.422 |
| Consensus scan KiB/s | 123.672 | 226.179 | 224.171 |
| CAS attempts/s | 83.010 | 118.545 | 120.445 |
| CAS committed KiB/s | 47.894 | 78.590 | 79.584 |
| Blob GET/s | 2.142 | 9.899 | 10.145 |
| Blob GET KiB/s | 16.994 | 294.050 | 319.845 |
| Blob SET/s | 4.548 | 9.564 | 9.653 |
| Blob SET KiB/s | 93.885 | 276.197 | 288.420 |
| Blob DELETE/s | 4.990 | 10.279 | 9.933 |
| Blob DELETE KiB/s | 89.763 | 263.530 | 258.652 |

PSS is the mean of end-window snapshots, not peak memory. Backend bytes are API
payloads, not physical storage/network billing. PostgreSQL itself used about
0.017 upstream versus 0.027 branch cores. Its container memory, including page
cache/shared memory, was 289 versus 395 MiB. No Materialize CPU throttling was
recorded. Repeats show scan and GC variability, not a confidence interval.

## Process placement

| Role | CPU upstream / branch | PSS MiB upstream / branch | Scans/s upstream / branch | CAS/s upstream / branch |
| --- | ---: | ---: | ---: | ---: |
| Frontend | 0.273 / 0.268 | 586.2 / 589.8 | 1.187 / 1.560 | 30.663 / 36.524 |
| Controller | included in envd / 0.025 | included in envd / 400.4 | included in envd / 0.063 | included in envd / 0.122 |
| Standalone relay | included in envd / 0.033 | included in envd / 37.7 | included in envd / 0.000 | included in envd / 0.000 |
| Catalog replica | 0.316 / 0.688 | 481.5 / 870.8 | 1.685 / 5.242 | 52.212 / 77.455 |
| System replica | 0.015 / 0.073 | 93.3 / 303.8 | 0.015 / 0.276 | 0.053 / 1.502 |
| Probe replica | 0.015 / 0.071 | 98.6 / 317.5 | 0.012 / 0.205 | 0.040 / 1.430 |
| Quickstart replica | 0.016 / 0.077 | 94.3 / 345.3 | 0.038 / 0.236 | 0.042 / 1.513 |

The frontend alone is nearly unchanged. The catalog replica adds 0.373 cores,
the other replicas together add 0.176, and controller plus relay add 0.058.
Upstream embeds provisioning and PubSub in environmentd, so the new processes
are not independently comparable to zero upstream work.

## CAS and blob work by responsibility

Every completed audited CAS/GET/SET/DELETE is assigned once. Group counts and
bytes conserve the event totals and reconcile with scraped counters. This is
logical dataset **and activity** accounting, not attribution from process names.

| Responsibility | CAS/s upstream / branch | GET KiB/s upstream / branch | SET KiB/s upstream / branch |
| --- | ---: | ---: | ---: |
| durable catalog projection outputs | 53.170 / 74.985 | 0.897 / 0.220 | 33.310 / 48.248 |
| catalog journal | 1.326 / 2.086 | 0.217 / 250.656 | 0.729 / 144.114 |
| replica frontier observations | 0.000 / 6.857 | 0.000 / 33.154 | 0.000 / 46.236 |
| cooperative storage observations | 0.000 / 2.712 | 0.000 / 8.573 | 0.000 / 20.458 |
| ready-made / legacy frontier relations | 3.354 / 2.171 | 15.241 / 0.112 | 45.146 / 1.130 |
| other builtin introspection | 23.072 / 27.455 | 0.640 / 1.334 | 13.779 / 15.024 |
| table transaction WAL | 2.052 / 2.179 | 0.000 / 0.000 | 0.920 / 0.988 |
| expression / plan cache | 0.003 / 0.028 | 0.000 / 0.000 | 0.000 / 0.000 |
| user collections | 0.038 / 0.072 | 0.000 / 0.000 | 0.000 / 0.000 |

Zero-byte blob liveness checks are outside this table. The JSON retains them.
CAS attempts include mismatches and a few retryable backend errors, not just
successful writes. Full per-process call/byte and DELETE accounting is in the JSON.

### Catalog completeness and recovery protection

Both inventories contain **51 durable catalog projection outputs**, with **50
advancing during the idle windows**. The AWS PrivateLink connections output does
not advance in either fixture. Their empty committed advances rise from **50.04 to 69.13/s**. Total output CAS rises from
53.17 to 74.98/s, accounting for 61% of the deployment CAS increase. Output
progress writes advance each output's own shard, not the raw catalog shard.

Exact upper joins connect 52.45/s of branch output advances to adapter empty
catalog advances, 16.64/s to replica nonempty publications, and 0.04/s to adapter
nonempty publications. These links establish propagation, not removable work.
Output scheduling and coalescing determine how many catalog advances propagate.

The feedback path is concrete: catalog completeness advances an MV output,
which permits advancing its durable recovery requirement, which writes the
catalog again and advances downstream completeness. Upstream advances the raw
catalog about once per second too. It does not persist the distributed protection
records in that journal. This is not evidence that each query independently
writes all outputs or that a second oracle read causes the idle traffic.

Primary branch acknowledged packed catalog rows:

| Record category | Rows/s | Packed KiB/s |
| --- | ---: | ---: |
| Compaction permission | 64.54 | 9.74 |
| Maintained-output recovery | 17.01 | 3.10 |
| Other, including client requirements | 102.60 | 17.44 |

These are packed row bytes, not compressed blob bytes. Retraction and insertion
both count. Client requirements are aggregate execution-owned protection, not
one durable object per SQL session. They advance while idle through timeline
windows, replica input progress and builtin logging/index retention.

A separate 120s DEBUG diagnostic covered 19,586 of 19,588 committed rows through
validated prefix ranges. All covered rows were ClientReadRequirement,
MaintainedReadRequirement or CollectionCompactionBound. It solved individual kind
counts for 57 of 60 commits without summing duplicate follower observations.
The uncovered two-row commit is the storage-usage **ID allocator bump**, verified
at `coord/message_handler.rs::storage_usage_update`, not a read grant. Thus the
large residual bucket is overwhelmingly client requirements in that diagnostic,
not audit-log growth or a per-second controller status write. It does not prove
an exact grant byte share in the earlier idle windows. The snapshot identifies
one adapter and four replica protection incarnations. Which local token wins
each aggregate minimum is not measured.

### Why catalog bytes grow much more than catalog CAS

The raw journal makes only 2.09 CAS attempts/s in the branch, versus 1.33 upstream.
Its branch GET bytes split into **113.0 KiB/s incremental follower row fetches,
42.8 SQL source fetches, 93.1 compaction and 1.8 GC**. SET bytes split into
**24.6 direct uploads, 116.9 compaction and 2.6 rollups**. Catalog work explains
91% of extra GET bytes and 79% of extra SET bytes in these windows.

The same changes are consumed by independent process-local followers and SQL
catalog projections. Compaction rewrites mixed historical state. It is incorrect
to allocate those rewritten bytes proportionally to new grant bytes, or promise
that removing 25 KiB/s of uploads removes 117 KiB/s of compaction output.

### Frontiers and other introspection

Native frontier observations replace centrally authored ready-made rows with
replica-owned reports, cooperative storage sampling and authority-validated SQL
reductions. That repair costs work even after sharing the two input arrangements.
Replica observations commit 4.66 data appends/s and storage observations 1.21/s.
Baseline's two ready-made relations each commit about one data append/s.

Publishers already coalesce reports, batch deltas and skip equal rows. Replica
publishers open writers, not subscriptions to peer observations. Their shared
Persist writers can compact peer batches. The storage sampler really does retain
and incrementally consume peer rows before conditional replacement. Reverting it
to whole-shard desired-state ownership would destroy peer observations.

The two observation sources also advance empty progress about once per second.
The two legacy raw frontier sources remain registered and contribute about two
more empty advances/s. Removing fallback maintenance requires preserving the
legacy/direct-read contract, not merely deleting a timer. Other builtin sources
account for another 4.38 CAS/s of net increase, including added/deployment-qualified
introspection and source-statistics partition work. Renamed raw relations must
not be counted as wholly new capabilities. This pass changes none of them.

## CPU attribution

CPU profiles are 60s post-window captures. Stack fractions are not exact
per-component idle core totals. Symbolization checks the actual ELF build IDs.
Unknown callers and 128-frame truncation remain visible.

- **Catalog query execution:** 71% of branch catalog-replica samples occur under
  compute worker execution, versus 84% upstream. Root-dataflow scheduling totals
  increase from 0.159 to 0.286 elapsed s/s. The four native frontier arrangements/
  reductions account for 0.063, versus 0.007 for upstream's two direct indexes.
  That difference is 44% of the root-scheduling increase. Status/hydration and
  object-dependency dataflows also grow. Scheduling elapsed time is not OS CPU
  and cannot be added to profile sample fractions.
- **Repeated immutable plan decoding:** 5.3% of catalog-replica samples occur in
  written-plan reads or plan deserialization. Most deserialization stacks hit the
  depth limit, so their exact caller is unknown. A complete stack reaches
  `ExpressionCache::read_plans`. Source inspection finds storage metadata
  resolution repeatedly reading selected plans to obtain `value_desc`.
  Cache hits avoid backend refresh but still deserialize full plans. This is
  not repeated SQL replanning or repeated whole-shard backend snapshots.
- **Distributed protection and catalog following:** non-catalog replicas spend
  16–21% of their samples applying catalog changes, plus protection, observation,
  Persist and service work absent from those upstream replica processes.
  Controller samples are 45% catalog catch-up and 28% other controller work.
  Its measured total is only 0.025 cores, so provisioning is not the main problem.
- **Maintenance:** adapter samples include 23% Persist GC, versus 5% in upstream
  environmentd. Upstream has more sampled compaction in that process. The I/O
  ledger identifies journal maintenance, but does not turn these process-level
  CPU fractions into per-shard CPU measurements. Relay CPU is measured separately,
  but it exposes no CPU/heap profiling route in this harness.

## Memory attribution

Valid heaps were captured at approximately the same fixture age, **before** CPU
profiling, which pauses allocation sampling. First-run native heaps are excluded.
The native repeat is the heap comparison, not a replacement for the first CPU run.

| Measured memory | Upstream MiB | Branch repeat MiB |
| --- | ---: | ---: |
| PSS | 1354 | 2927 |
| Anonymous PSS | 839 | 2021 |
| File-backed PSS | 515 | 906 |
| jemalloc allocated | 535 | 1183 |
| jemalloc resident | 733 | 1697 |
| Encoded blob-cache gauge | 51 | 363 |
| Sampled live heap, excluding standalone relay | 466 | 1118 |

These rows overlap and must not be summed. `jemalloc_retained` is virtual
retention, not RAM. The extra file-backed residency is material in these debug
builds. Sampled heap does not explain every resident page, thread stack or arena.

The catalog replica's sampled heap grows from 311 to 533 MiB. Classified dataflow/
arrangement allocations grow from 247 to 296 MiB. Native heaps also contain
encoded batch buffers and file-read buffers. Non-catalog replicas grow from
9–12 to 116–141 MiB each, including 22–29 MiB of catalog/SQL allocations,
28–37 MiB of encoded Persist batches and 39–60 MiB of detached async file reads.
Those detached stacks do not identify which cache retains the buffer.

Source inspection establishes the architectural duplication without guessing
buffer ownership. Each follower calls `Catalog::open_committed`, reconstructs an
environment-wide SQL catalog, and unconditionally opens an expression store.
`DurableCache` retains serialized entries from the whole expression shard, not
only that replica's selections. Written plans are decoded on demand. Selected
execution plans are process-local. Controllerd also keeps a full catalog and raw
expression store, although its decoded startup plan maps are dropped.

This is not a steady-state full reconstruction on every tick. Followers apply
incremental commits and reuse unchanged catalog items. The duplication is in
retained representations and cache ownership, plus some repeated decoding.

## Consensus scans: measured gap, bounded attribution

The branch makes 7.58 scans/s versus 2.94 upstream, with a native repeat at 6.42.
The catalog replica contributes 5.24 versus 1.69/s, or 77% of the primary net gap.
These scan Persist metadata, not table WAL rows or timestamp-oracle storage.
The measured WAL CAS difference is only 0.13/s and has negligible idle blob cost.
Oracle calls are separate: write/apply rates are about 1.57/1.54 versus 1.00/1.00/s.

The [scan-origin audit](20260903_decoupled_coordination_replica_scans.md) directly
established reader/upper fallback duplication, MV metadata-CAS recovery, rollup
refresh/history and GC. Multiple empty scans can use the same cached sequence
number. PubSub normally updates local state but does not suppress all fallback
scans. An oracle timestamp cannot replace missing catalog contents.

**This pass does not supply a fresh exact caller split of the remaining scans.**
The earlier scan audit used different settings and candidate code. Its per-caller
rates must not be pasted into this comparison. Source paths still identify
candidates, and extra output CAS/maintenance supplies a causal explanation for
part of the gap. Current counters alone do not quantify that part. Persist-internal
coalescing and the pre-rollup refresh repair remain outside the requested scope.

## Suggested next scopes

1. **Cache immutable producer metadata at the caller.** Reuse schema/time-dependence
   facts keyed by selected revision rather than repeatedly decoding complete
   plans. Keep selection/version checks, cross-cluster producers and pending
   misses. Measure decoding samples and total CPU. This is the narrowest proven
   redundant work, but it will not materially fix CAS or blob traffic.
2. **Exclude irrelevant protection rows from shared SQL catalog input.** Audit
   every consumer of `mz_catalog_shared` before filtering ClientReadRequirement,
   MaintainedReadRequirement and CollectionCompactionBound before its arrangement.
   Keep identity/authority rows and retained-metrics isolation. This targets SQL
   row processing, not follower correctness or journal bytes. Empty progress still
   propagates, so no CAS saving is promised from a row filter alone.
3. **Bound catalog-output completeness fanout without slowing protection.** The
   19 extra committed output advances/s are the strongest write-cost target.
   Determine which advances can be coalesced within the existing completeness
   contract while data updates remain prompt. Any new latency budget needs an
   explicit decision. Do not freeze certification, retimestamp permissions with
   REFRESH, change publication cadence, or elect an output-maintenance owner.
4. **Reduce non-execution catalog/cache ownership.** Start by avoiding controller
   plan-store ownership and unnecessary startup cache decoding where lifecycle
   validation allows it. Lazily open follower stores on actual plan demand.
   Disabling the best-effort cache flag alone is insufficient because fallback
   paths reopen the store. A lightweight catalog projection or selected-revision
   store is a larger boundary change, not a quick cache-limit adjustment.

Required admission grants cannot be delayed. Maintained recovery requirements and
compaction permissions are different obligations, even when a publication advances
both. Existing publication equality checks and batching are not missing. A query
WAL-reader reuse cleanup remains useful for startup/ownership, but is not the
largest measured idle-cost opportunity. None of these savings is benchmarked yet.

## Evidence and verification

Evidence root: `~/.local/share/aj/m3-evidence/logical-component-load/`.

- Primary runs: `native-1794209699`, `main-1794211447`.
  Native repeat and valid heaps: `native-heap-1794213327`.
- `provenance.json` records hashes/build IDs and exact production-source
  equivalence to retained instrumented binaries. Diagnostic patches are not
  applied to production sources.
- Each run has `analysis.json`, reconciled `backend-analysis.json`,
  `logical-backend.json`, `derived-attribution.json`, and symbolized profiles.
  `publication-coverage.json` retains accepted/rejected prefix ranges rather than
  hiding incomplete coverage. Source audits are `catalog-producers.md`,
  `frontier-components.md` and `catalog-memory-source.md` in the evidence root.
- The baseline's post-measurement inventory initially used a native-only relation
  and failed. The exact SQL failure was reproduced. The inventory then passed
  against the common raw relation on restart. Original cost windows and profiles
  passed and remain unchanged. The failed overall run status is preserved.
- Reducer tests, Python Black/Ruff and diff whitespace checks pass. Full repository
  formatting previously failed at an unchanged generic call in
  `src/catalog/src/catalog.rs`. Broad CI remains non-green and was not rerun.
  This does not establish release-build costs, cloud billing, multi-adapter scaling,
  arbitrary-version compatibility or latency under user load.

The harness now captures heaps before CPU profiles and records profile timing.
Optional publication diagnostics run after measurement and restore the prior
logging setting even when a scrape fails. Reproduction tools are
`misc/benchmarks/native-idle/analyze_components.py`,
`analyze_profile_components.py` and `analyze_publications.py`, layered on the
existing resource/backend/profile reducers. All owned runtimes are stopped. The three stopped fixture metadata containers
were removed, with about 150 GiB free disk remaining.

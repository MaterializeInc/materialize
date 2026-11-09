# One-minute read-protection publication

**A minute cuts journal churn and catalog-output writes. It does not make native
idle overhead disappear.** In the primary comparison CPU is about 86% above upstream and PSS more than
doubles. Blob traffic remains higher. Scan rates depend strongly on notification
delivery and are not a stable branch-minus-upstream difference. The publication interval
controls one source of work, not frontier observations, SQL reductions, metadata
wait recovery, or the footprint of additional catalog-owning processes.

## Matched comparison

| Metric | Pinned upstream | Native 10s | Native 60s |
|---|---:|---:|---:|
| Materialize CPU, cores | 0.635 | 1.237 | 1.181 |
| PSS, MiB (smaps) | 1354 | 2896 | 2848 |
| Consensus CAS/s | 83.01 | 119.50 | 99.36 |
| Consensus scans/s | 2.94 | 7.00 | 6.64 |
| Blob GET/s | 2.14 | 10.02 | 7.42 |
| Blob GET KiB/s | 16.99 | 306.95 | 90.24 |
| Blob SET/s | 4.55 | 9.61 | 8.45 |
| Blob SET KiB/s | 93.88 | 282.31 | 151.60 |

Native has two 10s fixtures and three fresh 60s fixtures. Each contributes two
five-minute windows after five minutes of warmup. The upstream reference has one
fixture. All three new fixtures passed. The third adds post-window CPU profiles,
not profiling during the resource windows. None of the high-CPU windows was
excluded. Native 60s fixture means range from 1.084 to 1.272 cores, so the small
CPU reduction from 10s is not a demonstrated stable improvement.

Both candidates are Rust 1.98.1 development builds with assertions, PostgreSQL 18
SERIALIZABLE consensus, file blobs, one SQL frontend and four replicas. Metric
sinks, incremental compaction and columnar accumulable diffs are disabled. The
upstream shared-MV batching limit is zero to match native. Effective native
settings and cluster inventories match except for the interval. SQL reports
`1 min`, and every replica's actual launch configuration carries that value.
Diagnostic options and capture timing are retained with each run.

Native production source is equivalent to `a0d87d072a`, current branch
`5d0d6a9acd` adds diagnostics and documentation. Upstream is
`77ae0a3034efefb22919b6257dd40e5ebc1827bb`. Retained primary binaries were verified
by SHA256 and ELF build ID before reuse. The primary binaries carry the same
backend audit instrumentation used in the
[previous comparison](20260903_decoupled_coordination_logical_load.md).

## What the interval changes

The reference inventories both contain 51 catalog MV output shards, with 50
advancing during measurement. AWS PrivateLink connections is the non-advancing
output in both. Native adds adapters, adapter endpoints, adapter statuses and
deployment authority as MVs. Columns, index columns, history-retention strategies
and MV refresh strategies are tables instead of upstream's MVs. There is no net
increase of five MVs explaining the extra output CAS.

Packed catalog updates fall from about 188 to 33 per second, close to the
sixfold interval change. Durable projection CAS falls from 75.9 to 58.5/s.
That accounts for about 87% of the total CAS reduction. The exact-upper audit
links most disappearing output progress to replica protection publications.
Adapter-driven empty output progress stays near 51/s, versus 50/s upstream.

Each projection writes its own output shard. These are mostly empty completeness
advances, not result-row changes or writes back into the catalog journal. A
protection publication advances the shared input upper and can make many such
outputs advance. Coalescing means one input publication does not imply exactly
one append per output.

The setting delays advancement/release of protection. It does **not** delay
admission of a stronger new grant, authorization checks or catalog following.
The timer is jittered between half and 1.5 times the configured interval, before
scheduling and contention delays. This is not a lease expiry.

## Why the residual I/O remains

The completed-operation ledger attributes every audited CAS and blob event once.
It reconciles with counter deltas using the recorded scrape-boundary bounds.
CAS here includes attempts and maintenance, not only committed data appends.
Small differences from the headline counter rates are boundary effects.

| Component | CAS/s main | CAS/s 60s | GET KiB/s main | GET KiB/s 60s | SET KiB/s main | SET KiB/s 60s |
|---|---:|---:|---:|---:|---:|---:|
| catalog journal | 1.33 | 1.40 | 0.22 | 45.16 | 0.73 | 30.43 |
| durable catalog projection outputs | 53.17 | 58.47 | 0.90 | 3.68 | 33.31 | 39.03 |
| replica frontier observations | 0.00 | 6.71 | 0.00 | 32.15 | 0.00 | 46.14 |
| cooperative storage observations | 0.00 | 2.50 | 0.00 | 8.24 | 0.00 | 18.93 |
| ready-made / legacy frontier relations | 3.35 | 2.14 | 15.24 | 0.04 | 45.15 | 1.13 |
| other builtin introspection | 23.07 | 25.96 | 0.64 | 0.97 | 13.78 | 14.96 |
| table transaction WAL | 2.05 | 2.10 | 0.00 | 0.00 | 0.92 | 0.99 |
| expression / plan cache | 0.00 | 0.03 | 0.00 | 0.00 | 0.00 | 0.00 |
| user collections | 0.04 | 0.06 | 0.00 | 0.00 | 0.00 | 0.00 |

- **Frontiers have their own publication cadence.** Replicas publish observations
  and the adapter publishes cooperative storage observations around once per
  second. The 60s protection setting does not change that. Native stores execution
  identity and known/read/write progress, then validates and reduces those facts
  into public views. Upstream publishes ready-made relations centrally. Four
  independent replica producers also make more small batches. This accounts for
  about 8 net additional CAS/s and substantial encoding, reading and compaction.
  Both captured replica-frontier inventories contain 132 log-index rows. The gap
  is not explained by inventing a new set of logging observations.
- **Legacy progress is still coupled in.** Native advances the legacy frontier
  relations even though protected-mode result rows come from the new reductions.
  The public SQL retains dynamically guarded legacy branches. Stopping those
  writers alone can hold back output progress. Removing this cost requires
  removing inactive input dependencies too.
  The source-statistics shard has two independently clocked writer partitions,
  each capable of empty advancement. Native also registers the storage-status
  writer. The latter two account for about 2 extra empty CAS/s in other
  introspection. The remainder there is reader/since lifecycle and maintenance,
  not an unexplained burst of changed rows.
- **Protection traffic is smaller, not absent.** Its journal is consumed by
  incremental followers and the SQL input, and compacted. Those reads and rewrites
  remain even when publication happens once a minute. Projection maintenance also
  varies: one 60s fixture has 10.7 KiB/s of projection rollup reads, while another
  has only 0.33 KiB/s of projection GC reads. These are not SQL-source refetches.

Replica observation publishers are writer-only. Their reads of peer batches are
compaction work, not subscriptions to peers. The cooperative storage publisher
also consumes shared observed state.

### Consensus scans

**The total branch-minus-upstream scan gap is not stable.** The primary figures
are 6.64 versus 2.94/s. A fresh configuration-matched diagnostic pair measured
native at 7.45 and upstream at 9.03/s. These runs use scan tracing rather than the
primary backend audit. Do not pool their resource windows with the primary ones.
Every process/window reconciles completed scan records with its metrics. No scan
failure or unclassified caller was observed in these windows.

| Actual scan caller/mechanism | Main /s | Native 60s /s |
|---|---:|---:|
| append upper recovery | 0.000 | 0.181 |
| command CAS recovery | 0.018 | 0.718 |
| explicit upper | 0.000 | 0.230 |
| live-state maintenance | 0.423 | 0.456 |
| machine acquisition | 0.093 | 0.100 |
| physical since | 0.000 | 0.010 |
| reader fallback | 0.967 | 1.999 |
| rollup | 0.649 | 0.794 |
| rollup state refresh | 0.649 | 0.792 |
| upper-only fallback | 0.000 | 0.877 |
| writer-upper fallback | 6.234 | 1.289 |

The fresh upstream run has 6.01/s of writer-upper fallback scans in environmentd.
Of 3,611 such scans, 3,384 found newer state rather than returning empty. Its
PubSub server dropped 3,407 diffs to full subscriber queues during the windows,
versus 324 in the primary upstream fixture. This is recovery of late/lost cached
state, not a catalog linearizability requirement. The aggregate drop counter
cannot match individual drops to scans. Native's separate relay dropped 133
messages, or 0.22/s, in the fresh diagnostic.

The mechanism is concrete: `rpc.rs` forwards with `try_send` and discards a diff
when the subscriber queue is full. The code defaults both client-receiver and
server-connection queues to 25 entries. The embedded same-process connection
uses the client-receiver queue. Notification delivery, burst timing and fallback
polling therefore affect scan rates even with identical logical SQL workloads.
A blanket claim of a fixed 3.7 extra native scans/s is not supported.

That does **not** erase native-specific work. The catalog replica has 3.14 more
scans/s in the fresh pair: about 1.01 extra reader fallback, 0.78 metadata/progress
watches, 0.61 command-CAS recovery, 0.39 writer-upper fallback, 0.26 rollup work,
and smaller remaining paths. Catalog-progress observers across the deployment
account for only 0.108/s. Storage-metadata watches account for 0.769/s. The
reader/input identity audit maps those calls to the following responsibilities:

- Native's catalog replica has 124 scan-bearing reader IDs across 80 shards,
  versus 104 across 76 upstream. This is not a full reader census. Several readers
  of one shard have different jobs: MV output correction is not a duplicate of
  downstream indexing. Audit events has six scan-bearing reader IDs in both runs.
- New authority, raw-catalog and subscription inputs account for 0.223 reader
  scans/s. Authority has 14 downstream imports plus its output-correction reader.
  Index-columns and object-global-IDs also have repeated downstream imports.
  Arrangement reuse is a concrete candidate, with retention boundaries preserved.
  The actual observation shards have **zero** catalog-replica reader/upper-only
  fallback scans in these windows. Public frontier indexes reuse the shared
  validated inputs rather than independently reimporting observations.
- Storage metadata retains 73 distinct watches, with no restarts or new watches
  during measurement. Eighteen logical IDs share one WAL watch. All 90 sampled
  logical IDs have read policies and committed bounds. Eighty-nine advance between
  every sample, and the last is complete. There is no large measured set of
  policy-less, permanently quiet metadata watches to remove. They support
  bootstrap, scheduling and compaction-policy progress independently of the
  publication timer.
- Four quiet physical relations contribute exactly 140 reader scans in each run.
  Logical WAL progress can advance while a transactional data shard stays quiet.
  Native reader scans are empty 1,066/1,172 times, upstream 556/563. The remaining
  reader-rate difference cannot be assigned solely to reader count: notification
  timing, cache updates and backoff also matter. Raw catalog's overlapping
  progress/metadata observers are a narrow reuse candidate, not guaranteed
  savings or permission to stop required scheduling observations.

Ordinary listen and upper-watch retries are recovery polling, often on quiet
inputs. Shared observation writers also introduce real CAS/upper contention.
Neither should be described as an extra linearizable catalog read per timer
turn. Existing counters independently reconcile the primary runs at the
mechanism level, but cannot recover their exact historical callers. In particular,
sleep wakes are not scans and rollup `noop(truncated)` includes successful writes.

The fresh diagnostic still shows a CPU gap, 1.041 versus 0.703 cores, and CAS and
blob gaps close to the primary comparison. Its smaller CPU gap reinforces the
need to report variability rather than treat the primary 86% as a fixed tax.

## Where CPU remains

| Process | CPU main | CPU 60s | PSS MiB main | PSS MiB 60s |
|---|---:|---:|---:|---:|
| SQL frontend | 0.273 | 0.251 | 586 | 563 |
| controllerd | included in envd | 0.018 | included in envd | 394 |
| pubsubd | included in envd | 0.031 | included in envd | 37 |
| System replica | 0.015 | 0.060 | 93 | 286 |
| Catalog-server replica | 0.316 | 0.694 | 481 | 966 |
| Probe replica | 0.015 | 0.063 | 99 | 295 |
| Quickstart replica | 0.016 | 0.062 | 94 | 308 |

The catalog replica accounts for 69% of the remaining CPU delta. The other three
replicas account for 26%. SQL frontend plus controller and relay account for the
remaining 5%. This process-level accounting is from the measured windows.

The post-window profile identifies the work, but is not a simultaneous
per-component decomposition of those CPU windows:

- Catalog replica: 71.6% of samples are compute-worker execution. The four frontier
  roots consume 0.0634 scheduling seconds per elapsed second, versus 0.00692 for
  upstream's two direct frontier indexes. They explain about 72% of the increase in
  measured root scheduling time. Source, sink and hydration status reductions
  also cost more. Shared inputs have removed duplicate observation readers, not
  the validation, joins, reductions and progress propagation themselves.
- The catalog profile also has 1.7% in known written-plan cache decoding and 4.6%
  in plan deserialization whose caller is truncated/unclassified. Schema
  reconciliation reads an immutable selected written plan repeatedly. Retaining
  that schema/decoded plan is a concrete improvement on our side. Do not assign
  all unclassified decoding to that caller without deeper stacks.
- A non-catalog system replica has 363 samples versus upstream's 70 in equal-length
  captures. Parquet encoding appears in 81 samples, and compaction is another
  prominent category. The backend ledger places its steady writes and peer reads
  in observation publication and maintenance. This work is independent of the
  slower protection timer.

Timely's `Subgraph::schedule` surrounds operator execution. Its presence in a
stack is not proof that the sample is progress-accounting overhead. Named root
scheduling measurements and the profile are separate, complementary evidence.

## Memory is not just publication traffic

The saved smaps snapshots put native 60s at about 1,865 MiB anonymous and 984 MiB
file-backed PSS, versus 839 and 515 MiB upstream. These categories reconcile the
memory total. File-backed PSS does not identify individual mappings.

Live jemalloc allocation is about 1,064 MiB versus 535 MiB upstream. Its resident
statistic is about 1,592 versus 733 MiB. These gauges overlap with PSS and cache
bytes and are sampled separately. They must not be summed as independent costs.
The fixed footprint includes additional processes, full catalog/expression state
in each native follower, their caches, allocator residency and mapped code/data.
It cannot be attributed wholly to retained catalog objects: a valid controller
heap profile has only about 54 MiB of sampled live allocation despite much larger
PSS. A narrower follower catalog API remains a useful architectural opportunity,
not a claim that it would remove the entire gap.

Going from 10s to 60s lowers total Persist blob-cache bytes from about 347 to
159 MiB. It does not eliminate compute history. The catalog replica's PSS rises
from about 874 to 966 MiB. Of that increase, 77 MiB is file-backed and only
14 MiB anonymous, so PSS growth is not evidence for a retention explanation.
Separately, two valid 60s heaps show 329–385 MiB under dataflow/trace
allocation paths versus 296 MiB in the valid 10s heap. This is consistent with
slower release retaining more history, but does not identify the responsible
arrangements. Batch bytes referenced by current raw-catalog shard state shrink
rather than grow. That metric is not blob-cache occupancy.

PSS in this report uses `smaps_rollup` so its anonymous/file decomposition is
consistent. The accompanying JSON also retains the older psutil readings. Those
are adjacent, not atomic: one process differs by 110 MiB between the two reads,
changing the native mean by about 18 MiB. That does not change the conclusion.

## Proposed next steps

1. Examine native-specific frontier view construction to remove inactive legacy
   inputs and then their writers. Do not just stop the writers: the current
   dynamic branches still depend on their progress. Separately address redundant
   empty advancement across source-statistics partitions, preserving pending data
   updates and completeness. These are candidates, not verified fixes.
2. Retain immutable selected-plan/schema results instead of decoding them on
   unchanged reconciliation turns. Measure with the same development fixture.
3. Use the fresh scan caller ledger to choose a usage-side repair. Do not infer
   that every refresh is necessary for linearizability, or change Persist from a
   global retry counter alone.
4. If a near-baseline footprint is required, examine frontier payload/batch costs,
   reduction plans and the full-catalog dependency of controller and replica
   followers. These costs need a separate design decision. Increasing the
   protection interval again does not address them.

No production default or Persist behavior changed. This experiment does not
establish write-heavy retention, query latency, restart replay or upgrade
readiness at 60s. CI remains non-green/not rerun.

## Evidence and checks

Portable figures and per-window/per-process values are in
[the accompanying JSON](20260903_decoupled_coordination_publication_interval.json).
Raw evidence is under
`~/.local/share/aj/m3-evidence/publication-interval/`: `provenance.json`,
`comparison.json`, `compare.py`, `memory-residual.json`, `scheduling-residual.json`,
`cpu-residual.py`, `scan-summary.json`, `reader-scan-residual.md/json`, the three
`native-60s-*` fixtures, and `scans/`.
The previous reference fixtures and heap audit remain in
`~/.local/share/aj/m3-evidence/logical-component-load/`.

The old upstream run's measurements passed, but its post-window native-only
inventory query failed. That failure was reproduced and the inventory repaired
without rewriting its measurements. The invalid first 10s heap remains excluded.
Independent review found no blockers for the measured comparison. Its duration
normalization and memory-wording findings were corrected. Targeted diagnostic
tests, Black, Ruff and `git diff --check` pass. All owned
runtimes stopped. The five stopped metadata containers and their anonymous
volumes were removed. About 135 GiB remains free. Temporary scan instrumentation
was reversed before the diagnostic runs, after retaining its binaries and patch.
Production Rust has no diff.

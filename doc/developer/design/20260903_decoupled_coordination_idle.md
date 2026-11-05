# Native coordination: idle resource comparison

The fresh native deployment uses about **2.1× CPU and 2.2× proportional resident
memory** versus upstream main in this development-build fixture. The catalog
replica accounts for roughly 62% of the extra CPU. Controllerd itself costs only
about 0.026 cores, but adds 411 MiB PSS.

The clearest avoidable backend cost is repeated metadata polling: about
227 consensus scans/s versus 5/s upstream. These are predominantly empty,
incremental refreshes, not repeated full-catalog snapshots. The 10-second
read-protection publication setting does not slow the separate one-second
observation and reconciliation loops.

## What was compared

- Native source `324726428`, version `26.46.0-dev.0`, with one adapterd,
  controllerd, pubsubd and four clusterd processes.
- Upstream main `77ae0a3034`, version `26.50.0-dev.0`, with environmentd and
  four clusterd processes. PubSub is embedded in environmentd.
- Same Rust 1.98.1 development profiles and target flags, soft assertions on,
  info logging, one worker per replica, 4 GiB replica allocations and no replica
  CPU quota. Native frontend/relay quotas permit all 16 host CPUs. No observed
  CPU throttling. At least 13.3 GiB host memory remained available in measured
  windows. Builds and environments ran sequentially.
- A private PostgreSQL 18 container per fixture, 2 GiB limit, SERIALIZABLE
  Persist transactions, file blobs and new metadata. No shared development
  database was reset. This is not a CockroachDB or remote object-store benchmark.
- `mz_system`, `mz_catalog_server`, `mz_probe` and `quickstart` each have one
  replica. The workload is a one-row table with a default index, then no user
  traffic during measurement. SQL succeeds before and after the idle windows.
- Native `SHOW catalog_read_protection_publish_interval` returns **10 s**.
  Replica publication jitters around that target and only writes when needed.
  Both builds expose full shard metrics via `persist_shard_metrics=both`.
- Five minutes of warmup after readiness/setup, then two adjacent five-minute
  windows. Process samples are every two seconds. CPU and heap profiles run
  afterward. The extended native control adds later windows to inspect RAM drift.

This compares two implementations, not an isolated patch. Builtin inventories,
optimizer/rendering code and some Persist implementations differ. Fresh idle
history is not the long-run equilibrium of 30-day retained telemetry.

## Default-build results

CPU is cores, where 1.0 is one fully used CPU. CPU and RSS are means across the
first two windows. PSS is the endpoint of the second window, approximately
15 minutes after setup. MiB means 2^20 bytes. PSS apportions shared pages and is
suitable for totals. RSS is not summed across processes. RSS uses sampled kernel counters, which can
be approximate. PSS comes from `smaps` at the boundary, not the same instant as
the mean RSS.

| Process | Main CPU | Native CPU | Main RSS MiB | Native RSS MiB | Main PSS MiB | Native PSS MiB |
|---|---:|---:|---:|---:|---:|---:|
| Frontend | 0.263 | 0.292 | 545.9 | 571.8 | 560.6 | 589.8 |
| Controller | n/a | 0.027 | n/a | 394.4 | n/a | 410.8 |
| Relay | n/a | 0.028 | n/a | 35.9 | n/a | 37.5 |
| System replica | 0.018 | 0.100 | 217.0 | 522.1 | 94.8 | 348.9 |
| Catalog replica | 0.311 | 0.735 | 602.4 | 1059.3 | 494.9 | 902.3 |
| Probe replica | 0.017 | 0.060 | 222.5 | 513.1 | 102.5 | 348.7 |
| Quickstart replica | 0.018 | 0.067 | 223.4 | 517.8 | 98.5 | 371.5 |
| Total Materialize | 0.628 | 1.309 | not summed | not summed | 1351.4 | 3009.6 |

PostgreSQL additionally consumes 0.017 cores on main and
0.045 on native. Its resource usage is separate from the
Materialize totals.

## Persist activity

These are external API starts/s and payload KiB/s, averaged over both windows.
They exclude Persist memory-cache hits, but file reads can still hit the OS page
cache. They are not disk IOPS, network wire bytes or SQL transaction counts.

| Operation | Main calls/s | Native calls/s | Main payload KiB/s | Native payload KiB/s |
|---|---:|---:|---:|---:|
| consensus_cas | 82.98 | 120.02 | 48.2 | 79.8 |
| consensus_scan | 5.05 | 228.05 | 125.5 | 243.3 |
| consensus_head | 0.08 | 0.10 | 0.0 | 0.0 |
| consensus_truncate | 0.65 | 0.90 | 0.0 | 0.0 |
| blob_get | 2.15 | 14.26 | 18.2 | 321.0 |
| blob_set | 4.55 | 9.72 | 95.0 | 290.0 |
| blob_delete | 4.15 | 9.35 | 80.3 | 258.9 |

CAS success includes an expectation mismatch. CAS bytes count committed payload.
Blob DELETE bytes are the size deleted, **not transferred bytes**. HEAD and
truncate have no useful payload-byte total here. Timestamp-oracle operations are
recorded separately, not folded into Persist counters.

## Attribution

### Most metadata reads are redundant fresh-upper polls

In the second native window, `fetch_upper` is 222.31/s, close to 227.10 scans/s.
Adapterd contributes 131.02 scans/s and the catalog replica 79.84/s. Their empty
state-refresh rates are 129.81/s and 76.38/s. Payload grows only 2.27× despite the
roughly 42× scan-call increase.

- The adapter sampler traverses its inventory every second and fetches an upper
  before checking whether the observation changed. The captured inventory has
  106 collections, including 28 transactional collections. It reads their shared
  WAL 28 times per round instead of once: approximately **27 avoidable calls per
  round**, without any change to freshness or publication frequency.
- The catalog follower resolves wanted metadata on each execution-owned round,
  even without structural catalog changes. Its footprint implies 73 distinct
  upper shards per round. At the observed reconciliation rate this predicts
  about 69 upper reads/s, explaining most of its measured 76.28/s `fetch_upper`.
- Empty replicas still refresh the catalog and synchronize the expression cache
  twice, including empty plan requests. Their roughly three upper reads/s match
  that structure. Quickstart adds its one table's WAL upper.
- Controllerd refreshes five times per tick. Its 301 ticks account for exactly
  1,505 `fetch_upper` calls in the second window.

`recent_upper` provides write-linearized freshness and scans after the cached
sequence number. Do not weaken that API globally. The adapter has no metadata
state initialization in this window, and the catalog replica has 19 against
23,979 scans. Neither records a slow-path state-refresh fallback.

Code: `src/storage-controller/src/frontier_observations.rs:205–240`,
`src/clusterd/src/catalog_follower.rs:505–550`,
`src/clusterd/src/catalog_follower/storage_metadata.rs:203–232`,
`src/persist-client/src/internal/state_versions.rs:674`.

### CPU is primarily replica work, not controllerd

The catalog replica grows from about 0.31 to 0.74 cores. Its native CPU profile
places roughly 70% of samples under the compute worker and 6% under the catalog
follower. Persist decoding/maintenance also contributes. These are overlapping
inclusive stacks from a separate 30-second window, not an additive accounting
of the CPU gap. Polling clearly explains backend requests, but is not proven to
be the largest CPU consumer.

Native system/probe/quickstart replicas also perform catalog following,
protection/frontier publication and Persist maintenance absent from upstream's
mostly idle command-driven replicas. Adapterd itself is much closer to
environmentd, about 0.29 versus 0.26 cores. The relay is included in native totals.

### Frontier reductions are the largest named compute hotspot

Separate, configuration-aligned diagnostics use the existing scheduling logs
with a five-minute warmup and two samples about 60 seconds apart. The address
join identifies one scheduled root operator per dataflow. Counters and root
identities remain stable. Synthetic address entries without scheduling counters
are excluded, not treated as zero execution time.

| Dataflow index/relation | Main scheduling ms/s | Native scheduling ms/s |
|---|---:|---:|
| `mz_frontiers_ind` | 2.9 | 52.8 |
| `mz_cluster_replica_frontiers_ind` | 3.1 | 34.7 |
| `mz_source_statuses_ind` | 7.3 | 16.9 |
| `mz_hydration_statuses_ind` | 7.0 | 12.9 |
| `mz_object_dependencies` | 1.8 | 11.1 |
| `mz_sink_statuses_ind` | 3.1 | 9.7 |

Across stable roots, total scheduling time is 145.4 ms/s on main and 318.5 ms/s
on native. The two frontier indices account for **81.5 of the 173.1 ms/s
increase, about 47%**. The native authority projection itself costs 6.7 ms/s.
These timings cover 136/138 stable roots, respectively. The difference is not
simply a much larger number of dataflows.

This locates a cost in the shared-observation reductions and their downstream
status views. It does not account for every extra process CPU cycle. Scheduling
logs measure elapsed time, not OS CPU, and exclude asynchronous Persist work
and some worker overhead. Their publication delay and the diagnostic queries
also limit precision. Do not sum nested operator timings or equate the 47%
figure with 47% of the entire deployment's CPU increase.

Keep replica-owned reporting and the small authority projection. The optimization
target is the incremental reduction work, not a return to adapter-owned whole
shard snapshots. Definitions are in
`src/catalog/src/builtin/mz_internal.rs:3328` and
`src/catalog/src/builtin/mz_catalog.rs:3092`.

### RAM includes caches, allocator residency and loaded code

At the final default-native boundary, blob caches contain 31 MiB in controllerd,
53 MiB in adapterd, 128 MiB in the catalog replica and 59–68 MiB in the other
replicas. Upstream's corresponding frontend/catalog caches hold 26/32 MiB, while
its other replicas hold about 0.1–0.2 MiB. Each cache has a 128 MiB capacity,
not a preallocated reservation.

Native's system replica reports 146 MiB jemalloc allocation and 220 MiB allocator
residency, versus 18/33 MiB upstream. Its PSS consists of 273 MiB anonymous and
75 MiB file-backed pages. Controllerd's 411 MiB PSS includes 236 MiB file-backed
pages. Thus neither a sum of RSS nor heap allocation alone describes the cost.
File-backed pages can be reclaimed under memory pressure.

Each native replica retains an environment-wide durable catalog, SQL-facing
catalog state and a mirror of the plan-store keys/opaque bytes. Bootstrap parses
and locally optimizes SQL definitions even though execution uses committed
physical plans. The system-replica heap profile attributes about 10% of sampled
live allocation to catalog reconstruction and 6% to the plan-store mirror.
This is real overhead, but does not explain the entire RAM gap.

Ordinary protection updates are incremental and do not reparse SQL. Major maps
use structural sharing. Remaining broad work includes repeated inventory scans,
producer-plan decoding, schema resolution and shard-alias scans on unchanged
reconciliation rounds.

Code: `src/clusterd/src/catalog_follower.rs:286–321`,
`src/catalog/src/catalog.rs:1574`, `src/catalog/src/catalog/state.rs:1528`,
`src/durable-cache/src/lib.rs:138–205`.

## Configuration controls and memory drift

Upstream defaults differ for metric sinks, incremental Persist compaction and
columnar accumulable diffs. A fresh main control turns all three off and sets
`compute_mv_sink_shared_batch_max_bytes=0`. It measures 0.594–0.597 cores and
1,344 MiB final PSS. Those differences do not explain native's higher resource
usage.

The aligned native control enables `enable_compute_temporal_bucketing=true`,
which upstream now enables unconditionally. The override is present in the
replica's launch configuration, not just adapter `SHOW`. Its first two windows
measure 1.338/1.340 cores and 3,032 MiB endpoint PSS, versus matched main's
0.597/0.594 cores and 1,344 MiB. Configurable default differences do not explain
the gap. Remaining implementation differences include time-major chunking,
Persist part decoding and the file-blob read implementation.

The extended native windows remain at 1.321/1.347 cores. PSS rises from 2,932 MiB
at the first endpoint to 3,282 MiB at the fourth. Across those endpoints,
jemalloc allocation rises from 1,176 to 1,532 MiB, while blob caches grow from
321 to 608 MiB. **About 81% of the live-allocation increase is cache growth.**
The configured cache capacities bound that component, but these windows do not
prove a long-run RSS/PSS plateau or rule out growth elsewhere.

Even the initial native windows are not flat in RAM: PSS rises from 2,927 to
3,010 MiB between endpoints. Report the age and cache/allocator trend rather
than interpreting this as either a proven leak or a long-run plateau.

## Prioritized next steps

1. **Deduplicate WAL upper reads within each sampler round.** Retain logical WAL
   progress, not physical table uppers. This is the smallest concrete reduction.
2. **Optimize the measured frontier reductions.** Inspect their plans and input
   changes, then remove redundant work or share existing arrangements where
   justified. Preserve authorized-execution filtering, public reductions and
   the distinction between missing and completed frontiers. Do not trade away
   freshness or add another durable projection without evidence that it is needed.
3. **Separate structural reconciliation from progress observation.** Cache
   immutable selected plan revisions and schemas, skip empty plan-store reads,
   and invalidate derived inventories on relevant committed updates. Preserve
   missing-entry retries, post-grant freshness, retirement checks and convergence.
4. **Make shared observations reduce sampling work, not only duplicate writes.**
   Reuse existing progress notifications with periodic catch-up. Keep fresh
   observations at the boundaries that actually require them. Do not introduce
   a designated adapter or a new coordination protocol.
5. **Budget caches and reduce allocation churn.** Smaller or role-specific cache
   budgets can save RAM but increase GET/decode work. Measure that tradeoff before
   changing defaults. Retaining an execution projection instead of rebuilding a
   full SQL catalog in every replica is a larger architectural followup.

Increasing the 10-second publication interval alone does not fix the one-second
polling loops. No architectural repair is implemented in this experiment.

## Verification, limitations and reproduction

- The original upstream run captured all idle windows, SQL checks and CPU
  profiles, but failed its final cleanup assertion. An independent check found
  no remaining processes. Its original failed status is preserved. Its Postgres
  timing uses original file modification times as an approximate proxy.
- The default native and configuration-matched main fixtures passed measurement,
  profiling and cleanup. The extended native fixture also passed. An optional
  external scheduling diagnostic on matched main raced shutdown and failed.
  Early integrated scheduling queries joined dataflow IDs directly to operator
  IDs and returned no rows. Those captures are invalid for attribution and are
  excluded. The corrected query maps dataflow addresses to root operator IDs,
  following `test/testdrive/explain-analyze.td`. Both corrected diagnostic fixtures
  passed, including nonempty counter validation and cleanup. Their four-second
  process samples are smoke checks, not additional performance windows.
- CPU profiles temporarily suspend jemalloc heap sampling. Heap captures are
  sampled allocation evidence and can omit allocations created while sampling
  was suspended. Process/PSS/allocator gauges supply memory totals.
- Process-lifetime counters remain continuous. Matched main has one shard's
  metrics disappear/reappear across processes and three shard counters reset.
  Those shard deltas are excluded and recorded, not treated as zero. The
  process-level backend rates and CPU/PSS comparison do not depend on them.
- Two adjacent windows are not independent deployment replications. This is one
  adapter, a small fresh catalog, debug builds and local backends. It does not
  establish production overhead, multi-adapter scaling, query latency, upgrade
  readiness or green CI. Existing unverified M3/CI outcomes remain unverified.

The companion scripts are in `misc/benchmarks/native-idle/`. Run from the repo
root with `bin/pyactivate`. All recorded fixtures used the same Postgres image ID
and repository digest, preserved in the companion JSON. Use `--postgres-image`
with that digest to reproduce the backend exactly. The collector is Linux-only and requires Docker and
curl. Profile reduction additionally needs Go/pprof, `llvm-symbolizer` and
`readelf`. Install the scoped extra Python dependency with
`bin/pyactivate -m pip install -r misc/benchmarks/native-idle/requirements.txt`.
Copy or hard-link pinned binaries into `$E/artifacts/main/`
(`environmentd`, `clusterd`) and `$E/artifacts/native/` (`controllerd`, `adapterd`,
`clusterd`, `pubsubd`). Both builds used the existing `bin/environmentd --build-only`
and `bin/controllerd --build-only` wrappers, with `CARGO_BUILD_JOBS=2`,
`RUSTUP_TOOLCHAIN=1.98.1`, `RUST_MIN_STACK=8388608` and `MZ_DEV_CI_BUILDER=1`.
Use isolated Cargo targets, or explicitly invalidate workspace artifacts when
switching worktrees. Blind shared-target reuse produced stale crate artifacts
and was corrected before native measurement.

Controllerd lacks metrics/profiling routes by default. Apply the companion
`controller-metrics.patch` only for the native measurement build, pin those
binaries, then reverse the patch. The production source is unchanged.

```sh
E="$HOME/.local/share/aj/m3-evidence/native-main-idle"
B=misc/benchmarks/native-idle
bin/pyactivate "$B/observe.py" main --evidence-dir "$E"
bin/pyactivate "$B/observe.py" native --evidence-dir "$E"
bin/pyactivate "$B/observe.py" main --evidence-dir "$E" --label main-matched \
  --system-parameter enable_metric_sink=false \
  --system-parameter persist_enable_incremental_compaction=false \
  --system-parameter enable_columnar_accumulable_diff=false \
  --system-parameter compute_mv_sink_shared_batch_max_bytes=0
bin/pyactivate "$B/observe.py" native --evidence-dir "$E" --label native-bucketed \
  --windows 4 --system-parameter enable_compute_temporal_bucketing=true \
  --scheduling-diagnostics
bin/pyactivate "$B/analyze.py" "$E"/<completed-run>
bin/pyactivate "$B/analyze_profile.py" "$E"/<completed-run> replica-s2-s2 cpu
bin/pyactivate "$B/analyze_scheduling.py" "$E"/<diagnostic-run>
```

Raw manifests, collector copies, process samples, metrics, profiles, build logs
and source audits are under `$E`. Original runs are `main-1793871889`,
`native-1793874637`, `main-matched-1793876174` and `native-bucketed-1793877535`.
Corrected diagnostic runs are `main-scheduling-1793880215` and
`native-scheduling-1793880610`. They use the matching configuration above,
`--warmup 300 --window 4 --windows 1 --profile-seconds 0 --scheduling-diagnostics`.
The [reduced evidence](20260903_decoupled_coordination_idle.json) preserves
versions, artifact hashes, settings, window-level measurements and scheduling
results independently of the disposable runtime databases.

Independent report review reproduced the key results without consequential
findings. Black, Ruff, reducer tests, Python syntax checks and the portable
collector's live smoke/diagnostic runs passed. The Docker ownership-filter check
also passed. Cleanup failure branches were hardened after review. The launcher
timeout fallback was inspected but not fault-injected. These are scoped checks,
not broad CI.

All owned runtime processes and containers are gone. Disposable databases/blobs,
compiler scratch and the unused upstream worktree were removed. Pinned binary
hashes were rechecked and raw measurement evidence retained. About 30 GiB disk
space remains free. No production Rust source change is part of this result.

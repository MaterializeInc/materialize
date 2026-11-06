# Native coordination: resource review

The subsequent [backend trace](20260903_decoupled_coordination_backend_load.md)
resolves the CAS/blob attribution left open here. This report remains the
uninstrumented CPU, memory and scheduling comparison.

Native still uses **2.0× CPU and 2.2× PSS** in the fresh idle development-build
comparison. The catalog replica contributes **62% of the extra CPU**. The main
caller-side targets are frontier computation and observation traffic, not another
round of Persist polling changes. No production behavior changed.

## Comparison

One frontend and four one-worker replicas per deployment: `mz_system`,
`mz_catalog_server`, `mz_probe` and `quickstart`. Native additionally runs
controllerd and pubsubd. Upstream embeds the relay in environmentd. The workload
is an indexed one-row table, then no user traffic during the idle windows.

Native production source is unchanged since `cf8b8ba066`, recorded at
`2208e3157c`. Upstream is the pinned `77ae0a3034` baseline, not newly fetched main.
Verified existing debug binaries were reused. Native includes only the existing
controller metrics/profiling routes, not scan tracing. Recorded build patches
were checked against committed source because embedded version labels predate
some commits. Both use Rust 1.98.1 and the recorded development profiles.

Each fresh private PostgreSQL 18/file-blob environment warmed for five minutes,
then ran two five-minute windows. Native read-hold publication remains **10 s**.
Common settings disable metric sinks, incremental compaction and columnar
accumulable diffs. Main additionally disables shared MV sink batches, a feature
absent from native. Shard metrics and info logging are enabled on both.

Runs and profile processing were sequential. No CPU throttling occurred. At least
12.0 GiB host memory remained available in measured windows. This is an idle
single-adapter comparison, not a scale test, release benchmark, or remote
CockroachDB/S3 benchmark. Branch versions, builtin inventories and dependencies
also differ, so not every difference is attributable to coordination alone.

## CPU and memory by process

CPU is the mean across the two windows, in cores. PSS is the second endpoint,
about fifteen minutes after setup. PSS apportions shared pages and can be summed.
RSS, USS and both window endpoints are retained in the accompanying data.

| Process | Main CPU | Native CPU | Main PSS MiB | Native PSS MiB |
| --- | ---: | ---: | ---: | ---: |
| Frontend | 0.270 | 0.282 | 564.3 | 579.1 |
| Controller | included above | 0.025 | included above | 410.6 |
| Relay | included above | 0.029 | included above | 38.2 |
| System replica | 0.014 | 0.086 | 99.5 | 346.9 |
| Catalog replica | 0.312 | 0.703 | 480.0 | 905.8 |
| Probe replica | 0.014 | 0.062 | 92.5 | 311.6 |
| Quickstart replica | 0.016 | 0.069 | 93.8 | 344.7 |
| Total Materialize | 0.626 | 1.256 | 1330.1 | 2936.8 |

The frontend difference is small. Native adds about 0.39 cores on the catalog
replica, 0.17 across the other replicas, and 0.054 in controller/relay processes.
PostgreSQL separately uses 0.019 versus 0.027 cores and about 274 versus 355 MiB
container memory at the final boundary. Container memory includes cache and is
not directly comparable with process PSS.

## Consensus and blob load

External API starts/s and payload KiB/s, averaged across both windows. Blob GETs
exclude Persist cache hits but can still hit the OS page cache. These are not
disk IOPS, network wire bytes or SQL transaction counts. CAS success includes an
expected-value mismatch. DELETE bytes describe deleted size, not transferred data.

| Operation | Main calls/s | Native calls/s | Main KiB/s | Native KiB/s |
| --- | ---: | ---: | ---: | ---: |
| consensus_scan | 7.96 | 8.57 | 126.21 | 233.05 |
| consensus_cas | 83.06 | 119.47 | 47.92 | 79.03 |
| consensus_head | 0.08 | 0.10 | 0.00 | 0.00 |
| consensus_truncate | 0.63 | 0.87 | 0.00 | 0.00 |
| blob_get | 2.15 | 13.92 | 17.56 | 321.77 |
| blob_set | 4.56 | 9.51 | 94.73 | 280.57 |
| blob_delete | 4.28 | 9.74 | 79.56 | 260.90 |

Scans are **8.07 / 9.07 native versus 8.44 / 7.49 upstream**, not a fixed large
request-count gap. Catalog-replica scans remain higher, offset by fewer frontend
scans. Their payload is still about 1.85× higher. Consensus CAS attempts are 1.44×,
blob GET calls 6.48× and GET payload 18.32× higher. Native blob SET calls are 2.09×.

Per-process API calls/s. Pubsubd exposes no Persist external counters, rather
than measured zero-valued series. Upstream includes its relay in environmentd.

| Process | Scans main/native | CAS main/native | Blob GET main/native | Blob SET main/native |
| --- | ---: | ---: | ---: | ---: |
| Frontend | 5.78 / 2.07 | 30.71 / 36.47 | 0.02 / 0.49 | 3.59 / 2.00 |
| Controller | n/a / 0.03 | n/a / 0.10 | n/a / 0.50 | n/a / 0.01 |
| Relay | n/a / n/a | n/a / n/a | n/a / n/a | n/a / n/a |
| System replica | 0.02 / 0.21 | 0.05 / 1.46 | 0.02 / 1.14 | 0.00 / 1.33 |
| Catalog replica | 2.11 / 5.77 | 52.21 / 78.47 | 2.07 / 9.42 | 0.97 / 3.45 |
| Probe replica | 0.01 / 0.24 | 0.04 / 1.47 | 0.02 / 1.24 | 0.00 / 1.37 |
| Quickstart replica | 0.04 / 0.26 | 0.04 / 1.49 | 0.02 / 1.13 | 0.00 / 1.34 |

Timestamp-oracle traffic is separate. Native averages 1.58 `write_ts` and 1.56
`apply_write` calls/s, versus about 1.00 each upstream. `read_ts` is 0.266 versus
0.200/s. PostgreSQL records about 134 versus 95 commits/s and 188 versus 127 KiB/s
of WAL. Those backend counters include oracle work and sampler queries.

## Where the extra work goes

### Frontier computation is the largest identified CPU target

The catalog replica uses 0.703 versus 0.312 cores. Its separate CPU profile puts
74.3% of samples under the compute worker, 5.2% under the catalog follower and
1.2% under written-plan decoding. These inclusive stacks overlap. Upstream puts
84.7% under its compute worker. They are post-window samples, not an additive
accounting of the idle CPU difference.

The post-window scheduling diagnostic identifies stable root operators:

| Dataflow | Main ms/s | Native ms/s |
| --- | ---: | ---: |
| mz_frontiers_ind | 3.13 | 50.91 |
| mz_cluster_replica_frontiers_ind | 3.47 | 33.29 |
| mz_source_statuses_ind | 7.56 | 15.51 |
| mz_hydration_statuses_ind | 7.44 | 12.21 |
| mz_object_dependencies | 1.85 | 10.72 |
| mz_sink_statuses_ind | 3.45 | 8.88 |

Total root scheduling time is 154.6 versus 300.6 ms/s, with stable inventories
and no added/removed roots. The two frontier indexes contribute **77.6 of the
146.0 ms/s increase**, about 53%. This is elapsed operator time, not 53% of the
deployment CPU gap. Native authority projection costs another 6.6 ms/s.

The frontier views filter observations by placement and deployment/incarnation
authority, then compute public reductions. That is real work beyond upstream's
simpler frontend-published relations. Their shared input/reduction structure is
the first place to examine plans and reuse existing arrangements. Do not assume
every static JSON expression is reevaluated on every frontier update.

### Observation streams dominate the new read/write pressure

The native catalog replica performs 9.42 of 13.92 external blob GETs/s. The
three other replicas add about 3.50/s. Their largest named part-fetch/write
activity is on `mz_replica_frontier_observations` and the catalog. The catalog
replica also reads `mz_storage_frontier_observations`. Upstream's catalog replica
performs 2.07 external GETs/s, and the other replicas about 0.02 each.

These are independent incremental followers and durable observation producers,
not repeated full-catalog snapshots. Even user-idle replicas publish advancing
introspection logging-index frontiers. Replica publication already suppresses
unchanged rows and drains ready events in batches. Recommending those existing
checks as a new optimization would be misleading.

Shard part-fetch counters include cache hits. They locate logical read pressure,
not exact external request attribution. Blob writes include compaction and rollups.
This run does not prove how much backend traffic a producer batching change
would remove, nor that independent owners can simply stop publishing.

### Memory has several owners

Final boundary MiB, before profiling:

| Category | Main | Native |
| --- | ---: | ---: |
| Anonymous PSS | 815.1 | 2029.6 |
| File-backed PSS | 515.1 | 907.2 |
| jemalloc allocated | 537.2 | 1195.2 |
| jemalloc resident | 729.3 | 1763.1 |
| Blob-cache payload | 57.2 | 377.2 |

These categories overlap. Blob payload is live allocation, and allocated bytes
are already part of allocator residency. Retained virtual memory is not another
resident cost. The 320 MiB cache-payload difference is substantial beside the
658 MiB extra allocated heap, but does not explain the whole 1,607 MiB PSS gap.
About 392 MiB of that PSS gap is file-backed. Controllerd alone has 237 MiB
file-backed PSS, which includes reclaimable executable pages.

Each follower reconstructs a SQL catalog and retains durable/catalog/plan state.
Fresh sampled heaps attribute about 13–22 MiB per catalog-bearing native process
to catalog item reconstruction. These are inclusive allocation stacks, not
disjoint object-byte censuses. They establish a real owner, not an explanation
for the entire premium. User-idle replicas also retain encoded observation
batches and catalog blob payloads.
They do not accidentally create separate Persist caches for each local component.

Native PSS grows from 2,854 to 2,937 MiB between the two endpoints, versus 1,312 to
1,330 MiB upstream. These are finite-age measurements, not a demonstrated plateau
or leak. Cutting cache budgets might increase already elevated blob reads.
That tradeoff needs measurement, not an assumed memory win.

## Ranked caller-side recommendations

1. **Optimize the frontier relations first.** Inspect the physical plans for
   `mz_frontiers_ind`, `mz_cluster_replica_frontiers_ind` and their status-view
   consumers. Try sharing the current-observation/placement work through existing
   arrangements and simplifying repeated joins/reductions. Add another maintained
   arrangement only if measurement justifies its memory and maintenance cost.
   The named scheduling gap makes this the strongest CPU target. Keep the durable
   authority projection, replica ownership, deployment/incarnation filters, and
   the distinction between unknown and completed frontiers. Verify public results
   across join, restart, retirement and promotion. No slower publication cadence.

2. **Reuse the existing full WAL reader for query snapshots.**
   `QueryClient::collection_reader` starts a third full `TxnsRead` alongside the
   readers in StorageCollections and storage-controller in the same adapter.
   Idle retirement cleanup also calls this path, including for a nontransactional
   status snapshot, so it is not limited to user
   snapshot queries. Inject a same-location, same-shard full reader after WAL
   bootstrap, retaining the metadata-only query API and snapshot protection.
   This is definite duplicated ownership, but its separate CPU/I/O saving has not
   been measured. It will not remove the frontier dataflows' compute cost.

3. **Reuse immutable producer facts instead of repeatedly decoding whole plans.**
   `catalog_follower/storage_metadata.rs::resolve` reads complete written plans
   every ordinary resolution to obtain MV schemas. Local cache hits avoid I/O but
   still deserialize `GlobalExpressions` in `catalog/expr_cache.rs::read_plans`.
   Reuse selected plans where available, otherwise retain only the needed facts
   keyed by namespace, producer, revision and item version. Evict with demand.
   Keep current selections, aliases, permissions and negative lookups dynamic.
   The measured decode stack is only 1.2% of catalog-replica samples, so this is a
   small cleanup, not the solution to the CPU gap. Dropping unused MIR/notices
   from replica selection is another narrow lifetime improvement, but the whole
   selected-plan allocation stack is only 0.5 MiB in this fixture.

4. **Investigate observation batch boundaries before changing producers.**
   `ReplicaEnactment::apply_progress` aggregates partial reports before enqueuing
   individual rows. `execution/frontiers.rs` already deduplicates rows and uses
   `recv_many`. Measure whether one aggregated report is fragmented into several
   appends. If so, preserve that report as one ordered producer batch, including
   retirement. Also check duplicated downstream raw-input consumption while
   optimizing the frontier views. More than one blob SET/s is not proof of
   fragmentation because compaction writes count too. Do not replace independent
   owners with an elected publisher or claim all new durable traffic is avoidable.

5. **Keep retirement and large memory redesigns separate.** Current adapter
   cleanup snapshots one current-status collection and the sessions/subscriptions
   tables on the protection cadence. An incremental consumer could avoid repeated
   snapshots, but must retain the monotone retirement certificate, snapshot
   protection, conditional retractions, WAL ordering and late-row cleanup.
   Controller plan-store lifetime and full SQL-catalog reconstruction are further
   memory candidates. Replacing that representation would be a broader design,
   not a small cache optimization. Current profiles do not justify claiming large
   savings from those changes or labeling the observed memory a leak.
   If RAM becomes the priority, first test the existing cache-budget setting
   against backend misses. That is cheaper than inventing role-specific cache or
   catalog machinery. No default change is justified by these measurements alone.

Source boundaries are `src/catalog/src/builtin/{mz_internal,mz_catalog}.rs`,
`src/clusterd/src/catalog_follower{.rs,/storage_metadata.rs,/execution/frontiers.rs}`,
`src/catalog/src/expr_cache.rs`, `src/adapter/src/query_client.rs`,
`src/adapter/src/coord/retired_rows.rs` and
`src/storage-client/src/collection_writer.rs`. The source notes in the evidence
retain ownership details and corrections. No pre-rollup change, shared Persist
recovery mechanism, cache implementation change or new consensus protocol is
part of these recommendations.

## Evidence and limits

[Recorded data](20260903_decoupled_coordination_resource_review.json) contains
both windows, every process, allocator/cache categories, backend payload/rates,
oracle and PostgreSQL counters, settings, hashes, scheduling and named profile
stacks. Raw evidence is under `~/.local/share/aj/m3-evidence/native-main-usage/`:

- `main-1793988015/` and `native-1793989320/`: passed fresh fixtures.
- `provenance.json` and `source-verification.json`: binary hashes and reconstructed
  build inputs checked against committed source.
- `comparison.json`, `profile-summary.json` and `summarize.py`: reduction evidence.
- `memory-ownership-audit.md` and `usage-path-notes.md`: reviewed source findings.
- `native-runtime.log`, `native-exact-retry.log`: the initial invocation and exact
  retry failed before measurement on the upstream-only setting. No assertion was
  weakened. The corrected native invocation passed.

All CPU and heap profiles were captured after the idle windows and symbolized
against their exact binary build IDs. Heap sampling pauses during each CPU
capture, preserving earlier samples but missing allocations during that interval.
Small process profiles have few CPU samples. Per-file mappings were captured
post-window and can overlap profiling, so they identify mapping owners but are
not used as idle memory totals. The pre-profile smaps boundaries supply those.

Counter checks reject process replacement and lifetime resets. Shard-series
additions/removals remain explicit. The scan comparison is workload-variable,
not a promise that metadata reads now exactly match upstream. Profile fractions,
operator elapsed time and idle OS CPU are different measurements.

Both final fixtures, profile processing and cheap repository checks passed. All
owned runtimes are stopped. The harness now enforces the 30-GiB disk reserve as
well as memory headroom. Broad CI remains non-green and was not rerun. No proposed
repair has been implemented or benchmarked.

# Cached storage-frontier publication

Reusing `storage_collections` removes about **106 consensus scans per second**
from the native idle fixture: **46% of deployment-wide scans and 80% of adapter
scans**. These windows show no clear improvement in total Materialize CPU or
memory. PostgreSQL CPU fell by about 0.009 cores, roughly 19%.

## Change and boundaries

The native publisher now reads the existing cached write frontier for an exact
object/shard binding. `storage_collections` already follows Persist progress
through its shared-state watches, including one WAL watch for all transactional
collections. Its existing fallback refreshes remain unchanged.

The separate per-collection `recent_upper` polling function is gone. The
publisher's inventory stores shard IDs instead of schemas, locations and WAL
metadata. A small binding-checked cache accessor replaces those backend reads.
Production line count is slightly higher because of that accessor and dependency
wiring, but there is no new state store, watcher, leader or progress protocol.

Cooperative publication is unchanged: consume shared observations incrementally,
merge monotonically, then compare-and-append. Conflicts refresh before retry.
Catalog certification still controls read bounds and retirement. Missing cached
state is unknown, never completion. Metadata-only recovery entries are excluded.
Live read policies retain completed collections' empty write frontier until drop.
Timestamp selection, authorization and write fencing keep their existing reads.

## Measurements

Each cell contains the first and second five-minute windows. Both fresh native
environments warmed for five minutes after readiness and setup. They used the
same fixture and effective settings as the earlier
[idle comparison](20260903_decoupled_coordination_idle.md): one adapter, four
one-worker replicas, an indexed one-row table, a private PostgreSQL 18 backend,
file blobs, debug builds with soft assertions, and
`catalog_read_protection_publish_interval = '10 s'`. Observation cadence remains
unchanged. Builds and lifecycle traffic did not overlap the measured windows.

| Measurement | Before | After |
|---|---:|---:|
| Deployment consensus scans/s | 228.46 / 229.58 | 122.20 / 123.04 |
| Adapter consensus scans/s | 131.34 / 131.16 | 25.44 / 25.97 |
| Consensus scan payload, kB/s | 236.82 / 229.03 | 248.89 / 235.17 |
| Consensus CAS/s | 119.38 / 121.09 | 120.71 / 121.96 |
| Blob gets/s | 14.36 / 14.28 | 14.20 / 14.65 |
| Blob sets/s | 9.74 / 9.63 | 9.76 / 9.67 |
| Materialize CPU, cores | 1.284 / 1.309 | 1.291 / 1.279 |
| Adapter CPU, cores | 0.291 / 0.292 | 0.295 / 0.267 |
| Catalog replica CPU, cores | 0.707 / 0.744 | 0.734 / 0.720 |
| Controller CPU, cores | 0.025 / 0.026 | 0.026 / 0.027 |
| PostgreSQL CPU, cores | 0.0463 / 0.0473 | 0.0381 / 0.0377 |
| Materialize endpoint PSS, MiB | 2890 / 3000 | 2939 / 3068 |

This removes mostly empty metadata refreshes, not history replay or blob work.
The remaining catalog-replica polling and frontier reductions are unchanged.
Per-process CPU, RSS, PSS and polling rates are in the
[measurement record](20260903_decoupled_coordination_cached_frontiers.json).

These are two adjacent windows in one fresh deployment per version, not
independent repeats or a production scaling prediction. No CPU throttling was
measured, and available host RAM stayed above 13.49 GiB. The candidate build
briefly left only 367 MiB of disk free. Inactive compiler scratch was removed
during startup, before readiness and the five-minute warmup.

## Verification and limitations

The storage-collections suite and full storage-controller unit suite passed.
Targeted Clippy passed for storage-client, storage-controller and compute-client,
including test targets. The cache-binding test failed with its shard comparison
removed and passed after restoration. Coverage includes shared-WAL progress,
completed and unknown frontiers, lagging publishers, real CAS conflicts,
restarts, exact-binding retirement and shutdown.

The live lifecycle check passed on restarted and fresh candidate fixtures. A
survivor kept inserting while a second adapter joined, restarted and was removed. The
check observed actual table, index and materialized-view frontiers, verified
monotonicity and row preservation, and required both peer executions to catch up.
It used internal SQL as `mz_system` for diagnostic catalog access. HTTP remained
responsive. No permissions were changed.

The measured candidate run retains an overall failed status because its
post-window diagnostic initially used a restricted view through the normal SQL
role. That failure was reproduced unchanged. A subsequent array-parameter typing
error was also reproduced and fixed in the harness. The idle windows themselves
passed and are unaffected. The corrected standalone lifecycle run and the
fresh collector/lifecycle smoke passed.
The restart log also contains finalization retries for dropped test shards.
Physical finalization timing is not verified by this frontier-publication check.
Broad CI remains unverified/non-green as recorded in the
[handoff](20260903_decoupled_coordination_log.md).

## Reproduction and evidence

Evidence root: `~/.local/share/aj/m3-evidence/cached-storage-frontiers/`.
Measured runs are `before/before-1793891643` and `after/after-1793894562`.
`comparison.json`, build inputs, source patch, binary hashes, raw metrics and
process samples are retained there. `lifecycle-typed.log` records the successful
standalone lifecycle check. Earlier failed diagnostics remain alongside it.
`after/lifecycle-smoke-1793896387` records the passing fresh fixture and cleanup.
All owned processes are stopped. Disposable databases, blobs and compiler
scratch were removed, leaving about 16 GiB free. Raw evidence and binaries remain.

Build with `bin/controllerd --build-only`, Rust 1.98.1, two Cargo jobs and the
environment recorded in the measurement JSON. Match the temporary
`misc/benchmarks/native-idle/controller-metrics.patch` in both measurement builds,
pin the binaries, then reverse the patch. Production source has no added metrics
routes. Put each version's four binaries in its own `$E/artifacts/native/`.

```sh
bin/pyactivate misc/benchmarks/native-idle/observe.py native \
  --evidence-dir "$E" --warmup 300 --window 300 --windows 2 \
  --profile-seconds 0 --frontier-lifecycle
bin/pyactivate misc/benchmarks/native-idle/analyze.py "$E"/<run>
```

The lifecycle option runs after measurement and stops its own peer and objects.
The collector stops the fixture and its private backend. A short lifecycle-only
smoke can use `--warmup 0 --window 4 --windows 1`. Do not use its windows as idle
performance evidence.

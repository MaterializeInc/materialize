# Shared frontier inputs: measured result

Shared validated indexes eliminate duplicate frontier-source consumption. Total
blob GETs fell 28.7%, from 14.09/s to 10.05/s. This is a read-traffic repair,
not a solution to the remaining catalog write and compaction load.

## Change and boundaries

Two ordinary indexes in `mz_catalog_server` now arrange the existing
`mz_current_replica_frontiers` and `mz_current_storage_frontiers` views by
`object_id`. Both public frontier indexes import these arrangements instead of
independently importing the observation sources. Existing catalog index ordering
and optimizer index selection provide the sharing. No new owner, protocol,
durable output shard, sampling mechanism or Persist behavior is introduced.

The validation and public-view SQL is unchanged: deployment/incarnation,
placement and object/shard checks still apply. Unknown observations remain
different from known empty frontiers. The durable deployment-authority projection
remains the barrier. The shared indexes do not request metrics-history retention.
Existing dependency read holds propagate through index imports. The builtin index
inventory and dependency MVs have replacement declarations for their changed
inlined fingerprints. Cross-release migration was not demonstrated in this run.

## Method

Fresh native fixtures before and after the change used one adapter, controller,
relay and four single-worker replicas, plus an indexed one-row table. Each had
five minutes of warmup and two five-minute idle windows. Runs were sequential.
Read-hold publication remained 10s. `enable_metric_sink`, incremental compaction
and columnar accumulable diff were disabled in both fixtures. The recorded
upstream run used the same workload and the previously documented shared-batch
setting. See the [prior comparison](20260903_decoupled_coordination_backend_load.md).

These are instrumented default development builds, not production benchmarks.
Metadata uses private PostgreSQL SERIALIZABLE transactions and blobs use local
files. Calls and bytes describe backend APIs and payloads, not billed network
traffic or physical disk I/O. Rates use each process's measured elapsed time.
CPU is cores consumed. Memory is the mean PSS at the two window ends, not peak
memory. PostgreSQL and the host page cache are not included in process totals.

Every available audited counter reconciled with its event ledger. All seven
native roles and all five upstream roles were present. An earlier native run
missed process discovery through a symlink and is excluded. Discovery now resolves
the binary path and rejects missing roles. The reducer rejects empty inventories.

## Total Materialize load

| Metric | Recorded upstream | Native before | Native after |
| --- | ---: | ---: | ---: |
| CPU cores | 0.652 | 1.295 | 1.251 |
| PSS MiB | 1335.7 | 2932.8 | 2939.4 |
| Consensus scans/s | 3.607 | 8.880 | 7.560 |
| Scan payload KiB/s | 136.72 | 229.77 | 226.42 |
| CAS calls/s | 83.031 | 119.771 | 120.877 |
| CAS payload KiB/s | 47.92 | 79.18 | 80.01 |
| GET calls/s | 2.276 | 14.092 | 10.050 |
| GET payload KiB/s | 26.97 | 326.22 | 298.38 |
| SET calls/s | 4.554 | 9.539 | 9.584 |
| SET payload KiB/s | 95.02 | 280.13 | 286.63 |
| DELETE calls/s | 4.301 | 8.742 | 8.669 |
| DELETE payload KiB/s | 80.59 | 257.33 | 249.50 |

CAS calls include attempts that lose contention. CAS payload counts committed
bytes, not bytes in rejected attempts. DELETE payload counts removed content,
not request size. Scan payloads come from existing metrics rather than the
GET/SET/CAS event ledger.

CPU fell 3.4% and PSS rose 0.2%. Catalog-replica CPU fell only 1.7%. These small
changes do not establish a repeatable CPU or memory improvement. CAS calls rose
0.9% and SET bytes rose 2.3%. Maintenance timing and distributed ownership vary
between runs, so neither small regressions nor savings are isolated causal effects.

## Per-process measurements

Each backend cell is **calls/s / KiB/s**. The JSON preserves all available
operation counters, including head and truncate calls. Pubsub has no Persist
backend client counters, shown as absent rather than inferred zero traffic.
`s1` is the system cluster, `s2` the catalog server, `s3` probe and `u1` quickstart.

### Recorded upstream

| Process | CPU | PSS MiB | Scan | CAS | GET | SET | DELETE |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| environmentd | 0.283 | 564.7 | 1.48 / 58.9 | 30.68 / 18.5 | 0.15 / 9.8 | 3.57 / 62.8 | 3.40 / 60.4 |
| replica-s1-s1 | 0.015 | 94.3 | 0.01 / 0.3 | 0.05 / 0.0 | 0.02 / 0.1 | 0.00 / 0.0 | 0.00 / 0.0 |
| replica-s2-s2 | 0.322 | 483.6 | 2.07 / 77.1 | 52.22 / 29.4 | 2.07 / 16.8 | 0.99 / 32.3 | 0.90 / 20.2 |
| replica-s3-s3 | 0.015 | 97.6 | 0.01 / 0.3 | 0.04 / 0.0 | 0.02 / 0.1 | 0.00 / 0.0 | 0.00 / 0.0 |
| replica-u1-u1 | 0.017 | 95.5 | 0.04 / 0.2 | 0.04 / 0.0 | 0.02 / 0.1 | 0.00 / 0.0 | 0.00 / 0.0 |

### Native before

| Process | CPU | PSS MiB | Scan | CAS | GET | SET | DELETE |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| controllerd | 0.026 | 427.1 | 0.05 / 0.3 | 0.12 / 0.1 | 0.51 / 27.6 | 0.01 / 0.7 | 0.00 / 0.0 |
| pubsubd | 0.034 | 37.9 | n/a | n/a | n/a | n/a | n/a |
| replica-s1-s1 | 0.080 | 330.7 | 0.21 / 12.5 | 1.46 / 2.8 | 1.23 / 45.8 | 1.33 / 23.2 | 1.90 / 42.5 |
| replica-s2-s2 | 0.724 | 876.1 | 6.19 / 119.4 | 78.64 / 49.1 | 9.48 / 143.3 | 3.45 / 130.4 | 1.79 / 35.9 |
| replica-s3-s3 | 0.064 | 313.7 | 0.24 / 7.0 | 1.43 / 2.7 | 1.16 / 37.0 | 1.34 / 21.6 | 0.20 / 31.4 |
| replica-u1-u1 | 0.087 | 341.9 | 0.26 / 12.5 | 1.52 / 2.6 | 1.25 / 36.3 | 1.39 / 21.0 | 1.80 / 14.2 |
| adapterd | 0.279 | 605.4 | 1.92 / 78.0 | 36.60 / 21.9 | 0.46 / 36.1 | 2.01 / 83.3 | 3.05 / 133.3 |

### Native after

| Process | CPU | PSS MiB | Scan | CAS | GET | SET | DELETE |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| controllerd | 0.025 | 406.0 | 0.05 / 0.4 | 0.12 / 0.1 | 0.51 / 26.5 | 0.01 / 0.9 | 0.00 / 0.0 |
| pubsubd | 0.034 | 37.6 | n/a | n/a | n/a | n/a | n/a |
| replica-s1-s1 | 0.062 | 307.4 | 0.29 / 3.8 | 1.49 / 2.9 | 1.30 / 33.8 | 1.41 / 20.1 | 0.07 / 0.3 |
| replica-s2-s2 | 0.712 | 882.8 | 5.14 / 127.5 | 79.66 / 49.8 | 5.42 / 99.0 | 3.42 / 105.7 | 3.88 / 64.8 |
| replica-s3-s3 | 0.074 | 328.3 | 0.22 / 8.4 | 1.46 / 2.8 | 1.18 / 38.2 | 1.36 / 24.7 | 2.02 / 30.4 |
| replica-u1-u1 | 0.067 | 379.7 | 0.25 / 5.3 | 1.51 / 2.4 | 1.14 / 47.1 | 1.35 / 31.1 | 0.10 / 8.0 |
| adapterd | 0.276 | 597.5 | 1.62 / 81.0 | 36.64 / 21.9 | 0.50 / 53.9 | 2.03 / 104.1 | 2.60 / 146.0 |

The scan rate also fell, but this run did not trace scan callers again. That
change is observational rather than a complete causal attribution.

## Direct evidence for the reduction

Before the change, the catalog replica fetched 1,791 replica-observation keys
and 660 storage-observation keys twice within 50ms through different readers.
No delete occurred between either read. These repeats total 11,665,830 bytes
over the measured windows. After the change, neither observation shard had
any repeated GET key in that process.

The catalog replica's GET rate fell 9.48 to 5.42/s. Total GETs fell by 4.04/s,
consistent with removing roughly 4.08 duplicate reads/s. The duplicated payload
was about 19 KiB/s. Total GET payload fell by 27.84 KiB/s, which also includes
changes in compaction, row fetches and maintenance. Do not attribute all of
that byte reduction to source sharing.

Stored physical plans for both public indexes import both shared indexes. Their
complete JSON source lists contain neither observation source. The check runs on
the owner, new peer, restarted peer and recovered catalog replica. Text EXPLAIN
alone is insufficient because it can omit identity source operators. The same
shared-input assertion fails against the pre-change binary.

## Correctness checks

- Native lifecycle passed while a survivor kept writing: peer join, peer restart,
  catch-up and peer removal preserved exact table/MV contents and monotone public
  frontier observations.
- The catalog replica was identity-checked, terminated and replaced. An open
  transaction retained its global and per-replica frontier rows across recovery.
  A separate session observed successful reconstruction before equality checks.
- Catalog EXPLAIN inventory and plans, index accounting and builtin OID SLTs
  passed after regeneration, then passed again without rewrite mode.
- The isolated `SHOW INDEXES IN CLUSTER mz_catalog_server` testdrive assertion
  was regenerated and checked against the restarted native deployment. Its only
  additions are the two shared indexes. The whole Kafka-dependent file was not run.
- Python reducer regressions, targeted Black/Ruff and changed-file Rust formatting
  passed. Full `bin/fmt --check` fails on the unchanged overlong generic call in
  `src/catalog/src/catalog.rs:2164`, reproduced with the same command.

The view SQL and publisher code are unchanged. The Kafka-dependent full frontier
and indexes testdrive suites, deliberate invalid-provenance SQL fixtures and
cross-release upgrade tests were not run here. This is not green CI. Broad CI
limitations remain in the [handoff](20260903_decoupled_coordination_log.md).

## Remaining load and proposed next work

The raw catalog still costs about 256 KiB/s of GET payload after the change:
116 KiB/s row fetch, 45 KiB/s source fetch, 93 KiB/s compaction and 3 KiB/s GC.
It also produces about 126 KiB/s of compaction SET payload plus 25 KiB/s of
ordinary uploads. Sharing frontier inputs does not address this journal traffic.

The catalog replica still commits roughly 71 empty progress appends/s across
its outputs. Both versions have 50 durable catalog outputs. These are writes to
each MV's own shard, not 50 writers advancing the raw catalog shard. Native
catalog publications create extra timestamps that propagate even through views
that filter out their rows. This matches the prior causal attribution.

Proposed next scope: examine how application-owned catalog publications and
common input arrangements can avoid unnecessary journal/progress work while
preserving certification and prompt config visibility. Separately examine the
distributed catalog compaction work before changing maintenance ownership. Do
not add a designated leader, slow publication, or change Persist as part of this
frontier repair. The remaining CPU and memory gap requires separate attribution,
not an assumption that removing four GETs/s should erase it.

## Evidence

The [machine-readable comparison](20260903_decoupled_coordination_shared_frontiers.json)
contains per-process rates and duplicate-key evidence. Full local artifacts are
under `~/.local/share/aj/m3-evidence/shared-frontier-inputs/`:

- `prechange/native-before-verified-1794141291/`: accepted before run.
- `postchange/native-after-1794143937/`: accepted after run and lifecycle evidence.
- `postchange/shared-plan-witness-1794143806/`: plan witness, not a cost comparison.
- `postchange/build.json`, binary hashes and saved patches: exact instrumented
  candidate provenance. Instrumentation was reversed before the production commit.
- `slt-verify.log`, `final-unit-checks.log`, `implementation-review.md` and
  `measurement-audit.json`: checks, independent review and rejected-run accounting.

The recorded upstream fixture is `backend-load-attribution/main-audit-1793998684/`
at revision `77ae0a3034efefb22919b6257dd40e5ebc1827bb`. Before binaries are
from `604c47268074fcd100910dc9ab4c4720f1ba1662`. The candidate is `ce57c3fff2`
plus this production change and the same diagnostic patches. The intervening
commit changed diagnostics/documentation, not production behavior.

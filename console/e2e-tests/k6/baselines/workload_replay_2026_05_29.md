# k6 Baseline: Cluster Detail under VU Ramp

**Date:** 2026-05-29
**Workload:** workload-replay (continuous ingestion grew the catalog between dump and run)
**Environment:** Local workload-replay
**Console:** Production code, no optimizations (`main`)
**Cluster tested:** `mz_catalog_server` (system cluster, id: `s2`, default replica size)
**Script:** `cluster-detail.ts`, ramp 0→5→20→50→100 VUs over 6m

## Per-label `http_req_duration` (full ramp, 6m)

| Label | Cadence | Min | Med | Avg | p95 | p99 | Max | Threshold | Pass? |
|---|---|---|---|---|---|---|---|---|---|
| healthCheck | 5s | 2.04ms | 2.79ms | 3.42ms | 6.66ms | 11.66ms | 47.51ms | p95<500ms | ✓ |
| clusters.clusterFreshness | 5s | 432.69ms | 6.59s | 7.46s | 15.83s | 16.58s | 16.63s | p95<2s | ✗ |
| clusters.largestClusterReplica | 60s | 701.49ms | 6.14s | 7.93s | 19.39s | 20.12s | 20.48s | p95<5s | ✗ |
| clusters.replicaUtilizationHistory | 20s | 157.83ms | 10.69s | 11.89s | 30.26s | 33.09s | 34.09s | p95<2s | ✗ |
| clusters.list | 5s | 319.5ms | 19.45s | 21.24s | 45.26s | 45.88s | 50.95s | p95<2s | ✗ |
| clusters.largestMaintainedQueries | 60s | 1m0s | 1m0s | 1m0s | 1m0s | 1m0s | 1m0s | p95<5s | ✗ (timeouts) |
| clusters.materializationLag | 5s | — | — | — | — | — | — | p95<2s | not captured by dump |

**Total:** 1538 requests, 51 failures (3.31%), throughput 3.94 RPS

## Execution

| Metric | Value |
|---|---|
| Iterations completed | 248 (71 interrupted) |
| Iteration duration (avg) | 48.23s |
| Iteration duration (p95) | 2m3s |
| Iteration duration (max) | 2m37s |
| Peak VUs | 100 |
| Data received | 266 MB |
| Data sent | 3.0 MB |

## Key observations

- All polling queries that touch `mz_catalog_server` with real work degrade 16–45×; `healthCheck` stays flat at p95 6.66ms — the bottleneck is catalog_server compute, not request handling or networking.
- `clusters.list` is the worst non-timeout offender: p95 45.26s, 23× over its 2s budget.
- `largestMaintainedQueries` pins at the 60s k6 request-timeout ceiling and accounts for all 51 failed requests. **Partial artifact:** the dumped request has `replicaHeapLimit: 0` baked in (the React Query hook short-circuits on null/undefined but not on 0). With a realistic heap-limit value the query would still be slow under load, but probably not at the timeout ceiling.
- `materializationLag` did not fire during the dump (only 11 labels captured this run vs 12 prior). Its threshold pass is k6's no-data default, not a real result.
- Throughput plateaus at ~4 RPS; VUs run ~10× slower than their 5s polling target (iter_duration avg 48s, p95 2m3s).
- Re-runs against the same workload-replay container are not directly comparable — continuous ingestion grows the catalog between runs, which makes catalog_server queries heavier each time. Capture the docker container state alongside any new baseline.

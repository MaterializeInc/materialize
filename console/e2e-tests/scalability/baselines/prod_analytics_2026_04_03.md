# Scalability Baselines: prod_analytics workload replay

**Date:** 2026-04-03
**Workload:** `workload_prod_analytics.yml` (1002 tables, 9 sources, 25 MVs, 7 clusters)
**Environment:** Local workload replay, `bootstrap_replica_size="50cc"`
**Console:** Production code, no optimizations
**Cluster tested:** `mz_catalog_server` (system cluster, id: `s2`)

## Query Audit — Cluster Detail (single tab, 2min)

| Query | Count | Avg | Max | Errors |
|-------|-------|-----|-----|--------|
| largestMaintainedQueries | 2 | 2917ms | 3116ms | 0 |
| largestClusterReplica | 3 | 1208ms | 1859ms | 0 |
| clustersList | 24 | 276ms | 1368ms | 0 |
| rbacCheck | 2 | 450ms | 875ms | 0 |
| clusterFreshness | 1 | 888ms | 888ms | 0 |
| materializationLag | 1 | 855ms | 855ms | 0 |
| replicaUtilizationHistory | 6 | 211ms | 304ms | 0 |
| healthCheck | 25 | 14ms | 124ms | 0 |
| deploymentLineage | 6 | 35ms | 88ms | 0 |
| canCreateObjects | 1 | 62ms | 62ms | 0 |

**Total:** 72 requests, 0 failures, peak concurrent: 6

### Key observations
- `largestMaintainedQueries` is the slowest query (3.1s max) — uses session variables
- `clustersList` polls every 5s (24 calls in 2min) — biggest call volume
- `healthCheck` polls every 5s (25 calls) — fast (124ms max)
- All server time, negligible queue time on a single tab

## Cold Load — Cluster Detail

| Metric | Value |
|--------|-------|
| Time to content ("Resource Usage") | 1412ms |
| Queries during load | 10 |

## Navigation Flow — Cluster List to Detail

| Step | Time | Queries |
|------|------|---------|
| 1. Cold: Cluster List | 1414ms | 8 |
| 2. Click: Cluster Detail | 792ms | 4 |
| 3. Back: Cluster List | 1495ms | 10 |
| 4. Click: Cluster Detail (revisit) | 793ms | 4 |

**Cache speedup:** 0% (792ms → 793ms) — `page.goto()` back to list resets React state, so cache has no effect

### Key observations
- Clicking into cluster detail from the list (792ms) is faster than cold load (1412ms) — shared queries are already cached from the list page
- Revisit shows identical time — no additional cache benefit beyond what the list page already provided

## Tab Stress — 6 Tabs on Cluster Detail (2min hold)

| Query | Count | Avg | Max | Max Queue | Max Server |
|-------|-------|-----|-----|-----------|------------|
| replicaUtilizationHistory | 42 | 1324ms | 7088ms | 1820ms | 7055ms |
| clustersList | 129 | 2283ms | 7076ms | 4210ms | 6779ms |
| largestMaintainedQueries | 17 | 3807ms | 5104ms | 1ms | 5103ms |
| healthCheck | 173 | 394ms | 4542ms | 4533ms | 21ms |
| largestClusterReplica | 23 | 2156ms | 3733ms | 2719ms | 3497ms |
| deploymentLineage | 42 | 705ms | 2628ms | 2589ms | 1279ms |
| materializationLag | 6 | 1236ms | 1814ms | 279ms | 1798ms |
| clusterFreshness | 8 | 1149ms | 1839ms | 1ms | 1799ms |
| rbacCheck | 12 | 599ms | 1395ms | 944ms | 1392ms |
| canCreateObjects | 6 | 211ms | 946ms | 888ms | 86ms |

**Total:** 464 requests, 0 failures, peak concurrent: 16

### Key observations
- **Connection pool starvation:** health check queue reaches 4.5s (server time is only 21ms)
- `clustersList` is the biggest volume offender (129 calls) — polls every 5s per tab
- `largestMaintainedQueries` degrades from 3.1s (single tab) to 5.1s (6 tabs) — pure server time, no queueing
- `replicaUtilizationHistory` worst case 7.1s — severe degradation under contention
- Peak concurrency 16: 6× clustersList + 6× healthCheck + others
- At peak, 6 clustersList and 6 healthCheck requests are in-flight simultaneously across all tabs
- 0 failures — no timeouts even under heavy load on this workload

## Query Audit — Sources List (single tab, 2min)

| Query | Count | Avg | Max | Errors |
|-------|-------|-----|-----|--------|
| sourcesList | 23 | 430ms | 1040ms | 0 |
| rbacCheck | 2 | 187ms | 334ms | 0 |
| healthCheck | 25 | 15ms | 122ms | 0 |
| canCreateObjects | 2 | 77ms | 78ms | 0 |
| databaseList | 1 | 39ms | 39ms | 0 |

**Total:** 54 requests, 0 failures, peak concurrent: 7

### Key observations
- `sourcesList` polls every 5s (23 calls) — 1040ms max, heavier than sinks (more joins: mz_source_statuses, mz_kafka_sources, mz_webhook_sources)
- Same structure as sinks list but ~2x slower due to additional source-type joins
- `databaseList` label is now working (was unlabeled in earlier runs)

## Query Audit — Sinks List (single tab, 2min)

| Query | Count | Avg | Max | Errors |
|-------|-------|-----|-----|--------|
| sinksList | 24 | 205ms | 735ms | 0 |
| rbacCheck | 2 | 129ms | 223ms | 0 |
| healthCheck | 25 | 15ms | 135ms | 0 |
| databaseList | 1 | 60ms | 60ms | 0 |
| canCreateObjects | 1 | 44ms | 44ms | 0 |

**Total:** 54 requests, 0 failures, peak concurrent: 5

### Key observations
- `sinksList` polls every 5s (24 calls) — 735ms max, the only page-specific query
- Lightweight page overall — no session variables, no introspection queries
- `databaseList` fires once for the filter dropdown

## Query Audit — Object Explorer (single tab, 2min)

| Query | Count | Avg | Max | Errors |
|-------|-------|-----|-----|--------|
| healthCheck | 24 | 21ms | 123ms | 0 |
| rbacCheck | 2 | 153ms | 258ms | 0 |
| canCreateObjects | 1 | 75ms | 75ms | 0 |
| databaseDetails | 1 | 22ms | 22ms | 0 |

**Total:** 29 requests, 0 failures, peak concurrent: 4

### Key observations
- Lightest page by far — 29 requests vs 72 for cluster detail
- Tree data comes entirely from SUBSCRIBE (useAllObjects, useAllNamespaces) — no SQL polling
- Only global queries fire: healthCheck (24 calls), rbacCheck (2 calls)
- `databaseDetails` fires once when the page loads with a database selected
- No page-specific polling — ideal scalability profile

## Optimization Targets (ranked by impact)

1. **`clustersList` polling** — 129 calls in 6-tab test, max 7s. Migrating to SUBSCRIBE (`useAllClusters`) would collapse to ~6 calls
2. **`largestMaintainedQueries`** — slowest single query at 3.1s (5.1s under load). Uses session variables. Unified `mz_object_arrangement_sizes` table would eliminate session vars
3. **`healthCheck` volume** — 173 calls in 6-tab test, 4.5s queue time. SUBSCRIBE-derived health would reduce to ~6 calls
4. **`largestClusterReplica`** — 1.9s max, could be derived client-side from already-loaded cluster data
5. **`rbacCheck` polling** — polls every 5s per tab. Could use `staleTime: Infinity` since permissions rarely change

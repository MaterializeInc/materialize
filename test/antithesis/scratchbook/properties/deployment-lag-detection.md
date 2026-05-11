# deployment-lag-detection

## Summary
0DT caught-up check eventually detects lagging or crash-looping replicas and blocks promotion.

## Evidence

### Code Paths
- `src/adapter/src/coord/caught_up.rs:53-150` — `maybe_check_caught_up` with replica frontier snapshot
- `src/adapter/src/coord/caught_up.rs:127-136` — Lag comparison against allowed threshold
- `src/adapter/src/coord/caught_up.rs:145-149` — `problematic_replicas` detection
- Dynamic configs: `WITH_0DT_CAUGHT_UP_CHECK_ALLOWED_LAG`, `ENABLE_0DT_CAUGHT_UP_REPLICA_STATUS_CHECK`

### How It Works
Periodically during catchup, the coordinator queries `MZ_CLUSTER_REPLICA_FRONTIERS` and compares each replica's frontier against the expected threshold. If any replica's frontier lags beyond `allowed_lag`, promotion is blocked. Additionally, `analyze_replica_looping()` checks `mz_cluster_replica_status_history` for crash patterns.

### What Goes Wrong on Violation
If a stuck/crashing replica is not detected, promotion proceeds with an unhealthy replica. Post-promotion, queries routed to that replica fail or return stale results.

### SUT-Side Instrumentation Notes
- No existing Antithesis assertions
- Candidate: Add `assert_sometimes!(lagging_replica_blocked_promotion)` to confirm the detection path is exercised
- This is a liveness property — we want to confirm the system can detect the problem, not just that it doesn't happen

### Provenance
Surfaced by Lifecycle focus.

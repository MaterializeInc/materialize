# deployment-promotion-safety

## Summary
0DT deployment promotion happens only after all replicas have caught up to required frontiers.

## Evidence

### Code Paths
- `src/environmentd/src/deployment/state.rs:92-108` — `set_ready_to_promote` transitions Initializing->CatchingUp->ReadyToPromote
- `src/environmentd/src/deployment/preflight.rs:57-120` — `preflight_0dt` with `caught_up_max_wait` and `caught_up_trigger`
- `src/adapter/src/coord/caught_up.rs:53-150` — Replica frontier checks via `MZ_CLUSTER_REPLICA_FRONTIERS`
- `src/catalog/src/durable/error.rs:115-124` — `FenceError::DeployGeneration`

### How It Works
During 0DT deployment, the new coordinator boots in read-only mode. It runs preflight checks including `maybe_check_caught_up()` which compares replica frontiers against a cutoff threshold. Only after all replicas pass the check does the coordinator transition to ReadyToPromote. On promotion, the deployment generation is incremented, fencing out the old coordinator.

### What Goes Wrong on Violation
Premature promotion causes the new coordinator to serve queries while replicas are still rehydrating from storage. Users see stale data or timeouts. In the worst case, the old coordinator continues writing with a lower generation, causing split-brain.

### SUT-Side Instrumentation Notes
- No existing Antithesis assertions
- Candidate: At promotion time, add `assert_always!` that all tracked replica frontiers >= cutoff
- Candidate: Add `assert_reachable!("0dt_promotion_completed")` to confirm the promotion path is exercised

### Provenance
Surfaced by Lifecycle and Distributed Coordination focuses.

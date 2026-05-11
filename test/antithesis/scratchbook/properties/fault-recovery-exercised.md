# fault-recovery-exercised

## Summary
After coordinator crash, the system eventually recovers and serves queries.

## Evidence

### Code Paths
- `src/environmentd/src/environmentd/main.rs` — Main startup, catalog recovery
- `src/environmentd/src/http/probe.rs` — `/health/ready` endpoint
- `src/catalog/src/durable/persist.rs:1166-1192` — `open_inner` recovery path

### How It Works
On restart, environmentd re-reads the catalog from persist, increments the epoch, rehydrates compute/storage clusters, and starts accepting connections. The readiness probe (`/health/ready`) returns 200 only after the adapter is fully initialized.

### What Goes Wrong on Violation
The system fails to recover: it crashes on startup due to corrupt catalog state, enters an infinite restart loop, or becomes ready but cannot serve queries due to incomplete rehydration.

### Why This Is a Property
This is the most fundamental liveness property. It doesn't test a specific invariant — it tests that the entire recovery pipeline works end-to-end under adversarial crash timing.

### SUT-Side Instrumentation Notes
- No existing Antithesis assertions
- Best verified at workload level: crash environmentd, wait for readiness, issue SELECT query, assert success
- Candidate: Add `assert_sometimes!(recovery_completed_successfully)` after catalog recovery succeeds

### Provenance
Surfaced by Failure Recovery focus.

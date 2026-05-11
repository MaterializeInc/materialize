# mv-reflects-source-updates

## Summary
Materialized views eventually reflect changes to their source data.

## Evidence

### Code Paths
- `src/compute/src/render/` — Dataflow rendering for materialized views
- `src/compute/src/server.rs` — Compute server receives commands and renders dataflows
- `src/adapter/src/coord/sequencer/` — CREATE MATERIALIZED VIEW sequencing

### How It Works
When source data changes, differential dataflow operators in the compute layer process the deltas and update the materialized view's persist shard. The MV's frontier advances as updates are committed.

### What Goes Wrong on Violation
MVs show stale data permanently despite source updates. Users query a materialized view expecting fresh data and get results that never update. This is the core value proposition failure.

### Why This Is an End-to-End Property
Unlike internal properties (epoch fencing, CaS monotonicity), this property is directly observable by users. It combines source ingestion, compute processing, and persist writes into a single check.

### Workload Verification
1. INSERT INTO table1 VALUES (1, 'test')
2. Wait for MV that SELECTs from table1
3. SELECT * FROM mv1 — must eventually contain (1, 'test')

### SUT-Side Instrumentation Notes
- Best verified at workload level via SQL assertions
- Candidate: Add `assert_sometimes!(mv_frontier_advanced)` in the compute persist sink

### Provenance
Surfaced by Product Context focus.

# parallel-workload-no-unexpected-errors

## Summary
Randomized concurrent SQL against a shared pool of catalog objects should not
produce unexpected query failures, even while Antithesis injects coordinator and
replica faults.

## Evidence

### Code Paths
- `src/adapter/src/coord/sequencer/` — concurrent DDL/DML sequencing and catalog transactions
- `src/catalog/src/durable/` — catalog state persistence and recovery across restarts
- `src/compute/src/` — materialized-view rendering and execution after concurrent DDL

### How It Works
The Antithesis workload uses a fixed shared schema and a small pool of tables
and materialized views. Multiple worker threads repeatedly race `CREATE`,
`DROP`, `INSERT`, `UPDATE`, `DELETE`, and `SELECT` against those objects. This
deliberately forces the coordinator through concurrent catalog changes while the
Antithesis fault injector pauses or restarts components underneath it.

### What Goes Wrong on Violation
Unexpected SQL failures here usually mean a concurrency bug in catalog
sequencing, plan invalidation, or recovery. The workload already tolerates the
expected race outcomes like "object was dropped" or "concurrent catalog
modification"; what remains should be a real bug or an unclassified new failure
mode worth triage.

### Workload Verification
1. Ensure the shared schema exists
2. Spawn multiple worker threads
3. Randomly issue DDL/DML/SELECT against a fixed object pool
4. Count expected race/drop errors separately
5. Assert that no other SQL error escapes

### SUT-Side Instrumentation Notes
- Best primary signal is workload-side because the interesting failures are
  externally visible query errors, not one specific internal assertion site
- Candidate follow-up: add targeted SUT-side assertions for catalog invalidation
  and dropped-object dependency paths once a concrete failure mode is found

### Provenance
Adapted from the existing `test/parallel-workload/mzcompose.py` randomized SQL
stress test into the Antithesis workload model.

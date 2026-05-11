# epoch-fencing-prevents-split-brain

## Summary
Epoch-based leader fencing prevents two coordinators from concurrently writing to the catalog persist shard.

## Evidence

### Code Paths
- `src/catalog/src/durable/persist.rs:149-169` — `FenceableToken::validate()` and `maybe_fence()` check epoch on every write
- `src/catalog/src/durable/persist.rs:393-461` — `compare_and_append` with fence validation before consensus write
- `src/catalog/src/durable/error.rs:114-131` — `FenceError` enum: `DeployGeneration` and `Epoch` variants
- `src/catalog/src/durable/persist.rs:1166-1192` — Fence token generation during `open_inner`
- `src/environmentd/src/deployment/state.rs:24-123` — Deployment state machine transitions

### How It Works
On startup, the coordinator reads the current fence token from consensus and increments the epoch. The new token is written via CaS. All subsequent writes include the token; if consensus contains a higher epoch, the write fails with `FenceError::Epoch`.

### What Goes Wrong on Violation
Two coordinators with the same epoch could both write catalog mutations, leading to divergent schema state. Users would see inconsistent table definitions, lost DDL operations, or catalog corruption requiring manual intervention.

### Failure Scenario
1. Coordinator A is running with epoch 10
2. Coordinator A becomes partitioned from consensus
3. Coordinator B starts, reads epoch 10, increments to epoch 11
4. Partition heals; A attempts to write with epoch 10
5. **Expected**: A's write fails with FenceError
6. **Bug**: If A's CaS succeeds despite lower epoch (race in validation)

### SUT-Side Instrumentation Notes
- No existing Antithesis assertions in codebase
- Candidate instrumentation point: `FenceableToken::validate()` — add `assert_always!` that validates token comparison result matches expected fencing behavior
- Candidate: `compare_and_append` success path — add `assert_always!` that current_epoch >= write_epoch

### Provenance
Surfaced independently by Distributed Coordination and Failure Recovery focuses.

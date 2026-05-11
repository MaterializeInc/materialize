# idempotent-write-under-indeterminate

## Summary
Compare-and-append retries with the same idempotency token produce exactly one committed write — never duplicates, never loss.

## Evidence

### Code Paths
- `src/persist-client/src/internal/machine.rs:387-468` — Detailed comments on Indeterminate error handling and retry-with-idempotency-token
- `src/persist-client/src/internal/state.rs:1687` — `compare_and_append` function
- `src/persist-client/src/write.rs:409` — Retry wrapper with `IdempotencyToken`
- `src/persist-client/src/internal/state.rs:1715-1724` — Writer state and lease tracking

### How It Works
Each writer holds an `IdempotencyToken`. On Indeterminate error, the retry includes the same token. The state machine checks if a write with that token already succeeded (checking writer state). If so, it returns `AlreadyCommitted`. If not, it proceeds normally.

### What Goes Wrong on Violation
Duplicate writes: the shard contains two copies of the same batch, leading to double-counting in materialized views. Or lost writes: the batch is neither committed nor retried successfully, causing data loss.

### Key Subtlety
The comments at machine.rs:387-468 describe subtle scenarios where the writer must distinguish between "my write succeeded but I didn't get the ack" vs "my write failed and I need to retry." The IdempotencyToken is the mechanism, but the window between consensus write and state observation is where bugs hide.

### SUT-Side Instrumentation Notes
- No existing Antithesis assertions
- Candidate: After Indeterminate retry, add `assert_always!` that shard trace contains exactly one instance of the batch

### Provenance
Surfaced by Data Integrity focus.

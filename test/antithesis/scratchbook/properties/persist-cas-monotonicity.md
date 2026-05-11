# persist-cas-monotonicity

## Summary
Persist shard state versions (SeqNo) must never decrease across any observation point.

## Evidence

### Code Paths
- `src/persist-client/src/internal/state_versions.rs:48-87` — State version invariants: `earliest <= current.seqno`
- `src/persist-client/src/internal/state.rs:84-95` — `ROLLUP_THRESHOLD` and seqno-based rollup logic
- `src/persist-client/src/internal/state.rs:1324` — Invariant comment on rollup seqno
- `src/persist-client/src/internal/gc.rs` — GC respects seqno ordering
- `src/persist-client/src/write.rs:70-123` — WriteHandle CaS loop context

### How It Works
Every state mutation increments SeqNo. The CaS loop in Machine reads current state, computes new state with SeqNo+1, and atomically writes via consensus. If another writer interleaved, the CaS fails and the writer retries with the newer SeqNo. Rollups periodically snapshot state; rollup seqno must be <= current seqno.

### What Goes Wrong on Violation
SeqNo regression means state reconstruction from rollup + diffs produces wrong state. GC could delete diffs that are still needed. Writers could overwrite each other's changes. This is a data corruption scenario.

### Failure Scenario
1. Writer A reads state at SeqNo 100, begins computing new state
2. Writer B reads state at SeqNo 100, writes SeqNo 101
3. Writer A attempts to write SeqNo 101 — CaS should fail (current is now 101)
4. **Expected**: A retries, reads SeqNo 101, writes SeqNo 102
5. **Bug**: If CaS comparison is stale and A's write at 101 succeeds despite B's 101

### SUT-Side Instrumentation Notes
- No existing Antithesis assertions
- Candidate: `Machine::apply_unbatched_cmd` — add `assert_always!(new_seqno > old_seqno)` after every state transition
- Candidate: State reconstruction from rollup + diffs — add `assert_always!` that reconstructed state matches expected

### Provenance
Surfaced by Data Integrity and Distributed Coordination focuses.

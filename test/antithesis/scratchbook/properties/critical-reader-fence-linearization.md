# critical-reader-fence-linearization

## Summary
Critical reader opaque token comparison linearizes correctly — concurrent readers cannot bypass the fencing mechanism.

## Evidence

### Code Paths
- `src/persist-client/src/internal/state.rs:1937-1979` — `compare_and_downgrade_since()` with opaque fencing
- `src/persist-client/src/critical.rs` — `CriticalReaderId` and `Opaque` definitions

### How It Works
Critical readers hold a `since` frontier that prevents GC of data at held timestamps. The `compare_and_downgrade_since` operation uses an opaque token to fence: the caller provides `expected_opaque`, and if it doesn't match the current opaque in state, the operation fails (but still commits a SeqNo increment to prevent ABA). Only the caller with the correct opaque can advance the since.

### What Goes Wrong on Violation
If fencing is bypassed, two readers could both think they hold the since, leading to premature GC. Data needed by active readers is deleted, causing read failures or panics.

### SUT-Side Instrumentation Notes
- No existing Antithesis assertions
- Candidate: After successful downgrade, add `assert_always!(state.opaque == my_opaque)` to confirm fencing
- Candidate: On mismatch, add `assert_always!(seqno_advanced)` to confirm ABA prevention

### Provenance
Surfaced by Data Integrity focus.

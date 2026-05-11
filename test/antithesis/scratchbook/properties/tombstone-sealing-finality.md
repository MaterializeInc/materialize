# tombstone-sealing-finality

## Summary
Once a shard is tombstoned (upper and since both empty antichain), no further mutations are possible.

## Evidence

### Code Paths
- `src/persist-client/src/internal/state.rs:2128-2134` — `is_tombstone()` checks upper.is_empty() && since.is_empty() && writers.is_empty() && critical_readers.is_empty()
- `src/persist-client/src/internal/state.rs:1703-1712` — compare_and_append short-circuits on tombstone
- `src/persist-client/src/internal/state.rs:2146-2159` — `become_tombstone_and_shrink()` transition

### What Goes Wrong on Violation
If a tombstoned shard accepts new writes, deleted tables/views could have data resurrected. This would confuse users and violate the contract that DROP TABLE removes data permanently.

### SUT-Side Instrumentation Notes
- No existing Antithesis assertions
- Candidate: After `is_tombstone()` returns true, add `assert_always!` that subsequent append attempts return error
- Candidate: `become_tombstone_and_shrink()` — add `assert_unreachable!` after the transition if any subsequent mutation succeeds

### Provenance
Surfaced by Data Integrity focus.

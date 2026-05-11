# catalog-recovery-consistency

## Summary
After coordinator crash and restart, the catalog state is consistent: upper never decreases, snapshot is consolidated, all committed transactions visible.

## Evidence

### Code Paths
- `src/catalog/src/durable/persist.rs:536-539` — `sync_to_current_upper`
- `src/catalog/src/durable/persist.rs:575-577` — ListenEvent::Progress antichain logic
- `src/catalog/src/durable/persist.rs:706-724` — `consolidate` method
- `src/catalog/src/durable/persist.rs:593-612` — sync applies updates by timestamp, consolidates after each
- `src/catalog/src/durable/persist.rs:1092` — Assertion on snapshot consolidation
- `src/catalog/src/durable/persist.rs:1167-1170` — Fence token generation syncs to upper

### How It Works
On startup, the coordinator reads the persist shard from the latest rollup + incremental diffs, reconstructing the full catalog state. `sync_to_current_upper()` applies all updates up to the current upper antichain and consolidates the snapshot. The existing code has a debug assertion at line 1092 checking consolidation.

### What Goes Wrong on Violation
- Upper regression: coordinator sees older schema state than what was committed, losing recent DDL
- Unconsolidated snapshot: duplicate entries cause incorrect catalog lookups, potential panics
- Missing transactions: committed DDL not visible after restart, users lose tables/views

### Key Subtlety
Crash during `maybe_consolidate()` (lines 596, 610) could leave the snapshot in an intermediate state. On restart, the next sync must handle this gracefully by reconsolidating from the durable upper.

### SUT-Side Instrumentation Notes
- No existing Antithesis assertions (debug_assert exists at line 1092 but only in debug builds)
- Candidate: After `sync_to_current_upper()`, add `assert_always!` that upper >= previous upper
- Candidate: After consolidation, add `assert_always!` that no duplicate (kind, key) entries exist

### Provenance
Surfaced by Failure Recovery focus (merged from catalog-upper-monotonicity and catalog-snapshot-consolidation).

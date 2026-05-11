# group-commit-toctou-safety

## Summary
No phantom writes to tables deleted between write deferral and group_commit execution.

## Evidence

### Code Paths
- `src/adapter/src/coord/appends.rs:479-486` — Explicit TOCTOU check: "If the table... has been deleted while the write was deferred"
- `src/adapter/src/coord/appends.rs:214-216` — `defer_op` enqueue point
- `src/adapter/src/coord/appends.rs:394-399` — JIT lock acquisition in group_commit

### How It Works
When a write arrives and cannot immediately acquire the write lock, it is deferred. Later, group_commit processes deferred writes. Before applying each write, it checks `catalog().try_get_entry(table_id)`. If the table was dropped between deferral and execution, the write is silently dropped.

### What Goes Wrong on Violation
Writes land in a shard for a table that no longer exists in the catalog. This causes inconsistency between the catalog (table doesn't exist) and persist (shard has data). Downstream queries may panic or return garbage.

### The TOCTOU Window
The explicit comment at appends.rs:479 acknowledges the race. The window is between line 214 (write enqueued) and line 484 (catalog check during group_commit). Concurrent DDL (DROP TABLE) within this window is the trigger.

### SUT-Side Instrumentation Notes
- No existing Antithesis assertions
- Candidate: After group_commit drops a deferred write, add `assert_reachable!("group_commit_dropped_deferred_write_to_deleted_table")` to confirm this path is exercised
- Candidate: After group_commit succeeds, add `assert_always!` that all written table_ids still exist in catalog

### Provenance
Surfaced by Concurrency focus.

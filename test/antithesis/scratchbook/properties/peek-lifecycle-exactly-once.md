# peek-lifecycle-exactly-once

## Summary
Each peek command produces exactly one response — no duplicates, no leaks, no orphaned state.

## Evidence

### Code Paths
- `src/adapter/src/coord/peek.rs:80-95` — Explicit "1:1 contract between Peek and PeekResponseUnary" comment
- `src/adapter/src/coord/peek.rs:873-920` — Response routing with UUID tracking
- `src/adapter/src/coord/peek.rs:1174-1209` — `cancel_pending_peeks`: removes from client_pending_peeks then pending_peeks
- `src/adapter/src/coord/peek.rs:1256-1268` — `remove_pending_peek`: consistency check between two maps
- `src/adapter/src/coord/peek.rs:1221-1227` — `handle_peek_notification` removes before response

### How It Works
Peeks are tracked in two maps: `pending_peeks` (UUID -> PendingPeek) and `client_pending_peeks` (ConnectionId -> Set<UUID>). On response or cancellation, the peek is removed from both maps. Each UUID is unique (generated per-peek).

### What Goes Wrong on Violation
- Leaked peeks: UUID stays in pending_peeks forever, growing memory until OOM
- Duplicate responses: client receives two result sets for one query
- Missing responses: client hangs waiting for a peek that was silently dropped

### The Race Condition
The two-map removal (client_pending_peeks + pending_peeks) at lines 1256-1268 is not atomic. If CancelPendingPeeks races with PeekNotification:
1. Cancel removes UUID from client_pending_peeks
2. Peek response arrives, finds UUID in pending_peeks but not in client_pending_peeks
3. Orphaned state or double-processing

### SUT-Side Instrumentation Notes
- No existing Antithesis assertions
- Candidate: At coordinator shutdown or periodically, add `assert_always!(pending_peeks.is_empty() || active_connections_exist)` to detect leaks
- Candidate: On peek response, add `assert_always!` that UUID existed in pending_peeks before removal

### Provenance
Surfaced by Protocol Contracts and Concurrency focuses.

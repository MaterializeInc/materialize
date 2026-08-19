---
source: src/adapter/src/coord/read_then_write.rs
revision: 39dcae2fba
---

# adapter::coord::read_then_write

Provides coordinator-side support machinery for read-then-write DML operations (INSERT, UPDATE, DELETE).

`validate_read_then_write_dependencies` walks the transitive catalog dependency graph seeded by `ids: impl IntoIterator<Item = CatalogItemId>` and verifies that every reachable object is safe for use in a read-then-write operation. The traversal is iterative (worklist-based), not recursive, because dependency chains are user-controlled and can be arbitrarily deep; recursion would risk a stack overflow on the coordinator thread. A `seen` set deduplicates objects so diamond-shaped graphs are validated once per object. The traversal is bounded by `max_rw_dependencies: usize`; exceeding that limit returns `AdapterError::ReadThenWriteDependencyLimitExceeded { max_rw_dependencies }`.
An object is considered invalid if it is a source, secret, or connection; a non-user table (including system tables) or a source-export table; or a system view or system materialized view.
Additionally, any view or materialized view whose optimized expression contains a call to `mz_now()` is rejected, because the timestamp produced during the read phase would differ from the write timestamp and could yield inconsistent results.
User tables (that are not source exports), user-defined views, user-defined materialized views, functions, and types are accepted.
On failure, the function returns `AdapterError::Unsupported` (for `mz_now()` usage), `AdapterError::ReadThenWriteDependencyLimitExceeded` (when the dependency bound is exceeded), or `AdapterError::InvalidTableMutationSelection` with the offending object's name and type.

`Coordinator::handle_create_internal_subscribe` creates a subscribe that introspection does not see (`internal: true` on `ActiveSubscribe`). It takes ownership of `read_holds` and drops them only after the dataflow is shipped, preventing the `since` from advancing past `as_of` in the interim. The dataflow is shipped via `try_ship_dataflow` before the sink is registered, so a failure (e.g. a dependency dropped since optimization) leaves nothing to unwind. Results are delivered through a `response_tx` oneshot; if the receiver is gone when the handler runs, the internal subscribe is immediately retired via `drop_internal_subscribe`. `Coordinator::drop_internal_subscribe` cancels the dataflow on the compute side by delegating to `drop_compute_sink`.
`Coordinator::handle_attempt_write` enqueues a write attempt from the frontend read-then-write path. It checks that the connection is still active and the coordinator is not in read-only mode, then validates the target's current `GlobalId` generation against the caller's `target_global_id`; a generation mismatch sends `WriteResult::TargetChanged`. When `write_ts` is `Some`, the write is submitted as a `TimestampedWriteRequest` via the group committer; when `None`, it rides the next group commit as a blind write.

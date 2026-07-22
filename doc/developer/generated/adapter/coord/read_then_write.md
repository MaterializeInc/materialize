---
source: src/adapter/src/coord/read_then_write.rs
revision: 3713eed996
---

# adapter::coord::read_then_write

Provides coordinator-side support machinery for read-then-write DML operations (INSERT, UPDATE, DELETE).

`validate_read_then_write_dependencies` walks the transitive catalog dependency graph seeded by `ids: impl IntoIterator<Item = CatalogItemId>` and verifies that every reachable object is safe for use in a read-then-write operation. The traversal is iterative (worklist-based), not recursive, because dependency chains are user-controlled and can be arbitrarily deep; recursion would risk a stack overflow on the coordinator thread. A `seen` set deduplicates objects so diamond-shaped graphs are validated once per object. The traversal is bounded by `max_rw_dependencies: usize`; exceeding that limit returns `AdapterError::ReadThenWriteDependencyLimitExceeded { max_rw_dependencies }`.
An object is considered invalid if it is a source, secret, or connection; a non-user table (including system tables) or a source-export table; or a system view or system materialized view.
Additionally, any view or materialized view whose optimized expression contains a call to `mz_now()` is rejected, because the timestamp produced during the read phase would differ from the write timestamp and could yield inconsistent results.
User tables (that are not source exports), user-defined views, user-defined materialized views, functions, and types are accepted.
On failure, the function returns `AdapterError::Unsupported` (for `mz_now()` usage), `AdapterError::ReadThenWriteDependencyLimitExceeded` (when the dependency bound is exceeded), or `AdapterError::InvalidTableMutationSelection` with the offending object's name and type.

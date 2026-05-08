---
source: src/adapter/src/coord/read_then_write.rs
revision: e7ac38b338
---

# adapter::coord::read_then_write

Provides coordinator-side support machinery for read-then-write DML operations (INSERT, UPDATE, DELETE).

`validate_read_then_write_dependencies` recursively walks the catalog dependency graph rooted at a given `CatalogItemId` and verifies that every object reachable from it is safe for use in a read-then-write operation.
An object is considered invalid if it is a source, secret, or connection; a non-user table (including system tables) or a source-export table; or a system view or system materialized view.
Additionally, any view or materialized view whose optimized expression contains a call to `mz_now()` is rejected, because the timestamp produced during the read phase would differ from the write timestamp and could yield inconsistent results.
User tables (that are not source exports), user-defined views, user-defined materialized views, functions, and types are accepted.
On failure, the function returns `AdapterError::Unsupported` (for `mz_now()` usage) or `AdapterError::InvalidTableMutationSelection` with the offending object's name and type.

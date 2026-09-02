---
source: src/ore/src/stack.rs
revision: 67c62b92f4
---

# mz-ore::stack

Provides utilities for safe recursive code: stack growth and bounded-recursion enforcement.
`maybe_grow` wraps `stacker::maybe_grow` using the module-level `STACK_RED_ZONE` and `STACK_SIZE` constants (which are larger in debug builds to account for unoptimized stack frames).
`CheckedRecursion` is a trait that can be implemented by any context type embedding a `RecursionGuard`; its `checked_recur` / `checked_recur_mut` methods increment a depth counter, call `maybe_grow`, and return a `RecursionLimitError` once the configured limit is exceeded.
`RecursionGuard` holds the mutable depth counter and limit, and `RecursionLimitError` is the corresponding error type.
`RecursionLimitError` carries an optional numeric id (`Option<u64>`). When the `tracing` feature is enabled, `RecursionLimitError::new` captures a backtrace, emits it to the error log via `tracing::error!` tagged with the id, and stores that id in the struct. Its `Display` implementation prints the limit and, when an id is present, a reference to the corresponding log entry (e.g. `backtrace id #N in logs`).

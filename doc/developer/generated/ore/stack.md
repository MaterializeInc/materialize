---
source: src/ore/src/stack.rs
revision: 0094e55a71
---

# mz-ore::stack

Provides utilities for safe recursive code: stack growth and bounded-recursion enforcement.
`maybe_grow` wraps `stacker::maybe_grow` using the module-level `STACK_RED_ZONE` and `STACK_SIZE` constants (which are larger in debug builds to account for unoptimized stack frames).
`CheckedRecursion` is a trait that can be implemented by any context type embedding a `RecursionGuard`; its `checked_recur` / `checked_recur_mut` methods increment a depth counter, call `maybe_grow`, and return a `RecursionLimitError` once the configured limit is exceeded.
`RecursionGuard` holds the mutable depth counter and limit, and `RecursionLimitError` is the corresponding error type.
`RecursionLimitError` carries a `Backtrace` field (captured via `Backtrace::force_capture` at the point the limit is exceeded) and its `Display` implementation prints both the limit and the captured backtrace. Because `Backtrace` is not `Clone`, `RecursionLimitError` does not implement `Clone`.

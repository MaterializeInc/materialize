---
source: src/ore/src/assert.rs
revision: 5627c007af
---

# mz-ore::assert

Provides soft assertions — runtime-togglable checks that either panic (when enabled) or log an error (when disabled) — plus a set of ergonomic hard-assertion macros.

The global `SOFT_ASSERTIONS` atomic flag defaults to `true` in debug builds or when `MZ_SOFT_ASSERTIONS=1`; each soft-assert macro checks this flag with a single relaxed atomic load.
The `_or_log` variants (`soft_assert_or_log`, `soft_assert_eq_or_log`, `soft_assert_ne_or_log`, `soft_panic_or_log`) are preferred in production because they surface failures via `tracing::error!` instead of silently passing.
The `_no_log` variants and `soft_panic_no_log` are silent in production and exist for hot paths where even logging overhead is unacceptable.

Hard-assertion macros (`assert_contains!`, `assert_none!`, `assert_ok!`, `assert_err!`) augment the standard `assert!` family with descriptive panic messages that include the actual values, making test failures easier to diagnose.

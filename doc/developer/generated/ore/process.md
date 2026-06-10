---
source: src/ore/src/process.rs
revision: 2006447a62
---

# mz-ore::process

Provides process-exit utilities for Materialize services.
`exit_thread_safe` calls `libc::_exit` instead of `std::process::exit` to avoid the thread-safety issues of the C `exit` function.
The `halt!` macro (requires the `tracing` feature) logs a warning and exits with code 166, intended for non-crash shutdowns such as leadership loss or deferrable retries; `exit!` logs at info level and exits with a caller-supplied code.

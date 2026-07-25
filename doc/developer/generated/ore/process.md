---
source: src/ore/src/process.rs
revision: 445c5ce0f4
---

# mz-ore::process

Provides process-exit and thread-naming utilities for Materialize services.
`exit_thread_safe` calls `libc::_exit` instead of `std::process::exit` to avoid the thread-safety issues of the C `exit` function.
The `halt!` macro (requires the `tracing` feature) logs a warning and exits with code 166, intended for non-crash shutdowns such as leadership loss or deferrable retries; `exit!` logs at info level and exits with a caller-supplied code.
`set_current_thread_name` sets the OS-level name of the calling thread, visible to tools like `top -H`, `perf`, and `/proc/<pid>/task/*/comm`; on Linux, names are truncated to 15 bytes to fit the kernel limit. The function is a no-op on non-Linux platforms.

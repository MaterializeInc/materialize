---
source: src/ore/src/panic.rs
revision: dfc654b386
---

# mz-ore::panic

Provides an enhanced panic handler and compatible catch-unwind utilities.
`install_enhanced_handler` replaces the default Rust panic hook with one that always captures a backtrace, reports the panic to Sentry (skipping panics caught by `catch_unwind`), writes the panic message to stderr as atomically as possible, and then aborts the entire process.
`catch_unwind` and `catch_unwind_str` are drop-in replacements for `std::panic::catch_unwind` that cooperate with the enhanced handler by incrementing a thread-local (and optional async task-local) counter that suppresses the abort path.

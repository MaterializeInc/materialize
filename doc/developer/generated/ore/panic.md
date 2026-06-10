---
source: src/ore/src/panic.rs
revision: 09fe656a55
---

# mz-ore::panic

Provides an enhanced panic handler and compatible catch-unwind utilities.
`install_enhanced_handler` replaces the default Rust panic hook with one that always captures a backtrace, reports the panic to Sentry (skipping panics caught by `catch_unwind`), writes the panic message to stderr as atomically as possible, and then aborts the entire process.
`catch_unwind` and `catch_unwind_str` are drop-in replacements for `std::panic::catch_unwind` that cooperate with the enhanced handler by incrementing a thread-local (and optional async task-local) counter that suppresses the abort path.
`catch_unwind_with_details` extends `catch_unwind` by also recovering the panic's source location and a full backtrace captured at the panic site (via `Backtrace::force_capture` in the enhanced handler). It returns a `CaughtPanic` struct bundling the `message`, `location`, and `backtrace` fields. The location and backtrace are only present when `install_enhanced_handler` has been called; the message is always recovered. The enhanced handler stashes these details in thread-local storage (`CAUGHT_PANIC_DETAILS`) when the `CAPTURE_PANIC_DETAILS` counter (incremented by `catch_unwind_with_details`) is non-zero.

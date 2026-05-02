---
source: src/ore-proc/src/test.rs
revision: 7c2004f36c
---

# ore-proc::test

Implements the `#[test]` procedural macro attribute, which wraps standard (and async) Rust test functions to automatically initialize Materialize's logging infrastructure before the test body runs.
The implementation is based on the `test-log` crate and handles both sync and async test functions, forwarding optional inner test attributes (e.g., `tokio::test`).
It detects whether it is compiling inside the `mz-ore` crate itself and adjusts the logging-init call path accordingly.

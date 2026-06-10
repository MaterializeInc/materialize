---
source: src/ore-proc/src/test.rs
revision: c2bf2758d8
---

# ore-proc::test

Implements the `#[test]` procedural macro attribute, which wraps standard (and async) Rust test functions to automatically initialize Materialize's logging infrastructure before the test body runs.
The implementation is based on the `test-log` crate and handles both sync and async test functions, forwarding optional inner test attributes (e.g., `tokio::test`).
It detects whether it is compiling as the `mz-ore` library target (versus an integration-test or bench binary within the same package) by checking both `CARGO_PKG_NAME` (must be `mz-ore`) and `CARGO_CRATE_NAME` (must be `mz_ore`). The library target uses `crate::test` for the logging-init path; all other compilation units within the package route through `::mz_ore::test`.

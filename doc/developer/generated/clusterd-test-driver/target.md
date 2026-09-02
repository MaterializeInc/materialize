---
source: src/clusterd-test-driver/src/target.rs
revision: 6d4c0fbb2b
---

# mz-clusterd-test-driver::target

Resolves the clusterd compute controller address.

`compute_addr()` returns the value of `CLUSTERD_COMPUTE_ADDR` (defaulting to `127.0.0.1:2101`).

`should_run_e2e_test()` returns `true` only when `CLUSTERD_COMPUTE_ADDR` is explicitly set, so end-to-end tests that require an external clusterd process are skipped in CI environments that don't provide one.

The driver always connects to an externally-managed clusterd (via mzcompose or a manually-launched process); it never spawns one itself.

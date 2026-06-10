---
source: src/ore/src/test.rs
revision: e757b4d11b
---

# mz-ore::test

Provides test-only utilities for logging initialization and function timeouts.
`init_logging` and `init_logging_default` initialize a `tracing_subscriber` global logger once (idempotent), respecting the `MZ_TEST_LOG_FILTER` environment variable.
`timeout` runs a closure on a separate thread and returns its result if it completes within the given `Duration`, propagates any panic, or returns an error if the thread does not finish in time; the underlying thread is not forcibly killed on timeout.

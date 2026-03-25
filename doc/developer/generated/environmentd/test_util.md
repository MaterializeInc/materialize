---
source: src/environmentd/src/test_util.rs
revision: 03db92b55b
---

# environmentd::test_util

Provides test infrastructure for integration tests against a full `environmentd` process.
Exposes `TestHarness` (builder for configuring a test server), `TestServerWithRuntime` (the running server plus Tokio runtime), and many helpers for connecting via PostgreSQL, HTTP, and WebSocket; for inspecting catalog state; and for controlling timing.
Only compiled when the `test` feature is enabled.

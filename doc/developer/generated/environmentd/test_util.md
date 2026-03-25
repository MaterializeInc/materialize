---
source: src/environmentd/src/test_util.rs
revision: ec9d15901e
---

# environmentd::test_util

Provides test infrastructure for integration tests against a full `environmentd` process.
Exposes `TestHarness` (builder for configuring a test server with options for data directory, TLS, authentication, cluster replica sizes, system parameters, and propagation of persist/Kafka/schema-registry URLs), `TestServerWithRuntime` (the running server plus Tokio runtime), and many helpers for connecting via PostgreSQL, HTTP, and WebSocket; for inspecting catalog state; and for controlling timing.
Only compiled when the `test` feature is enabled.

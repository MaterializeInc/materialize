---
source: src/environmentd/src/test_util.rs
revision: ed7294b842
---

# environmentd::test_util

Provides test infrastructure for integration tests against a full `environmentd` process.
Exposes `TestHarness` (builder for configuring a test server with options for data directory, TLS, authentication, cluster replica sizes, builtin cluster replication factors, system parameters, propagation of persist/Kafka/schema-registry URLs, and forced builtin schema migration mechanism), `TestServerWithRuntime` (the running server plus Tokio runtime), and many helpers for connecting via PostgreSQL, HTTP, and WebSocket; for inspecting catalog state; and for controlling timing.
`TestHarness::with_force_builtin_schema_migration` accepts `"evolution"` or `"replacement"` and forces every builtin storage collection through that migration mechanism, enabling targeted testing of each code path.
Only compiled when the `test` feature is enabled.

---
source: src/frontegg-mock/src/middleware/latency.rs
revision: 8041e666f1
---

# frontegg-mock::middleware::latency

Defines `latency_middleware`, an axum middleware layer that sleeps for `Context.latency` before forwarding each request, simulating configurable network latency in tests.

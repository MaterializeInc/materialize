---
source: src/frontegg-mock/src/middleware/logging.rs
revision: 546314e4eb
---

# frontegg-mock::middleware::logging

Defines `logging_middleware`, an axum middleware layer that logs each request's HTTP method, URI, response status, and elapsed latency at INFO level via `tracing`.

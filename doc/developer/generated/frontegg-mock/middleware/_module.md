---
source: src/frontegg-mock/src/middleware.rs
revision: 546314e4eb
---

# frontegg-mock::middleware

Re-exports three axum middleware functions: `latency_middleware` (artificial request delay), `logging_middleware` (tracing-based request logging), and `role_update_middleware` (drains in-band role update channel before each request).
All three are stacked on the axum `Router` in `server.rs`.

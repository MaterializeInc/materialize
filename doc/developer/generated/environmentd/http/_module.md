---
source: src/environmentd/src/http.rs
revision: a6e32d4ab1
---

# environmentd::http

Implements `environmentd`'s embedded HTTP server using Axum.
Supports multiple authentication modes (`None`, `Password`, `Frontegg`, `OIDC`) via a layered middleware stack, manages session-based and per-request authentication, and routes requests across a rich set of endpoints: SQL execution (REST and WebSocket), Prometheus metrics scraping, memory/heap profiling, catalog and coordinator introspection, webhook ingestion, MCP (AI agent interface), cluster replica proxying, and internal deployment management.
Key types include `HttpServer`, `HttpConfig`, `AuthedClient`, `Metrics`, and `InternalRouteConfig`.

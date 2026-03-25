---
source: src/environmentd/src/http.rs
revision: 964fd6a0ab
---

# environmentd::http

Implements `environmentd`'s embedded HTTP server using Axum.
Supports multiple authentication modes (`None`, `Password`, `Frontegg`, `Oidc`) via a layered middleware stack, manages session-based and per-request authentication (including WebSocket credential exchange), and routes requests across a rich set of endpoints: SQL execution (REST and WebSocket), Prometheus metrics scraping, memory/heap profiling, catalog and coordinator introspection, webhook ingestion, MCP (AI agent interface), console proxy, cluster replica proxying, and internal deployment management.
Key types include `HttpServer`, `HttpConfig`, `AuthedClient`, `Metrics`, and `InternalRouteConfig`.

Submodules:

* `catalog` — catalog dump endpoints.
* `cluster` — cluster replica proxy.
* `console` — internal console proxy and unauthenticated console config.
* `mcp` — Model Context Protocol (AI agent) handlers.
* `memory` — memory/heap profiling endpoints.
* `metrics` — HTTP-level metrics collection.
* `metrics_viz` — metrics visualization.
* `probe` — liveness/readiness probes.
* `prometheus` — Prometheus metric query endpoints.
* `root` — root page and static file serving.
* `sql` — SQL execution over REST and WebSocket.
* `webhook` — webhook source ingestion.

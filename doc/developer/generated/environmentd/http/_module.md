---
source: src/environmentd/src/http.rs
revision: 33b8db85da
---

# environmentd::http

Implements `environmentd`'s embedded HTTP server using Axum.
Supports multiple authentication modes (`None`, `Password`, `Frontegg`, `Oidc`) via a layered middleware stack, manages session-based and per-request authentication (including WebSocket credential exchange), and routes requests across a rich set of endpoints: SQL execution (REST and WebSocket), Prometheus metrics scraping, memory/heap profiling, catalog and coordinator introspection, webhook ingestion, MCP (AI agent interface), audit event injection, console proxy, cluster replica proxying, and internal deployment management.
Key types include `HttpServer`, `HttpConfig`, `AuthedClient`, `Metrics`, and `InternalRouteConfig`.
`HttpConfig` carries `allowed_origin` (the `AllowOrigin` predicate for the CORS layer), `allowed_origin_list` (the raw `Vec<HeaderValue>` injected as an Axum `Extension` into the MCP router for server-side origin validation against DNS rebinding attacks), and `mcp_metrics` (a `McpMetrics` handle injected as an Axum `Extension` into the MCP router for Prometheus metrics tracking). The MCP CORS layer restricts allowed methods to POST and allowed headers to `Authorization` and `Content-Type`.
`AuthError` enumerates authentication failure modes; the `OidcFailed(String)` variant carries the sanitized `OidcError` display string, which is forwarded to the client so the console can surface OIDC sign-in failures on the login page.

Submodules:

* `catalog` — catalog dump and audit event injection endpoints.
* `cluster` — cluster replica proxy.
* `console` — internal console proxy and unauthenticated console config.
* `mcp` — Model Context Protocol (AI agent) handlers.
* `mcp_metrics` — Prometheus metrics for MCP HTTP endpoints.
* `memory` — memory/heap profiling endpoints.
* `metrics` — HTTP-level metrics collection.
* `metrics_public` — public metrics endpoint.
* `metrics_viz` — metrics visualization.
* `probe` — liveness/readiness probes.
* `prometheus` — Prometheus metric query endpoints.
* `root` — root page and static file serving.
* `sql` — SQL execution over REST and WebSocket.
* `webhook` — webhook source ingestion.

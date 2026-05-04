# environmentd::http

Embedded Axum HTTP server exposing SQL execution, observability, AI agent access, and internal operations over HTTP/WebSocket. Authentication middleware runs on every protected request and supports four modes (`None`, `Password`, `Frontegg`, `Oidc`).

## Files (LOC ≈ 5,716 across this directory + http.rs)

| File | What it owns |
|---|---|
| `http.rs` (parent) | Router construction, middleware stack, auth flow, `HttpServer`/`HttpConfig`/`AuthedClient`, TLS, session management |
| `sql.rs` | `execute_request` (shared REST + MCP dispatch loop); WebSocket upgrade handlers; `SqlResponse`/`SqlResult` JSON serialization; transaction + COPY lifecycle |
| `mcp.rs` | MCP JSON-RPC 2.0 endpoints (`/api/mcp/agent`, `/api/mcp/developer`); feature-flag gating; DNS-rebinding protection; AST-based read-only enforcement |
| `webhook.rs` | Inbound webhook source ingestion: `WebhookAppender` lookup, HMAC validation, row packing, storage append |
| `prometheus.rs` | Prometheus metric query endpoints; compute/storage/frontier/usage metric SQL queries |
| `metrics.rs` | HTTP-level request metrics collection |
| `cluster.rs` | Reverse proxy to `clusterd` internal HTTP (profiling, metrics, tracing); `handle_clusters` overview page |
| `console.rs` | Internal console proxy + unauthenticated console config endpoint |
| `catalog.rs` | Catalog dump + audit event injection |
| `probe.rs` | Liveness/readiness probes |
| `root.rs` | Root page + static file serving |
| `memory.rs` | Memory/heap profiling endpoints |
| `metrics_viz.rs` | Metrics visualization page |

## Key concepts

- **`AuthedClient`** — auth output; carries an initialized `SessionClient` into every handler.
- **`execute_request`** — single SQL dispatch loop shared by REST POST and MCP `query`/`read_data_product` tools; handles multi-statement, transaction lifecycle, and COPY.
- **Route gating** — `HttpRoutesEnabled` bitfield controls which endpoint groups each listener exposes (internal vs. external).
- **Deferred `adapter_client`** — HTTP servers boot before the adapter is initialized; handlers block on a `Shared<oneshot::Receiver<AdapterClient>>`.
- **MCP protocol** — implements MCP spec `2025-11-25`; two distinct tool surfaces (user data products vs. system catalog); per-tool flag gating via `dyncfg`.

## Cross-references

- Caller: `Listeners::serve` in `src/environmentd/src/lib.rs` constructs `HttpConfig` and calls `serve_http`.
- SQL session: `mz_adapter::SessionClient` is the adapter seam.
- Webhook storage: `mz_adapter::webhook::WebhookAppender`.
- `clusterd` proxy target: `mz_controller::ReplicaHttpLocator`.
- Generated developer docs: `doc/developer/generated/environmentd/http/`.

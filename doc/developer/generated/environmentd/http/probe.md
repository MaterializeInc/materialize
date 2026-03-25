---
source: src/environmentd/src/http/probe.rs
revision: e757b4d11b
---

# environmentd::http::probe

Implements `handle_ready`, the HTTP readiness probe endpoint (`GET /api/readyz`).
Returns `200 OK` once the adapter client is available, indicating the server is ready to serve queries; supports a `?wait=true` query parameter to block until readiness rather than returning `503` immediately.

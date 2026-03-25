---
source: src/http-util/src/lib.rs
revision: e757b4d11b
---

# mz-http-util

Provides shared utilities for running HTTP servers in Materialize.

## Module structure

The crate is a single `lib.rs` with no submodules.
It exports a collection of axum handler functions and a macro for serving static files.

## Key items

* `template_response` — renders an `askama` template into an `axum` `Html` response.
* `make_handle_static!` — macro that generates a `handle_static` route handler; serves embedded files in release mode and reads from the filesystem in `dev-web` mode.
* `handle_liveness_check` — returns `200 OK` for health probes.
* `handle_prometheus` — encodes Prometheus metrics from a `MetricsRegistry` into the text exposition format.
* `handle_reload_tracing_filter` — accepts a JSON body with a new `EnvFilter` string and reloads the tracing layer via a `TracingHandle`.
* `handle_tracing` — returns the current tracing level filter as JSON.
* `build_cors_allowed_origin` — constructs a `tower-http` `AllowOrigin` policy from a list of allowed origin header values, supporting exact matches, wildcard subdomain globs (`*.example.org`), and a bare `*` for unrestricted access.

## Dependencies

Depends on `axum`, `tower-http`, `prometheus`, `mz-ore` (metrics + tracing features), `askama`, and `tracing-subscriber`.
Consumed by HTTP server components throughout Materialize that need common route handlers and CORS configuration.

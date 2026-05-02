---
source: src/http-util/src/lib.rs
revision: aaed3fa7d3
---

# mz-http-util

Provides shared utilities for running HTTP servers in Materialize.

## Module structure

The crate is a single `lib.rs` with no submodules.
It exports a collection of axum handler functions and a macro for serving static files.

## Key items

* `template_response` — renders an `askama` template into an `axum` `Html` response.
* `make_handle_static\!` — macro that generates a `handle_static` route handler; serves embedded files in release mode and reads from the filesystem in `dev-web` mode.
* `handle_liveness_check` — returns `200 OK` for health probes.
* `handle_prometheus` — encodes Prometheus metrics from a `MetricsRegistry` into the text exposition format.
* `handle_reload_tracing_filter` — accepts a JSON body with a new `EnvFilter` string and reloads the tracing layer via a `TracingHandle`.
* `handle_tracing` — returns the current tracing level filter as JSON.
* `origin_is_allowed` — returns `true` if an `Origin` header value matches any entry in an allowed list; supports bare `*` (any origin), exact match, and wildcard subdomain globs (`*.example.com`).
* `build_cors_allowed_origin` — constructs a `tower-http` `AllowOrigin` policy from a list of allowed origin header values, supporting exact matches, wildcard subdomain globs (`*.example.org`), and a bare `*` for unrestricted access.
* `DynamicFilterTarget` — the deserialized JSON body type for `handle_reload_tracing_filter`.

## Key dependencies

`axum`, `tower-http`, `prometheus`, `mz-ore` (metrics + tracing), `askama`, `tracing-subscriber`.

## Downstream consumers

HTTP server components throughout Materialize that need common route handlers, metrics endpoints, and CORS configuration.

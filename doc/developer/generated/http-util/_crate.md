---
source: src/http-util/src/lib.rs
revision: 7b43a3d28e
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
* `handle_prometheus` — encodes Prometheus metrics from a `MetricsRegistry` into the Prometheus text or protobuf exposition format, selected by the caller's `Accept` header. When the caller sends `X-Materialize-Accept-Enrich-Rules: 1`, the response also includes an `X-Materialize-Enrich-Rules` header carrying the registry's per-metric enrichment rules as gzipped, base64-encoded JSON.
* `encode_enrich_rules` / `decode_enrich_rules` — serialize and deserialize the enrichment-rules map (gzip + base64) for transport in `X-Materialize-Enrich-Rules`.
* `MATERIALIZE_ACCEPT_ENRICH_RULES_HEADER` / `MATERIALIZE_ENRICH_RULES_HEADER` — the request and response header name constants for the enrichment-rules negotiation.
* `handle_reload_tracing_filter` — accepts a JSON body with a new `EnvFilter` string and reloads the tracing layer via a `TracingHandle`.
* `handle_tracing` — returns the current tracing level filter as JSON.
* `origin_is_allowed` — returns `true` if an `Origin` header value matches any entry in an allowed list; supports bare `*` (any origin), exact match, and wildcard subdomain globs (`*.example.com`).
* `build_cors_allowed_origin` — constructs a `tower-http` `AllowOrigin` policy from a list of allowed origin header values, supporting exact matches, wildcard subdomain globs (`*.example.org`), and a bare `*` for unrestricted access.
* `DynamicFilterTarget` — the deserialized JSON body type for `handle_reload_tracing_filter`.

## Key dependencies

`axum`, `tower-http`, `prometheus`, `mz-ore` (metrics + tracing), `askama`, `tracing-subscriber`.

## Downstream consumers

HTTP server components throughout Materialize that need common route handlers, metrics endpoints, and CORS configuration.

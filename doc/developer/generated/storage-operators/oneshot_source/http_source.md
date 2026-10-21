---
source: src/storage-operators/src/oneshot_source/http_source.rs
revision: 26a454f145
---

# storage-operators::oneshot_source::http_source

Implements `OneshotSource` for generic HTTP as `HttpOneshotSource`, with associated `HttpObject` and `HttpChecksum` types.
The `list` method issues a `HEAD` (or fallback `GET`) request to read `Content-Length`, `ETag`, and `Last-Modified` metadata; the `get` method streams the object body with optional `Range` support.
Both the fallback `GET` in `list` and the `get` method call `check_success`, which rejects 3xx responses with `StorageErrorXKind::Redirect` and any other non-2xx response with `StorageErrorXKind::HttpStatus`, preventing error-page bodies from being ingested as data. Redirects are disabled on the underlying `reqwest` client (built by `build_http_client`) to close an SSRF hole; the initial `HEAD` in `list` returns a redirect error directly without falling through to the `GET` path.
`build_http_client` installs `MzHttpResolver`, which delegates DNS to `mz_ore::netio::resolve_address` and optionally rejects private/local addresses; only the IP resolution step is overridden, so SNI and TLS certificate validation use the original hostname.

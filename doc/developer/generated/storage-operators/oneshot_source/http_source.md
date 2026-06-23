---
source: src/storage-operators/src/oneshot_source/http_source.rs
revision: a23b5e40d2
---

# storage-operators::oneshot_source::http_source

Implements `OneshotSource` for generic HTTP as `HttpOneshotSource`, with associated `HttpObject` and `HttpChecksum` types.
The `list` method issues a `HEAD` (or fallback `GET`) request to read `Content-Length`, `ETag`, and `Last-Modified` metadata; the `get` method streams the object body with optional `Range` support.

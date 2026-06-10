---
source: src/ccsr/src/lib.rs
revision: 30d929249e
---

# mz-ccsr

Provides an async API client for Confluent-compatible schema registries (CCSRs), supporting schema lookup by ID or subject, subject listing, schema publishing, and compatibility configuration.
The client handles HTTP basic auth, TLS client certificates, DNS overrides, and dynamic URL callbacks, making it suitable for Materialize's Kafka source/sink integration which needs to retrieve and register Avro/Protobuf schemas at runtime.

## Module structure

* `client` — `Client` with schema CRUD methods and error types.
* `config` — `ClientConfig` builder and `Auth`.
* `tls` — serde-enabled `Identity` and `Certificate` wrappers.

## Key dependencies

* `reqwest` (with `native-tls-vendored`) — HTTP transport.
* `mz-tls-util` — PKCS #12 construction from PEM.
* `serde_json` — schema registry JSON wire format.

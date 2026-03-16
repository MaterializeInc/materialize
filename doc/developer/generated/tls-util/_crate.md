---
source: src/tls-util/src/lib.rs
revision: ca42a663e0
---

# mz-tls-util

Provides utilities for constructing TLS connectors for `tokio-postgres` connections.

## Module structure

The crate consists of a single `lib.rs` with no submodules.
It exposes two public functions and two public types:

* `make_tls` — builds a `MakeTlsConnector` (from `postgres-openssl`) for a given `tokio_postgres::Config`, respecting the configured `SslMode` to set peer verification and hostname verification.
* `pkcs12der_from_pem` — converts a PEM-encoded private key and certificate into a DER-encoded PKCS #12 archive (`Pkcs12Archive`), suitable for use with `reqwest::Identity`.
* `TlsError` — a `thiserror`-derived enum wrapping either a generic `anyhow::Error` or an `openssl::error::ErrorStack`.
* `Pkcs12Archive` — a plain struct holding the DER bytes and the (empty) password for the produced PKCS #12 archive.

## Key dependencies

* `openssl` (vendored) — certificate and key parsing, PKCS #12 construction, `SslConnector` building.
* `postgres-openssl` — provides `MakeTlsConnector`, the adapter between OpenSSL and `tokio-postgres`.
* `tokio-postgres` — supplies `Config` and `SslMode`.
* `anyhow` / `thiserror` — error handling.

## Downstream consumers

This crate is consumed by components that open TLS-protected PostgreSQL connections, such as the adapter and storage layers that connect to external PostgreSQL-compatible databases or Cockroach.

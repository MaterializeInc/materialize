---
source: src/storage-types/src/connections/iceberg_credentials.rs
revision: 8ceeee9a3a
---

# `storage_types::connections::iceberg_credentials`

Catalog-vended storage credentials for Iceberg sinks.

## Overview

A REST Iceberg catalog configured for access delegation returns temporary, table-scoped storage credentials in its `loadTable` response instead of expecting the client to hold permanent credentials. Those credentials expire, and OpenDAL has no notion of expiry: it uses whatever the `FileIO` was built with. `VendedCredentialLoader` closes that gap by re-fetching credentials from the catalog's `loadCredentials` endpoint on demand, caching the result until just before expiry.

## Locating the Endpoint

Locating the `loadCredentials` endpoint requires a round trip to the catalog's `config` endpoint, which announces a request prefix (`catalogs/<name>` for Unity Catalog, absent for others). Every resource path carries this prefix. `iceberg-rust` resolves the same value internally but keeps it private, so `table_credentials_endpoint` asks the server directly. `table_credentials_endpoint` now also takes a `headers: &HeaderMap` for the headers the catalog client puts on its own requests, which this one bypasses.

## Key Types

**`VendedCredential`** — A trait implemented by storage credentials a REST catalog can vend. Implementors carry the object store's notion of a credential and know which `storage-credentials` properties encode it. The `STORE` constant names the object store for diagnostics. `from_vended` builds the credential from a `StorageCredential`. `expires_at` returns when the credential expires, if the catalog reported it. Both `AwsCredential` (S3) and `GcsCredential` (GCS) implement this trait.

**`VendedCredentialLoader<C>`** — Generic over `C: VendedCredential`. Implements `ProvideCredential<Credential = C>` for OpenDAL. Holds the HTTP client, the resolved `credential_endpoint` URL, a `TokenProvider` for the catalog auth token, a `HeaderMap` of headers to include on every request (including the access-delegation header, which is inserted by `new`), and a `Mutex<Option<(C, Instant)>>` cache.

The cache lock is held across the fetch. Because `create_operator` builds a fresh OpenDAL `Operator` per file operation, reqsign's own credential cache never survives across operations, making this cache the only defense against one catalog round trip per storage request. Serializing on the lock means a stale entry costs one refetch rather than one per in-flight operation.

## Credential Refresh Timing

- A reported expiry property (`s3.session-token-expires-at-ms` for S3, `gcs.oauth2.token-expires-at` for GCS) is parsed and subtracted from `VENDED_CREDENTIAL_REFRESH_BUFFER` (15 min) to absorb clock skew and fetch latency. The buffer must stay above reqsign's own refresh buffers (120s for GCS tokens).
- When no expiry is reported, the credential is trusted for `VENDED_CREDENTIAL_DEFAULT_TTL` (5 min) and then re-fetched.

On an auth error (401/403) from the credentials endpoint, the cached catalog token is invalidated so the next attempt fetches a fresh one.

## Functions

- `table_credentials_endpoint` — resolves the `loadCredentials` URL for a table by querying the catalog's `config` endpoint first; called once when a sink dataflow starts
- `catalog_config_url` — constructs `/v1/config` with an optional `warehouse` query parameter, stripping any trailing slash so the path does not contain an empty segment
- `table_credentials_url` — constructs `/v1/<prefix>/namespaces/<ns>/tables/<name>/credentials`, splitting a multi-segment prefix on `/` so separators are not percent-encoded

## Private Types

**`LoadCredentialsResponse`** — Deserializes the `loadCredentials` response envelope. When the catalog returns multiple credentials, the one with the longest prefix is used as the closest approximation to the Iceberg specification's longest-prefix match.

**`CatalogConfigResponse`** — Deserializes the `config` response. `announced_prefix()` returns the server's request prefix with overrides taking priority over defaults, matching how the catalog client merges them.

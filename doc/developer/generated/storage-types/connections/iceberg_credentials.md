---
source: src/storage-types/src/connections/iceberg_credentials.rs
revision: f4ed781373
---

# `storage_types::connections::iceberg_credentials`

Catalog-vended storage credentials for Iceberg sinks.

## Overview

A REST Iceberg catalog configured for access delegation returns temporary, table-scoped storage credentials in its `loadTable` response instead of expecting the client to hold permanent credentials. Those credentials expire, and OpenDAL has no notion of expiry: it uses whatever the `FileIO` was built with. `VendedCredentialLoader` closes that gap by re-fetching credentials from the catalog's `loadCredentials` endpoint on demand, caching the result until just before expiry.

## Locating the Endpoint

Locating the `loadCredentials` endpoint requires a round trip to the catalog's `config` endpoint, which announces a request prefix (`catalogs/<name>` for Unity Catalog, absent for others). Every resource path carries this prefix. `iceberg-rust` resolves the same value internally but keeps it private, so `table_credentials_endpoint` asks the server directly.

## Key Types

**`VendedCredentialLoader`** — Implements `ProvideCredential<Credential = AwsCredential>` for OpenDAL. Holds the HTTP client, the resolved `credential_endpoint` URL, a `TokenProvider` for the catalog auth token, and a `Mutex<Option<(AwsCredential, Instant)>>` cache.

The cache lock is held across the fetch. Because `create_operator` builds a fresh OpenDAL `Operator` per file operation, reqsign's own credential cache never survives across operations, making this cache the only defense against one catalog round trip per S3 request. Serializing on the lock means a stale entry costs one refetch rather than one per in-flight operation.

## Credential Refresh Timing

- A reported `s3.session-token-expires-at-ms` is parsed and subtracted from `VENDED_CREDENTIAL_REFRESH_BUFFER` (15 min) to absorb clock skew and fetch latency
- When no expiry is reported, the credential is trusted for `VENDED_CREDENTIAL_DEFAULT_TTL` (5 min) and then re-fetched

On an auth error (401/403) from the credentials endpoint, the cached catalog token is invalidated so the next attempt fetches a fresh one.

## Functions

- `table_credentials_endpoint` — resolves the `loadCredentials` URL for a table by querying the catalog's `config` endpoint first; called once when a sink dataflow starts
- `catalog_config_url` — constructs `/v1/config` with an optional `warehouse` query parameter, stripping any trailing slash so the path does not contain an empty segment
- `table_credentials_url` — constructs `/v1/<prefix>/namespaces/<ns>/tables/<name>/credentials`, splitting a multi-segment prefix on `/` so separators are not percent-encoded

## Private Types

**`LoadCredentialsResponse`** — Deserializes the `loadCredentials` response envelope. When the catalog returns multiple credentials, the one with the longest prefix is used as the closest approximation to the Iceberg specification's longest-prefix match.

**`CatalogConfigResponse`** — Deserializes the `config` response. `announced_prefix()` returns the server's request prefix with overrides taking priority over defaults, matching how the catalog client merges them.

---
source: src/storage-types/src/connections/gcp.rs
revision: 5173c50671
---

# storage-types::connections::gcp

GCP connection configuration for storage sources and sinks.

`GcpConnection` holds a `credentials_json: CatalogItemId` pointing to a catalog secret that contains a GCP service-account key in JSON format (as produced by `gcloud iam service-accounts keys create`). `read_credentials` reads and validates the secret, returning both the raw JSON string and a parsed `CustomServiceAccount`; `validate` additionally mints a test token to confirm the credentials are valid. `validate_by_default` returns `false`, so validation is opt-in.

`GcpServiceAccountKeyTokenUri` is a helper deserializer that checks the `token_uri` field of a service-account JSON key; `validate_json` enforces that it equals `https://oauth2.googleapis.com/token`.

`GcpConnectionReference<C>` references a GCP connection by `CatalogItemId` plus the inlined or referenced connection object. `IntoInlineConnection` is implemented to resolve a `ReferencedConnection` to an `InlinedConnection`.

`GcpTokenProvider` wraps a `CustomServiceAccount` and implements the `iceberg_catalog_rest::TokenProvider` trait, minting tokens scoped to `https://www.googleapis.com/auth/cloud-platform` for use by the Iceberg REST catalog client (BigLake/Lakehouse).

`GcpConnectionValidationError` covers three failure modes: `SecretRead` (secret-store error), `ParseKey` (JSON parse or token-URI mismatch), and `FetchToken` (Google OAuth error). The `hint` for `ParseKey` directs operators to the expected JSON source command.

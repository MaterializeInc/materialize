---
source: src/persist/src/azure.rs
revision: 0a070511dd
---

# persist::azure

Implements the `Blob` trait backed by Azure Blob Storage via the `azure_storage_blob` 1.1 / `azure_core` 1.1 / `azure_identity` 1.0 SDK.
`AzureBlobConfig::new` builds a credential and a `BlobContainerClient` with retry and timeout transport options from the knobs configuration. Cloning `AzureBlobConfig` shares the underlying client and its HTTP connection pool; connection-pool isolation (as hedged gets require) needs a fresh `AzureBlobConfig::new`.
The credential selection order is: (1) if `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_FEDERATED_TOKEN_FILE` are all set and `AZURE_FEDERATED_TOKEN` is not set, `RefreshingWorkloadIdentityCredential` is used — a custom `TokenCredential` that re-reads the projected service account token file on every AAD access token refresh via `ClientAssertionCredential`, picking up Kubernetes token rotations; each scope set gets its own token slot kept fresh by a background task, and a failed refresh leaves the current token in place and retries after `TOKEN_REFRESH_RETRY_INTERVAL`; (2) otherwise, a fallback chain tries environment credentials (static federated token or client secret via `AZURE_CLIENT_SECRET`), managed identity (with a `MANAGED_IDENTITY_TIMEOUT` of 1 second to avoid blocking on the IMDS endpoint outside Azure), then Azure CLI.
`get` fetches blobs as concurrent 1 MiB ranged GETs (`GET_PARTITION_SIZE`) up to `GET_CONCURRENCY` in flight, with all requests pinned to the etag from the first response to detect concurrent modifications.
The Azurite emulator path signs an account SAS with the well-known emulator key instead of using shared key auth (which the new SDK no longer supports).
`delete` first checks for the blob's properties (returning `None` on a 404), then deletes it and propagates any errors; `restore` checks properties and returns an error if the blob does not exist.

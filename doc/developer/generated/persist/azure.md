---
source: src/persist/src/azure.rs
revision: db15d3b2dc
---

# persist::azure

Implements the `Blob` trait backed by Azure Blob Storage via the `azure_storage_blobs` SDK.
`AzureBlob` supports SAS-token, workload-identity, managed-identity, and emulator authentication modes; `get` streams response chunks concurrently via `FuturesOrdered` and assembles them into a `SegmentedBytes`.
`AzureBlobConfig::new` constructs an explicit `reqwest` HTTP client with per-attempt, read, and connect timeouts from the knobs configuration, passed to the SDK via `TransportOptions`, for all authentication modes. Cloning `AzureBlobConfig` shares the underlying client and its HTTP connection pool; connection-pool isolation (as hedged gets require) needs a fresh `AzureBlobConfig::new`.
When the AKS workload identity environment variables (`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_FEDERATED_TOKEN_FILE`) are present and `AZURE_FEDERATED_TOKEN` is not set, `AzureBlobConfig::new` uses `RefreshingWorkloadIdentityCredential` instead of the SDK's default credential chain. This custom credential re-reads the projected service account token file on every AAD access token refresh, picking up Kubernetes token rotations that would otherwise permanently lock a long-running process out of blob storage once the cached AAD token expires. Each scope set gets its own token slot kept fresh by a dedicated background task; a failed refresh leaves the current token in place and retries after `TOKEN_REFRESH_RETRY_INTERVAL`.
`delete` first checks for the blob's properties (returning `None` on a 404), then deletes it and propagates any errors; `restore` checks properties and returns an error if the blob does not exist.

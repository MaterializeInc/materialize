---
source: src/persist/src/azure.rs
revision: 56c3a468ac
---

# persist::azure

Implements the `Blob` trait backed by Azure Blob Storage via the `azure_storage_blobs` SDK.
`AzureBlob` supports SAS-token, default credential (e.g. workload identity), and emulator authentication modes; `get` streams response chunks concurrently via `FuturesOrdered` and assembles them into a `SegmentedBytes`.
`AzureBlobConfig::new` constructs an explicit `reqwest` HTTP client with per-attempt, read, and connect timeouts from the knobs configuration, passed to the SDK via `TransportOptions`, for all authentication modes.
`delete` first checks for the blob's properties (returning `None` on a 404), then deletes it and propagates any errors; `restore` checks properties and returns an error if the blob does not exist.

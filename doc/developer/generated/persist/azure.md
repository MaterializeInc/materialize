---
source: src/persist/src/azure.rs
revision: b89a9e0ec5
---

# persist::azure

Implements the `Blob` trait backed by Azure Blob Storage via the `azure_storage_blobs` SDK.
`AzureBlob` supports SAS-token, workload-identity, and emulator authentication modes; `get` streams response chunks concurrently via `FuturesOrdered` and assembles them into a `SegmentedBytes`.
The emulator path constructs an explicit `reqwest` HTTP client with per-attempt, read, and connect timeouts from the knobs configuration, passed to the SDK via `TransportOptions`.
`delete` first checks for the blob's properties (returning `None` on a 404), then deletes it and propagates any errors; `restore` checks properties and returns an error if the blob does not exist.

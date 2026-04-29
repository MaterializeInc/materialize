---
source: src/persist/src/azure.rs
revision: 9d0a7c3c6f
---

# persist::azure

Implements the `Blob` trait backed by Azure Blob Storage via the `azure_storage_blobs` SDK.
`AzureBlob` supports SAS-token, workload-identity, and emulator authentication modes; object data is streamed in chunks and spilled into lgalloc-backed regions.
The emulator path constructs an explicit `reqwest` HTTP client with per-attempt, read, and connect timeouts from the knobs configuration, passed to the SDK via `TransportOptions`.
`delete` first checks for the blob's properties (returning `None` on a 404), then deletes it and propagates any errors; `restore` verifies the blob exists and returns an error if it does not.

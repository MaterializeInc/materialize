---
source: src/persist/src/azure.rs
revision: 5f785f23fd
---

# persist::azure

Implements the `Blob` trait backed by Azure Blob Storage via the `azure_storage_blobs` SDK.
`AzureBlob` supports SAS-token, workload-identity, and emulator authentication modes; object data is streamed in chunks and spilled into lgalloc-backed regions.
`delete` removes the blob directly; `restore` verifies the blob exists and returns an error if it does not.

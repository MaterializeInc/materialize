---
source: src/persist/src/azure.rs
revision: a690dc9fd6
---

# persist::azure

Implements the `Blob` trait backed by Azure Blob Storage via the `azure_storage_blobs` SDK.
`AzureBlob` supports SAS-token, workload-identity, and emulator authentication modes; object data is streamed in chunks and spilled into lgalloc-backed regions.
`delete` and `restore` use rename-to-tombstone semantics consistent with the other `Blob` backends.

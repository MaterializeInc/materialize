---
source: src/persist/src/azure.rs
revision: a3b8bc5bf6
---

# persist::azure

Implements the `Blob` trait backed by Azure Blob Storage via the `azure_storage_blobs` SDK.
`AzureBlob` supports SAS-token, workload-identity, and emulator authentication modes; object data is streamed in chunks and spilled into lgalloc-backed regions.
`delete` removes the blob and treats a 404 response during the delete call as a successful no-op (returning `None`) to handle races between concurrent deletions; `restore` verifies the blob exists and returns an error if it does not.

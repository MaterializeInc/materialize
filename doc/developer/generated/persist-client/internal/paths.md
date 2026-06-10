---
source: src/persist-client/src/internal/paths.rs
revision: 4267863081
---

# persist-client::internal::paths

Defines the key-naming scheme for all persist blob objects and consensus entries: `PartId`, `RollupId`, `WriterKey`, `PartialBatchKey`, `PartialRollupKey`, and `BlobKey`.
`WriterKey` encodes either a writer UUID (for old blobs) or a semver version string (for newer blobs), allowing GC to determine whether an unlinked blob might still be written by an in-flight operation.

---
source: src/persist-client/src/internal/restore.rs
revision: e7103213d5
---

# persist-client::internal::restore

Provides `restore_blob`, which attempts to re-upload any blobs referenced by the current shard state in consensus that are missing from blob storage.
This is used in disaster-recovery scenarios where blob storage has lost data but consensus diffs are intact; the function walks all live diffs and re-writes any recoverable parts.

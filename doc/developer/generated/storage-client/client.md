---
source: src/storage-client/src/client.rs
revision: 4267863081
---

# storage-client::client

Defines the `StorageClient` trait and the `StorageCommand`/`StorageResponse` protocol enums that form the public API between the storage controller and storage replicas.
Also provides `PartitionedStorageState`, which fans out commands to multiple replica shards and merges their responses into a unified stream, and `TimestamplessUpdateBuilder` for staging Persist batches without a timestamp.
Key types include `RunIngestionCommand`, `RunSinkCommand`, `RunOneshotIngestion`, `StatusUpdate`, `Status`, `TableData`, and `AppendOnlyUpdate`.

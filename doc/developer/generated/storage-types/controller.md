---
source: src/storage-types/src/controller.rs
revision: 68716f517f
---

# storage-types::controller

Defines types used at the boundary between the storage controller and other subsystems.
`CollectionMetadata` records the persist location, data shard, schema, and optional txn-wal shard for a storage collection, and implements `AlterCompatible` to protect immutable fields.
`DurableCollectionMetadata` is the durable subset of `CollectionMetadata`, containing only the `data_shard`.
`StorageError` enumerates all error conditions surfaced by the storage controller, including identifier lifecycle errors, upper/since violations, instance-missing errors, persist-layer errors, and read-only mode violations.
`TxnsCodecRow` implements `TxnsCodec` to encode txn-wal entries as `SourceData` rows with columns `shard_id`, `ts`, and `batch`.
Also defines `AlterError` (returned when an incompatible alter is attempted) and `InvalidUpper` (carrying the actual upper antichain when an expected-upper check fails).

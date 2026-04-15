---
source: src/storage-types/src/controller.rs
revision: 00cc513fa5
---

# storage-types::controller

Defines types used at the boundary between the storage controller and other subsystems.
`CollectionMetadata` records the persist location, data shard, schema, and optional txn-wal shard for a storage collection, and implements `AlterCompatible` to protect immutable fields.
Also defines `AlterError` (returned when an incompatible alter is attempted) and various command/response types used in the controller protocol.

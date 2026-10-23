---
source: src/cluster-client/src/instances.rs
revision: bbb56d46f3
---

# mz_cluster_client::instances

Identifier type for storage instances.

## Key types

- **`StorageInstanceId`** -- Enum (`System(u64)` | `User(u64)`) that identifies a storage instance by namespace and numeric id. The inner id is constrained to 48 bits because it is packed into `mz_repr::GlobalId::IntrospectionSourceIndex`; the constructors `system()` and `user()` enforce this by returning `None` when the top 16 bits are set. The `Display`/`FromStr` pair serializes as `s<id>` (system) or `u<id>` (user).

## Relationships

`StorageInstanceId` is re-exported by `mz_storage_types` and used throughout the storage controller and adapter layers to address storage instances. The 48-bit packing constraint ties it structurally to `mz_repr::GlobalId::IntrospectionSourceIndex`.

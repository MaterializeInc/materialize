---
source: src/storage-types/src/instances.rs
revision: 4267863081
---

# storage-types::instances

Defines `StorageInstanceId`, the identifier for a storage instance, with `System` and `User` variants.
IDs are restricted to 48 bits so they can be packed into `GlobalId::IntrospectionSourceIndex`.
Implements `Display` (`s<id>` / `u<id>`) and `FromStr` for serialization.

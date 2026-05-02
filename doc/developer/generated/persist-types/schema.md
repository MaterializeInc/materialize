---
source: src/persist-types/src/schema.rs
revision: 4267863081
---

# persist-types::schema

Implements persist schema evolution: `backward_compatible` checks whether arrow data encoded with an old `DataType` can be migrated to a new one and, if so, returns a `Migration` that performs the transformation.
`Migration` supports adding nullable fields at the end, dropping fields, making fields nullable, and recursing into nested structs and lists; it tracks whether any data will be dropped (`contains_drop`) and whether sort order is preserved (`preserves_order`).
`SchemaId` is a monotonically increasing identifier assigned to each registered key/value schema pair on a shard.

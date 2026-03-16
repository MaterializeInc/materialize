---
source: src/catalog/src/memory/error.rs
revision: a5885973e5
---

# catalog::memory::error

Defines `Error` and `ErrorKind` for the in-memory catalog layer.
`ErrorKind` covers logical catalog violations such as reserved names (schemas, roles, clusters, replicas, network policies), read-only system objects, non-empty schema drops, dependency violations, and item-type mismatches.
These errors are distinct from the durable `CatalogError` and represent constraints enforced by the in-memory state machine.

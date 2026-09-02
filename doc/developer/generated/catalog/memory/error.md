---
source: src/catalog/src/memory/error.rs
revision: 605cc6fd1a
---

# catalog::memory::error

Defines `Error` and `ErrorKind` for the in-memory catalog layer.
`ErrorKind` covers logical catalog violations such as reserved names (schemas, roles, clusters, replicas, network policies), read-only system objects, non-empty schema drops, dependency violations, and item-type mismatches. The hint for the `ReservedRoleName` variant states that the role `"public"` and the prefixes `"mz_"` and `"pg_"` are reserved for system roles, and additionally that the role specification names `"current_user"`, `"current_role"`, `"session_user"`, `"user"`, and `"none"` are reserved.
These errors are distinct from the durable `CatalogError` and represent constraints enforced by the in-memory state machine.

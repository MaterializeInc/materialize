---
source: src/catalog/src/durable/transaction.rs
revision: f94584ddef
---

# catalog::durable::transaction

Defines `Transaction`, which batches multiple catalog mutations and commits them atomically to the durable store.
Internally holds per-collection `TableTransaction` instances for databases, schemas, items, comments, roles, role_auth, clusters, cluster replicas, introspection sources, ID allocators, configs, settings, system GID mappings, system configurations, default privileges, source references, system privileges, storage collection metadata, unfinalized shards, txn-wal shards, and network policies.
Provides methods for every catalog DDL operation (create/update/delete for clusters, replicas, items, roles, schemas, comments, network policies, storage metadata, etc.) as well as ID allocation from the per-collection allocators.
`TransactionBatch` is the serializable representation of the accumulated changes; it is passed to `DurableCatalogState::commit_transaction` for atomic persist writes.

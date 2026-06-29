---
source: src/catalog/src/durable/transaction.rs
revision: 3762081e1d
---

# catalog::durable::transaction

Defines `Transaction`, which batches multiple catalog mutations and commits them atomically to the durable store.
Internally holds per-collection `TableTransaction` instances for databases, schemas, items, comments, roles, role_auth, clusters, cluster replicas, introspection sources, ID allocators, configs, settings, system GID mappings, system configurations, cluster system configurations, replica system configurations, default privileges, source references, system privileges, storage collection metadata, unfinalized shards, txn-wal shards, and network policies.
Provides methods for every catalog DDL operation (create/update/delete for clusters, replicas, items, roles, schemas, comments, network policies, storage metadata, etc.) as well as ID allocation from the per-collection allocators (audit log, OIDs, schema, cluster, replica, role, network policy, and item allocators). `get_cluster_system_configurations`, `upsert_cluster_system_config`, and `remove_cluster_system_config` manage the per-cluster scoped system parameter overrides; the analogous `get_replica_system_configurations`, `upsert_replica_system_config`, and `remove_replica_system_config` methods manage per-replica overrides.
`insert_system_cluster` returns the allocated `ClusterId` on success.
`TransactionBatch` is the serializable representation of the accumulated changes; it is passed to `DurableCatalogState::commit_transaction` for atomic persist writes.

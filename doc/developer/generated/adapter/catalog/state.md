---
source: src/adapter/src/catalog/state.rs
revision: 3953456a45
---

# adapter::catalog::state

Defines `CatalogState`, the in-memory representation of the entire Materialize catalog.
`CatalogState` holds `imbl::OrdMap` collections for databases, schemas, roles, role auth (`role_auth_by_id`), clusters, network policies, the item namespace (keyed by both `CatalogItemId` and `GlobalId`), source references, temporary schemas per connection, comments, default privileges, system privileges, system configuration, scoped system parameters, and a `notices_by_dep_id` index mapping each `GlobalId` to the optimizer notices that depend on it; it also implements `SessionCatalog` for SQL name resolution.
`scoped_system_parameters` (type `ScopedParameters` from `crate::config`) is an in-memory mirror of the durable `cluster_system_configurations` and `replica_system_configurations` catalog collections, maintained by `apply.rs`. The `cluster_scoped_optimizer_overrides(cluster_id)` method returns the cluster-coherent `OptimizerFeatureOverrides` for a given cluster from this working copy, and `scoped_system_parameters()` exposes the entire working copy for the coordinator's scoped-parameter reconcile path. The field is excluded from the consistency-check snapshot because it is fully derived from durable state.
The file also defines `LocalExpressionCache`, a helper used during catalog open and catalog transactions to track which optimizer expressions were found in (or are missing from) the persistent expression cache, avoiding redundant re-optimization.
When `deserialize_item` re-parses an existing item (e.g. during a RENAME), it preserves the `optimized_plan`, `physical_plan`, and `dataflow_metainfo` fields from the previous `CatalogItem` incarnation via `plan_fields_mut`; these fields are not reconstructable from `create_sql` alone and must be carried over to avoid silently dropping plans for materialized views and indexes.
`concretize_replica_location` and `ensure_valid_replica_size` accept an `allow_disabled` boolean; when `true`, disabled sizes in the size map are permitted (used during catalog apply so existing replicas with disabled sizes remain registered without error). The `cluster_replica_size_has_disk` helper has been removed; the `disk` column is resolved in the `mz_cluster_replicas` materialized view via a LEFT JOIN against `mz_cluster_replica_size_internal`.
Connection inlining (used when resolving connection references for storage and compute) handles the `GlueSchemaRegistry` and `Gcp` connection types alongside `Kafka`, `Postgres`, and `Csr`.
This is the authoritative read-side view of the catalog; mutations go through `catalog::transact` and are applied by `catalog::apply`.

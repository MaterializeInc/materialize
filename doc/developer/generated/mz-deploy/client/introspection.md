---
source: src/mz-deploy/src/client/introspection.rs
revision: 3f3bdb0535
---

# mz-deploy::client::introspection

Read-only catalog introspection queries.

Methods on `IntrospectionClient` query `mz_catalog` and `information_schema` to inspect the live environment without modifying it. Provides batch existence checks for schemas, clusters, and objects, as well as dependency lookups used during deployment planning and sink repointing.

`DependentSink` identifies a sink that depends on an object in a schema being dropped; it is used during apply to find sinks that must be repointed before old schemas are dropped with CASCADE.

Cluster queries include autoscaling strategy support via `auto_scaling_query_parts`, which returns SQL fragments for joining `mz_internal.mz_cluster_auto_scaling_strategies` on regions that support the feature, or a `NULL` column on older regions. `parse_auto_scaling_strategy` deserializes the strategy JSON from a query row.

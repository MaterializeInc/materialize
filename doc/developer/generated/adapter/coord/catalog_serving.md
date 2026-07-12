---
source: src/adapter/src/coord/catalog_serving.rs
revision: 0edc10fed4
---

# adapter::coord::catalog_serving

Provides logic for the `mz_catalog_server` system cluster and the `mz_support` role.
`auto_run_on_catalog_server` determines whether a query should be transparently routed to the `mz_catalog_server` cluster based on whether it depends only on system-schema objects, does not reference per-replica introspection sources, and the session has auto-routing enabled.
`check_cluster_restrictions` enforces that queries running on the `mz_catalog_server` cluster do not reference user-defined objects. It inspects `Plan::Select`, `Plan::Subscribe`, `Plan::ReadThenWrite`, `Plan::Insert` (checking the insert's selection for non-constant inserts), and `Plan::CopyTo` (checking the select dataflow); all other plan kinds pass without inspection. A constant `INSERT` has no dependencies and passes the check, since it runs no computation on the cluster.

---
source: src/adapter/src/coord/catalog_serving.rs
revision: 9d0a7c3c6f
---

# adapter::coord::catalog_serving

Provides logic for the `mz_catalog_server` system cluster and the `mz_support` role.
`auto_run_on_catalog_server` determines whether a query should be transparently routed to the `mz_catalog_server` cluster based on whether it depends only on system-schema objects, does not reference per-replica introspection sources, and the session has auto-routing enabled.
`check_cluster_restrictions` enforces that queries running on the `mz_catalog_server` cluster do not reference user-defined objects.

---
source: src/mz-deploy/src/project/clusters.rs
revision: 2e6c03ac43
---

# mz-deploy::project::clusters

Cluster definition loading and validation.

`ClusterDefinition` holds the parsed state for a single cluster file: the cluster name, its `CREATE CLUSTER` statement, any `GRANT` statements targeting it, and any `COMMENT` statements targeting it.

`load_clusters` scans `<root>/clusters/` for `.sql` files, parses each file, and validates it via `classify_cluster_statements`. Profile overrides are resolved (a file matching the active profile name takes precedence over the default variant). After validation, if a `profile_suffix` is given, `apply_cluster_suffix` rewrites all cluster name references in the definition so the live cluster name carries the suffix.

All variants of each cluster file are validated independently before the active variant is resolved, so structural errors in non-active profile variants are caught even when they would not be used.


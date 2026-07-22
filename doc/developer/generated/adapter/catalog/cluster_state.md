---
source: src/adapter/src/catalog/cluster_state.rs
revision: 74f18a3354
---

# adapter::catalog::cluster_state

Projects a managed cluster's durable catalog config into the `ExpectedClusterState` compare-and-append witness, and checks a witness against the current config.

`project_expected(managed: &ClusterVariantManaged) -> ExpectedClusterState` is the single projection from catalog config to the witness. Building the witness the same way wherever a write is conditioned and wherever it is checked keeps the compared fields from drifting apart.

The function uses an exhaustive destructure of `ClusterVariantManaged` (no `..`) so that adding a field to the managed config is a compile error until the author decides whether the witness must cover it. The `auto_scaling_strategy` field is included in the witness (projected via `auto_scaling_policy`) because it determines whether, and at what size, a burst is warranted.

Also provides `cluster_matches_expected(state: &CatalogState, cluster_id: ClusterId, expected: &ExpectedClusterState) -> bool` which re-projects the live config and returns `false` if the cluster is missing, unmanaged, or its projected state no longer equals the witness. A missing or unmanaged cluster never matches. This is the compare half of the compare-and-append, evaluated inside the catalog transaction so the check and the commit cannot be separated.

---
source: src/adapter/src/catalog/cluster_state.rs
revision: 73111c3e52
---

# adapter::catalog::cluster_state

Projects a managed cluster's durable catalog config into the `ExpectedClusterState` compare-and-append witness, and checks a witness against the current config.

`project_expected(managed: &ClusterVariantManaged) -> ExpectedClusterState` is the single projection from catalog config to the witness. Building the witness the same way wherever a write is conditioned and wherever it is checked keeps the compared fields from drifting apart.

The function uses an exhaustive destructure of `ClusterVariantManaged` (no `..`) so that adding a field to the managed config is a compile error until the author decides whether the witness must cover it.

Also provides `check_cluster_state(state: &CatalogState, cluster_id: ClusterId, expected: &ExpectedClusterState) -> Result<(), AdapterError>` which re-projects the live config and returns `Err(AdapterError::ClusterStateChanged)` if it no longer equals the witness. This is the compare-and-append check the apply path calls before transacting each decision batch.

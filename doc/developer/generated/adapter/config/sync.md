---
source: src/adapter/src/config/sync.rs
revision: dbd2c3fc06
---

# adapter::config::sync

Implements `system_parameter_sync`, the periodic task loop that pulls parameters from the `SystemParameterFrontend` (LaunchDarkly or file) and pushes modified values to the `SystemParameterBackend` (coordinator via `ALTER SYSTEM SET`).
The loop ticks at a configurable interval (skipping missed ticks) and lazily initialises the frontend client on the first tick to avoid blocking startup.
After each environment-wide pull-and-push, `sync_scoped_params` reconciles per-cluster and per-replica scoped parameter overrides. It reads the `ENABLE_SCOPED_SYSTEM_PARAMETERS` feature gate from the `SynchronizedParameters` working copy (avoiding a coordinator round-trip on disabled environments). When enabled, it fetches a catalog snapshot via `Client::catalog_snapshot_expensive` (a direct coordinator round-trip, since the sync loop is not session-bound), evaluates each live cluster and replica against LaunchDarkly, and pushes the desired sparse `ScopedParameters` to the coordinator via `Client::update_scoped_system_parameters`. When disabled, it performs a single clearing push the first time it observes the feature as disabled, then does no per-tick work until it is re-enabled.
Once the frontend is initialized, it is shared with the coordinator via `Client::install_scoped_system_parameter_frontend` (as an `Arc`) so that create-cluster and create-replica paths can resolve a new object's scoped overrides synchronously within their own transaction, rather than waiting for the next tick.
`evaluate_scoped_parameters` (exported `pub(crate)`) partitions synced parameters by `ParameterScope` (`Replica` vs `Cluster`), builds `ReplicaEvalContext` and `ClusterEvalContext` structs for each live object, and delegates to `SystemParameterFrontend::pull_replica_overrides` and `pull_cluster_overrides` respectively. Optional `cluster_filter` and `replica_filter` arguments restrict evaluation to a subset of objects for the create-time synchronous resolve path.

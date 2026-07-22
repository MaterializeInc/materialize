---
source: src/adapter/src/coord/ddl.rs
revision: 8598d82c1c
---

# adapter::coord::ddl

Implements `Coordinator::catalog_transact` and related DDL helpers that combine a `catalog::transact` call with the downstream `apply_catalog_implications` step, publishing telemetry events and propagating errors back to the client.
Also provides `drop_compute_sinks`, `drop_vpc_endpoints_in_background`, and other coordinator-level housekeeping triggered by DDL.
`drop_compute_sinks` logs at `debug!` (not `error!`) when a sink is already absent, because this is a benign race that can occur when an internal subscribe is cleaned up concurrently with session disconnect processing.
`drop_vpc_endpoints_in_background` offloads VPC endpoint deletion to a background task; if `cloud_resource_controller` is absent it logs a warning and returns rather than panicking.
`catalog_transact_with_side_effects` supports DDL operations that need to run async side-effects (e.g. dropping persist shards) after the catalog transaction commits. It also tracks changes to the `OPTIMIZER_E2E_LATENCY_WARNING_THRESHOLD` system variable and propagates them to `optimizer_metrics.set_e2e_optimization_time_log_threshold` at transaction commit.
The `Op::InjectAuditEvents`, `Op::UpdateScopedSystemParameters`, and `Op::CheckClusterState` variants are recognized as no-ops for downstream implication processing.
Storage usage updates bypass `catalog_transact_inner` entirely and are written directly via `builtin_table_update`.
Connection limit checking recognizes `ConnectionDetails::GlueSchemaRegistry` and `ConnectionDetails::Gcp` alongside `Csr`, `Ssh`, `Aws`, and `IcebergCatalog` as connection types that do not count against per-type resource limits.

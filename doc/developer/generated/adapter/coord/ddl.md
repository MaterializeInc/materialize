---
source: src/adapter/src/coord/ddl.rs
revision: 44d6b9ac6a
---

# adapter::coord::ddl

Implements `Coordinator::catalog_transact` and related DDL helpers that combine a `catalog::transact` call with the downstream `apply_catalog_implications` step, publishing telemetry events and propagating errors back to the client.
Also provides `drop_compute_sinks`, `drop_vpc_endpoints_in_background`, and other coordinator-level housekeeping triggered by DDL.
`drop_vpc_endpoints_in_background` offloads VPC endpoint deletion to a background task; if `cloud_resource_controller` is absent it logs a warning and returns rather than panicking.
`catalog_transact_with_side_effects` supports DDL operations that need to run async side-effects (e.g. dropping persist shards) after the catalog transaction commits.
The `Op::InjectAuditEvents` variant is recognized as a no-op for downstream implication processing.

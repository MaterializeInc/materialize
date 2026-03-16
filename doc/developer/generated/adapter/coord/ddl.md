---
source: src/adapter/src/coord/ddl.rs
revision: 892cf626bc
---

# adapter::coord::ddl

Implements `Coordinator::catalog_transact` and related DDL helpers that combine a `catalog::transact` call with the downstream `apply_catalog_implications` step, publishing telemetry events and propagating errors back to the client.
Also provides `drop_compute_sinks`, `retire_execution`, and other coordinator-level housekeeping triggered by DDL.
`catalog_transact_with_side_effects` supports DDL operations that need to run async side-effects (e.g. dropping persist shards) after the catalog transaction commits.

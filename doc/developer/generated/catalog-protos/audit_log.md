---
source: src/catalog-protos/src/audit_log.rs
revision: 8598d82c1c
---

# mz-catalog-protos::audit_log

Implements `RustType` conversions between `mz_audit_log` Rust types and their protobuf representations in `crate::objects`.
This module exists as a separate crate from `mz_catalog` to work around Rust's orphan rules, which prevent implementing foreign traits on foreign types.
It covers all versioned `EventDetails` variants (including `CreateRoleV1`, `AlterAddColumnV1`, `AlterSourceTimestampIntervalV1`, `AlterClusterReconfigurationV1`, and `ClusterHydrationBurstV1`), `VersionedEvent`, and related enum types.
`AlterClusterReconfigurationV1` includes `ReconfigurationLifecycleV1` (variants `Started`, `Finalized`, `TimedOut`, `Cancelled`, `ResourceExhausted`) and `ClusterReplicaLoggingV1` (fields `log_logging`, `interval`).
`ClusterHydrationBurstV1` includes `HydrationBurstLifecycleV1` (`Started`, `Finished`) and `BurstFinishCauseV1` (`LingerElapsed`, `NoLongerWarranted`).
`CreateOrDropClusterReplicaReasonV1` includes reasons `Manual`, `Schedule`, `System`, `Reconfiguration`, `HydrationBurst`, and `Retired`.

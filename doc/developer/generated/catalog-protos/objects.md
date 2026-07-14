---
source: src/catalog-protos/src/objects.rs
revision: 8598d82c1c
---

# mz-catalog-protos::objects

Defines the current (v89) set of Rust structs and enums that represent all durably persisted catalog objects, generated from protobuf definitions.
This file is the canonical snapshot of the current catalog schema; `objects_v<N>.rs` files are frozen snapshots used as migration sources.
Key types include `ConfigKey`, `ConfigValue`, `SettingKey`, `SettingValue`, `IdAllocKey`, `RoleId`, `DatabaseId`, `SchemaId`, `AutoProvisionSource`, `RoleAttributes`, and many more covering every catalog entity. Scoped system configuration is represented by `ClusterSystemConfigurationKey`/`ClusterSystemConfigurationValue` (keyed by `ClusterId` and parameter name) and `ReplicaSystemConfigurationKey`/`ReplicaSystemConfigurationValue` (keyed by `ReplicaId` and parameter name), along with their corresponding `ClusterSystemConfiguration` and `ReplicaSystemConfiguration` wrapper structs and variants in `StateUpdateKind`. The `ManagedCluster` struct carries durable cluster autoscaling state: `auto_scaling_strategy` (`Option<AutoScalingStrategy>`), `reconfiguration` (`Option<ReconfigurationState>`), and `burst` (`Option<BurstState>`). `AutoScalingStrategy` holds an optional `OnHydration` sub-policy. `ReconfigurationState` records an in-flight graceful reconfiguration with a `ReconfigurationTarget`, deadline, and `OnTimeoutAction`. `BurstState` records an active hydration burst. The managed `ReplicaLocation` (`ManagedLocation`) stores `availability_zones` as a list rather than a single optional zone.
The `audit_log_event_v1` submodule includes `CreateRoleV1`, `AlterAddColumnV1`, `AlterSourceTimestampIntervalV1`, `AlterClusterReconfigurationV1` (with `ReconfigurationLifecycleV1` variants `Started`, `Finalized`, `TimedOut`, `Cancelled`, `ResourceExhausted` and `ClusterReplicaLoggingV1`), and `ClusterHydrationBurstV1` (with `HydrationBurstLifecycleV1` and `BurstFinishCauseV1` variants `LingerElapsed`, `NoLongerWarranted`) for audit-logging corresponding catalog events. `CreateOrDropClusterReplicaReasonV1` carries reasons `Manual`, `Schedule`, `System`, `Reconfiguration`, `HydrationBurst`, and `Retired`.
`ReconfigurationState` carries a `status` field (`ReconfigurationStatus`) recording the lifecycle status of the latest graceful reconfiguration.
`derive(Arbitrary)` on all types is gated behind `#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]`; the `proptest_derive` import is similarly conditional.

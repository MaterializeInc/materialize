// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Plain-data mirror of a managed cluster's durable configuration.
//!
//! These types carry the slices of a managed cluster's config that a caller
//! reasons over without touching the catalog or SQL layers. Keeping them free
//! of catalog and SQL dependencies lets one component reason over the config
//! and another project the live config onto the same types, without either
//! depending on the other.
//!
//! [`ExpectedClusterState`] is a compare-and-append witness. A caller captures
//! it from a config snapshot and pairs it with a conditional write. The applier
//! re-projects the live config and applies the write only if it still equals
//! the witness.

use std::collections::BTreeSet;
use std::time::Duration;

use mz_compute_types::config::ComputeReplicaLogging;
use mz_repr::Timestamp;

/// The availability zones a managed cluster's replicas are provisioned across,
/// in configured *provisioning order*.
///
/// The order is significant to provisioning: the orchestrator round-robins
/// replica placement across the list, so the first configured zone is filled
/// first. Equality is therefore structural and order-sensitive, and the order is
/// part of the [`ExpectedClusterState`] compare-and-append witness. To compare
/// two configurations as unordered *pools* of zones, ignoring placement order,
/// convert with [`AvailabilityZones::pool`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AvailabilityZones(pub Vec<String>);

impl AvailabilityZones {
    /// This configuration as an unordered [`AvailabilityZonePool`]: the same
    /// zones compared by membership rather than provisioning order. Two replicas
    /// drawing from the same pool are interchangeable however the lists were
    /// ordered.
    pub fn pool(&self) -> AvailabilityZonePool {
        AvailabilityZonePool(self.0.iter().cloned().collect())
    }
}

/// An unordered set of availability zones: an [`AvailabilityZones`] provisioning
/// list reduced to membership. Produced by [`AvailabilityZones::pool`] for the
/// one comparison that must ignore order, replica interchangeability.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AvailabilityZonePool(pub BTreeSet<String>);

/// The config dimensions that distinguish one replica from another. Two replicas
/// with equal shape are interchangeable. An `AVAILABILITY ZONES` difference is a
/// shape difference, but only as an unordered pool: reordering the same zones
/// does not change a replica's shape (see [`ReplicaShape::matches`]).
#[derive(Clone, Debug)]
pub struct ReplicaShape {
    pub size: String,
    pub availability_zones: AvailabilityZones,
    pub logging: ComputeReplicaLogging,
}

impl ReplicaShape {
    /// Whether two shapes are interchangeable. Availability zones are compared as
    /// unordered [pools](AvailabilityZones::pool): a replica already placed
    /// satisfies a desired shape with the same zones in a different order, so a
    /// mere reorder does not force a reprovision.
    pub fn matches(&self, other: &ReplicaShape) -> bool {
        self.size == other.size
            && self.logging == other.logging
            && self.availability_zones.pool() == other.availability_zones.pool()
    }
}

/// Compare-and-append witness over a managed cluster's durable config: the
/// fields a conditional write is conditioned on. The applier applies the write
/// only if the cluster's current config still projects to an equal witness.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpectedClusterState {
    pub size: String,
    pub replication_factor: u32,
    pub availability_zones: AvailabilityZones,
    pub logging: ComputeReplicaLogging,
    /// The scheduling policy. Part of the witness because it determines which
    /// policy owns the replica set, so a write conditioned on one schedule must
    /// not apply under another.
    pub schedule: ClusterSchedule,
    /// The autoscaling policy. Part of the witness because it determines whether,
    /// and at what size, a burst is warranted.
    pub auto_scaling_policy: Option<AutoScalingPolicy>,
    pub reconfiguration: Option<ReconfigurationRecord>,
    pub burst: Option<BurstRecord>,
}

/// An in-flight graceful reconfiguration record, mirrored from durable state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconfigurationRecord {
    pub target: ReconfigurationTarget,
    pub deadline: Timestamp,
    /// What to do once the deadline passes with the target not yet hydrated.
    pub on_timeout: OnTimeout,
}

/// The action a graceful reconfiguration applies once its `deadline` passes with
/// the target replicas not yet hydrated. Success always takes precedence: a
/// hydrated target cuts over regardless of this. A plain-data mirror of
/// `mz_sql::plan::OnTimeoutAction`, free of a dependency on the SQL layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnTimeout {
    /// Cut over to the (not-yet-hydrated) target anyway and clear the record.
    Commit,
    /// Drop the target replica set, reverting to the pre-reconfiguration shape,
    /// and clear the record without advancing the realized config.
    Rollback,
}

/// The full config shape a reconfiguration is moving the cluster to. Distinct
/// from a replica shape because it additionally carries `replication_factor`, a
/// cluster-level rather than replica-level dimension.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconfigurationTarget {
    pub size: String,
    pub replication_factor: u32,
    pub availability_zones: AvailabilityZones,
    pub logging: ComputeReplicaLogging,
}

impl ReconfigurationTarget {
    /// The per-replica shape of the target: everything but `replication_factor`.
    pub fn shape(&self) -> ReplicaShape {
        ReplicaShape {
            size: self.size.clone(),
            availability_zones: self.availability_zones.clone(),
            logging: self.logging.clone(),
        }
    }
}

/// An active hydration-burst record, mirrored from durable state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BurstRecord {
    pub burst_size: String,
    pub linger_duration: Duration,
    pub steady_hydrated_at: Option<Timestamp>,
}

/// A managed cluster's scheduling policy, mirrored from durable state. A
/// plain-data mirror of `mz_sql::plan::ClusterSchedule`, free of a dependency on
/// the SQL layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterSchedule {
    /// The cluster is user-managed: `replication_factor` is the capacity knob
    /// that sizes the replica set.
    Manual,
    /// The cluster is scheduled `ON REFRESH`: `replication_factor` is held at `0`
    /// and replicas run only around refresh times. `hydration_time_estimate` is
    /// how far ahead of a refresh the cluster should turn on so it can rehydrate
    /// before the refresh time.
    Refresh { hydration_time_estimate: Duration },
}

/// The user-configured autoscaling policy of a managed cluster, mirrored from
/// durable state. A plain-data mirror of `mz_sql::plan::AutoScalingStrategy`,
/// free of a dependency on the SQL layer.
///
/// Extensible: future strategies are additional optional sub-policies. v1 carries
/// only the `ON HYDRATION` burst policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AutoScalingPolicy {
    pub on_hydration: Option<OnHydrationPolicy>,
}

/// The `ON HYDRATION` burst sub-policy: while some object on the cluster is not
/// hydrated on a steady replica, run one extra replica at `hydration_size` to
/// accelerate hydration, lingering for `linger_duration` after the steady set
/// hydrates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OnHydrationPolicy {
    pub hydration_size: String,
    /// `None` falls back to the system default linger when the burst strategy
    /// writes its record.
    pub linger_duration: Option<Duration>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn azs(zones: &[&str]) -> AvailabilityZones {
        AvailabilityZones(zones.iter().map(|z| z.to_string()).collect())
    }

    fn shape(zones: &[&str]) -> ReplicaShape {
        ReplicaShape {
            size: "small".to_string(),
            availability_zones: azs(zones),
            logging: ComputeReplicaLogging::default(),
        }
    }

    #[mz_ore::test]
    fn availability_zones_identity_is_ordered_pool_is_not() {
        // Provisioning order is part of the value's identity, so a reorder is a
        // distinct configuration that the compare-and-append witness can see.
        assert_ne!(azs(&["a", "b"]), azs(&["b", "a"]));
        // The pool drops the order: the same zones in any order are one pool, but
        // a different set of zones is a different pool.
        assert_eq!(azs(&["a", "b"]).pool(), azs(&["b", "a"]).pool());
        assert_ne!(azs(&["a", "b"]).pool(), azs(&["a", "c"]).pool());
    }

    #[mz_ore::test]
    fn replica_shape_matches_ignores_zone_order() {
        // Interchangeability ignores provisioning order: an already-placed
        // replica satisfies a desired shape with the same zones reordered, so a
        // reorder alone never forces a reprovision.
        assert!(shape(&["a", "b"]).matches(&shape(&["b", "a"])));
        // A different zone set is a different shape.
        assert!(!shape(&["a", "b"]).matches(&shape(&["a", "c"])));
    }
}

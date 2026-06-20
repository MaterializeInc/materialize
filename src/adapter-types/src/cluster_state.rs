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

/// A pool of availability zones, compared as a set. Stored as a `Vec` to
/// preserve the configured provisioning order, but two pools holding the same
/// zones in a different order are equal.
#[derive(Clone, Debug, Default)]
pub struct AvailabilityZones(pub Vec<String>);

impl PartialEq for AvailabilityZones {
    fn eq(&self, other: &Self) -> bool {
        self.0.iter().collect::<BTreeSet<_>>() == other.0.iter().collect::<BTreeSet<_>>()
    }
}

impl Eq for AvailabilityZones {}

/// The config dimensions that distinguish one replica from another. Two replicas
/// with equal shape are interchangeable. An `AVAILABILITY ZONES` difference is a
/// shape difference. See [`AvailabilityZones`] for how the pool is compared.
#[derive(Clone, Debug)]
pub struct ReplicaShape {
    pub size: String,
    pub availability_zones: AvailabilityZones,
    pub logging: ComputeReplicaLogging,
}

impl ReplicaShape {
    /// Whether two shapes are interchangeable.
    pub fn matches(&self, other: &ReplicaShape) -> bool {
        self.size == other.size
            && self.logging == other.logging
            && self.availability_zones == other.availability_zones
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
    pub reconfiguration: Option<ReconfigurationRecord>,
    pub burst: Option<BurstRecord>,
}

/// An in-flight graceful reconfiguration record, mirrored from durable state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconfigurationRecord {
    pub target: ReconfigurationTarget,
    pub deadline: Timestamp,
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

/// An active hydration-burst record, mirrored from durable state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BurstRecord {
    pub burst_size: String,
    pub linger_duration: Duration,
    pub steady_hydrated_at: Option<Timestamp>,
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::traits::{UpgradeFrom, UpgradeInto};
use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::wire_compatible::{WireCompatible, wire_compatible};
use crate::durable::upgrade::{objects_v75 as v75, objects_v76 as v76};

wire_compatible!(v75::ClusterKey with v76::ClusterKey);
wire_compatible!(v75::ClusterReplicaKey with v76::ClusterReplicaKey);
wire_compatible!(v75::ClusterId with v76::ClusterId);
wire_compatible!(v75::RoleId with v76::RoleId);
wire_compatible!(v75::MzAclItem with v76::MzAclItem);
wire_compatible!(v75::ReplicaLogging with v76::ReplicaLogging);
wire_compatible!(v75::replica_config::UnmanagedLocation with v76::replica_config::UnmanagedLocation);
wire_compatible!(v75::OptimizerFeatureOverride with v76::OptimizerFeatureOverride);
wire_compatible!(v75::ClusterSchedule with v76::ClusterSchedule);

/// Removes the `disk` flag from managed cluster and replica configs.
pub fn upgrade(
    snapshot: Vec<v75::StateUpdateKind>,
) -> Vec<MigrationAction<v75::StateUpdateKind, v76::StateUpdateKind>> {
    let mut migrations = Vec::new();
    for update in snapshot {
        match update.kind {
            Some(v75::state_update_kind::Kind::Cluster(old_cluster)) => {
                let new_cluster =
                    v76::state_update_kind::Cluster::upgrade_from(old_cluster.clone());
                let old = v75::StateUpdateKind {
                    kind: Some(v75::state_update_kind::Kind::Cluster(old_cluster)),
                };
                let new = v76::StateUpdateKind {
                    kind: Some(v76::state_update_kind::Kind::Cluster(new_cluster)),
                };

                let migration = MigrationAction::Update(old, new);
                migrations.push(migration);
            }
            Some(v75::state_update_kind::Kind::ClusterReplica(old_replica)) => {
                let new_replica =
                    v76::state_update_kind::ClusterReplica::upgrade_from(old_replica.clone());
                let old = v75::StateUpdateKind {
                    kind: Some(v75::state_update_kind::Kind::ClusterReplica(old_replica)),
                };
                let new = v76::StateUpdateKind {
                    kind: Some(v76::state_update_kind::Kind::ClusterReplica(new_replica)),
                };

                let migration = MigrationAction::Update(old, new);
                migrations.push(migration);
            }
            _ => {}
        }
    }
    migrations
}

impl UpgradeFrom<v75::state_update_kind::Cluster> for v76::state_update_kind::Cluster {
    fn upgrade_from(old: v75::state_update_kind::Cluster) -> Self {
        v76::state_update_kind::Cluster {
            key: old.key.as_ref().map(WireCompatible::convert),
            value: old.value.map(UpgradeFrom::upgrade_from),
        }
    }
}

impl UpgradeFrom<v75::ClusterValue> for v76::ClusterValue {
    fn upgrade_from(old: v75::ClusterValue) -> Self {
        v76::ClusterValue {
            name: old.name,
            owner_id: old.owner_id.as_ref().map(WireCompatible::convert),
            privileges: old.privileges.iter().map(WireCompatible::convert).collect(),
            config: old.config.map(UpgradeFrom::upgrade_from),
        }
    }
}

impl UpgradeFrom<v75::ClusterConfig> for v76::ClusterConfig {
    fn upgrade_from(old: v75::ClusterConfig) -> Self {
        v76::ClusterConfig {
            workload_class: old.workload_class,
            variant: old.variant.map(UpgradeFrom::upgrade_from),
        }
    }
}

impl UpgradeFrom<v75::cluster_config::Variant> for v76::cluster_config::Variant {
    fn upgrade_from(old: v75::cluster_config::Variant) -> Self {
        match old {
            v75::cluster_config::Variant::Unmanaged(_empty) => {
                v76::cluster_config::Variant::Unmanaged(v76::Empty {})
            }
            v75::cluster_config::Variant::Managed(managed) => {
                v76::cluster_config::Variant::Managed(managed.upgrade_into())
            }
        }
    }
}

impl UpgradeFrom<v75::cluster_config::ManagedCluster> for v76::cluster_config::ManagedCluster {
    fn upgrade_from(loc: v75::cluster_config::ManagedCluster) -> Self {
        v76::cluster_config::ManagedCluster {
            size: loc.size,
            replication_factor: loc.replication_factor,
            availability_zones: loc.availability_zones,
            logging: loc.logging.as_ref().map(WireCompatible::convert),
            optimizer_feature_overrides: loc
                .optimizer_feature_overrides
                .iter()
                .map(WireCompatible::convert)
                .collect(),
            schedule: loc.schedule.as_ref().map(WireCompatible::convert),
        }
    }
}

impl UpgradeFrom<v75::state_update_kind::ClusterReplica>
    for v76::state_update_kind::ClusterReplica
{
    fn upgrade_from(old: v75::state_update_kind::ClusterReplica) -> Self {
        v76::state_update_kind::ClusterReplica {
            key: old.key.as_ref().map(WireCompatible::convert),
            value: old.value.map(UpgradeFrom::upgrade_from),
        }
    }
}

impl UpgradeFrom<v75::ClusterReplicaValue> for v76::ClusterReplicaValue {
    fn upgrade_from(old: v75::ClusterReplicaValue) -> Self {
        v76::ClusterReplicaValue {
            cluster_id: old.cluster_id.as_ref().map(WireCompatible::convert),
            name: old.name,
            config: old.config.map(UpgradeFrom::upgrade_from),
            owner_id: old.owner_id.as_ref().map(WireCompatible::convert),
        }
    }
}

impl UpgradeFrom<v75::ReplicaConfig> for v76::ReplicaConfig {
    fn upgrade_from(old: v75::ReplicaConfig) -> Self {
        v76::ReplicaConfig {
            logging: old.logging.as_ref().map(WireCompatible::convert),
            location: old.location.map(UpgradeFrom::upgrade_from),
        }
    }
}

impl UpgradeFrom<v75::replica_config::Location> for v76::replica_config::Location {
    fn upgrade_from(old: v75::replica_config::Location) -> Self {
        match old {
            v75::replica_config::Location::Unmanaged(loc) => {
                v76::replica_config::Location::Unmanaged(WireCompatible::convert(&loc))
            }
            v75::replica_config::Location::Managed(loc) => {
                v76::replica_config::Location::Managed(loc.upgrade_into())
            }
        }
    }
}

impl UpgradeFrom<v75::replica_config::ManagedLocation> for v76::replica_config::ManagedLocation {
    fn upgrade_from(loc: v75::replica_config::ManagedLocation) -> Self {
        v76::replica_config::ManagedLocation {
            size: loc.size,
            availability_zone: loc.availability_zone,
            internal: loc.internal,
            billed_as: loc.billed_as,
            pending: loc.pending,
        }
    }
}

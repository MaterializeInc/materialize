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
use crate::durable::upgrade::{objects_v74 as v74, objects_v75 as v75};

wire_compatible!(v74::ClusterReplicaKey with v75::ClusterReplicaKey);
wire_compatible!(v74::ClusterId with v75::ClusterId);
wire_compatible!(v74::RoleId with v75::RoleId);
wire_compatible!(v74::ReplicaLogging with v75::ReplicaLogging);
wire_compatible!(v74::replica_config::ManagedLocation with v75::replica_config::ManagedLocation);

/// Removes some options from unmanaged (unorchestrated?) replica configs.
pub fn upgrade(
    snapshot: Vec<v74::StateUpdateKind>,
) -> Vec<MigrationAction<v74::StateUpdateKind, v75::StateUpdateKind>> {
    let mut migrations = Vec::new();
    for update in snapshot {
        match update.kind {
            Some(v74::state_update_kind::Kind::ClusterReplica(old_replica)) => {
                let new_replica =
                    v75::state_update_kind::ClusterReplica::upgrade_from(old_replica.clone());
                let old = v74::StateUpdateKind {
                    kind: Some(v74::state_update_kind::Kind::ClusterReplica(old_replica)),
                };
                let new = v75::StateUpdateKind {
                    kind: Some(v75::state_update_kind::Kind::ClusterReplica(new_replica)),
                };

                let migration = MigrationAction::Update(old, new);
                migrations.push(migration);
            }
            _ => {}
        }
    }
    migrations
}

impl UpgradeFrom<v74::state_update_kind::ClusterReplica>
    for v75::state_update_kind::ClusterReplica
{
    fn upgrade_from(old: v74::state_update_kind::ClusterReplica) -> Self {
        v75::state_update_kind::ClusterReplica {
            key: old.key.as_ref().map(WireCompatible::convert),
            value: old.value.map(UpgradeFrom::upgrade_from),
        }
    }
}

impl UpgradeFrom<v74::ClusterReplicaValue> for v75::ClusterReplicaValue {
    fn upgrade_from(old: v74::ClusterReplicaValue) -> Self {
        v75::ClusterReplicaValue {
            cluster_id: old.cluster_id.as_ref().map(WireCompatible::convert),
            name: old.name,
            config: old.config.map(UpgradeFrom::upgrade_from),
            owner_id: old.owner_id.as_ref().map(WireCompatible::convert),
        }
    }
}

impl UpgradeFrom<v74::ReplicaConfig> for v75::ReplicaConfig {
    fn upgrade_from(old: v74::ReplicaConfig) -> Self {
        v75::ReplicaConfig {
            logging: old.logging.as_ref().map(WireCompatible::convert),
            location: old.location.map(UpgradeFrom::upgrade_from),
        }
    }
}

impl UpgradeFrom<v74::replica_config::Location> for v75::replica_config::Location {
    fn upgrade_from(old: v74::replica_config::Location) -> Self {
        match old {
            v74::replica_config::Location::Unmanaged(loc) => {
                v75::replica_config::Location::Unmanaged(loc.upgrade_into())
            }
            v74::replica_config::Location::Managed(loc) => {
                v75::replica_config::Location::Managed(WireCompatible::convert(&loc))
            }
        }
    }
}

impl UpgradeFrom<v74::replica_config::UnmanagedLocation>
    for v75::replica_config::UnmanagedLocation
{
    fn upgrade_from(loc: v74::replica_config::UnmanagedLocation) -> Self {
        v75::replica_config::UnmanagedLocation {
            storagectl_addrs: loc.storagectl_addrs,
            computectl_addrs: loc.computectl_addrs,
        }
    }
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::json_compatible::JsonCompatible;
use crate::durable::upgrade::objects_v89 as v89;
use crate::durable::upgrade::objects_v90 as v90;

crate::json_compatible!(v89::ClusterKey with v90::ClusterKey);
crate::json_compatible!(v89::ClusterReplicaKey with v90::ClusterReplicaKey);
crate::json_compatible!(v89::ClusterId with v90::ClusterId);
crate::json_compatible!(v89::RoleId with v90::RoleId);
crate::json_compatible!(v89::MzAclItem with v90::MzAclItem);
crate::json_compatible!(v89::ReplicaLogging with v90::ReplicaLogging);
crate::json_compatible!(v89::ReplicaLocation with v90::ReplicaLocation);
crate::json_compatible!(v89::OptimizerFeatureOverride with v90::OptimizerFeatureOverride);
crate::json_compatible!(v89::ClusterSchedule with v90::ClusterSchedule);
crate::json_compatible!(v89::AutoScalingStrategy with v90::AutoScalingStrategy);
crate::json_compatible!(v89::BurstState with v90::BurstState);
crate::json_compatible!(v89::ReconfigurationStatus with v90::ReconfigurationStatus);
crate::json_compatible!(v89::OnTimeoutAction with v90::OnTimeoutAction);

/// Adds the `arrangement_compression` flag to managed clusters and cluster
/// replicas, backfilling it as `false`. The flag also appears on a managed
/// cluster's in-flight `reconfiguration` target, backfilled the same way.
///
/// Managed `Cluster` and `ClusterReplica` records gained a new field, so their
/// stored JSON is no longer readable as the v90 type. Every such record is
/// rewritten. Unmanaged clusters and all other records are unchanged and pass
/// through untouched.
pub fn upgrade(
    snapshot: Vec<v89::StateUpdateKind>,
) -> Vec<MigrationAction<v89::StateUpdateKind, v90::StateUpdateKind>> {
    let mut migrations = Vec::new();
    for update in snapshot {
        match update {
            v89::StateUpdateKind::Cluster(old_cluster)
                if matches!(
                    old_cluster.value.config.variant,
                    v89::ClusterVariant::Managed(_)
                ) =>
            {
                let new_cluster = migrate_cluster(old_cluster.clone());
                migrations.push(MigrationAction::Update(
                    v89::StateUpdateKind::Cluster(old_cluster),
                    v90::StateUpdateKind::Cluster(new_cluster),
                ));
            }
            v89::StateUpdateKind::ClusterReplica(old_replica) => {
                let new_replica = migrate_replica(old_replica.clone());
                migrations.push(MigrationAction::Update(
                    v89::StateUpdateKind::ClusterReplica(old_replica),
                    v90::StateUpdateKind::ClusterReplica(new_replica),
                ));
            }
            _ => {}
        }
    }
    migrations
}

fn migrate_cluster(old: v89::Cluster) -> v90::Cluster {
    let v89::Cluster { key, value } = old;
    let v89::ClusterVariant::Managed(m) = value.config.variant else {
        unreachable!("caller guards on the managed variant");
    };
    v90::Cluster {
        key: JsonCompatible::convert(&key),
        value: v90::ClusterValue {
            name: value.name,
            owner_id: JsonCompatible::convert(&value.owner_id),
            privileges: value
                .privileges
                .iter()
                .map(JsonCompatible::convert)
                .collect(),
            config: v90::ClusterConfig {
                workload_class: value.config.workload_class,
                variant: v90::ClusterVariant::Managed(migrate_managed(m)),
            },
        },
    }
}

fn migrate_managed(m: v89::ManagedCluster) -> v90::ManagedCluster {
    v90::ManagedCluster {
        size: m.size,
        replication_factor: m.replication_factor,
        availability_zones: m.availability_zones,
        logging: JsonCompatible::convert(&m.logging),
        arrangement_compression: false,
        optimizer_feature_overrides: m
            .optimizer_feature_overrides
            .iter()
            .map(JsonCompatible::convert)
            .collect(),
        schedule: JsonCompatible::convert(&m.schedule),
        auto_scaling_strategy: m
            .auto_scaling_strategy
            .as_ref()
            .map(JsonCompatible::convert),
        reconfiguration: m.reconfiguration.map(migrate_reconfiguration),
        burst: m.burst.as_ref().map(JsonCompatible::convert),
    }
}

fn migrate_reconfiguration(old: v89::ReconfigurationState) -> v90::ReconfigurationState {
    let v89::ReconfigurationTarget {
        size,
        replication_factor,
        availability_zones,
        logging,
    } = old.target;
    v90::ReconfigurationState {
        target: v90::ReconfigurationTarget {
            size,
            replication_factor,
            availability_zones,
            logging: JsonCompatible::convert(&logging),
            arrangement_compression: false,
        },
        deadline: old.deadline,
        on_timeout: JsonCompatible::convert(&old.on_timeout),
        status: JsonCompatible::convert(&old.status),
    }
}

fn migrate_replica(old: v89::ClusterReplica) -> v90::ClusterReplica {
    let v89::ClusterReplica { key, value } = old;
    let v89::ReplicaConfig { logging, location } = value.config;
    v90::ClusterReplica {
        key: JsonCompatible::convert(&key),
        value: v90::ClusterReplicaValue {
            cluster_id: JsonCompatible::convert(&value.cluster_id),
            name: value.name,
            config: v90::ReplicaConfig {
                logging: JsonCompatible::convert(&logging),
                location: JsonCompatible::convert(&location),
                arrangement_compression: false,
            },
            owner_id: JsonCompatible::convert(&value.owner_id),
        },
    }
}

#[cfg(test)]
mod tests {
    use crate::durable::upgrade::MigrationAction;
    use crate::durable::upgrade::v89_to_v90::upgrade;
    use crate::durable::upgrade::{objects_v89 as v89, objects_v90 as v90};

    fn managed_cluster(id: u64) -> v89::Cluster {
        v89::Cluster {
            key: v89::ClusterKey {
                id: v89::ClusterId::User(id),
            },
            value: v89::ClusterValue {
                name: format!("cluster{id}"),
                owner_id: v89::RoleId::User(1),
                privileges: Vec::new(),
                config: v89::ClusterConfig {
                    workload_class: None,
                    variant: v89::ClusterVariant::Managed(v89::ManagedCluster {
                        size: "small".to_string(),
                        replication_factor: 1,
                        availability_zones: vec!["az1".to_string()],
                        logging: v89::ReplicaLogging {
                            log_logging: false,
                            interval: None,
                        },
                        optimizer_feature_overrides: Vec::new(),
                        schedule: v89::ClusterSchedule::Manual,
                        auto_scaling_strategy: None,
                        reconfiguration: None,
                        burst: None,
                    }),
                },
            },
        }
    }

    fn unmanaged_cluster(id: u64) -> v89::Cluster {
        v89::Cluster {
            key: v89::ClusterKey {
                id: v89::ClusterId::User(id),
            },
            value: v89::ClusterValue {
                name: format!("cluster{id}"),
                owner_id: v89::RoleId::User(1),
                privileges: Vec::new(),
                config: v89::ClusterConfig {
                    workload_class: None,
                    variant: v89::ClusterVariant::Unmanaged,
                },
            },
        }
    }

    fn replica(id: u64) -> v89::ClusterReplica {
        v89::ClusterReplica {
            key: v89::ClusterReplicaKey {
                id: v89::ReplicaId::User(id),
            },
            value: v89::ClusterReplicaValue {
                cluster_id: v89::ClusterId::User(1),
                name: format!("r{id}"),
                config: v89::ReplicaConfig {
                    logging: v89::ReplicaLogging {
                        log_logging: false,
                        interval: None,
                    },
                    location: v89::ReplicaLocation::Managed(v89::ManagedLocation {
                        size: "small".to_string(),
                        availability_zones: vec!["az1".to_string()],
                        billed_as: None,
                        internal: false,
                        pending: false,
                    }),
                },
                owner_id: v89::RoleId::User(1),
            },
        }
    }

    #[mz_ore::test]
    fn backfills_managed_cluster_and_replica_as_false() {
        let migrations = upgrade(vec![
            v89::StateUpdateKind::Cluster(managed_cluster(1)),
            v89::StateUpdateKind::Cluster(unmanaged_cluster(2)),
            v89::StateUpdateKind::ClusterReplica(replica(1)),
        ]);
        // Managed cluster and replica migrate; the unmanaged cluster passes through.
        assert_eq!(migrations.len(), 2);

        let MigrationAction::Update(_, v90::StateUpdateKind::Cluster(cluster)) = &migrations[0]
        else {
            panic!("expected a cluster update");
        };
        let v90::ClusterVariant::Managed(managed) = &cluster.value.config.variant else {
            panic!("expected a managed cluster");
        };
        assert!(!managed.arrangement_compression);

        let MigrationAction::Update(_, v90::StateUpdateKind::ClusterReplica(replica)) =
            &migrations[1]
        else {
            panic!("expected a replica update");
        };
        assert!(!replica.value.config.arrangement_compression);
    }
}

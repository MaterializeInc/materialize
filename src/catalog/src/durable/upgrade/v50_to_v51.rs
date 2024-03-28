// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_stash::upgrade::WireCompatible;
use mz_stash::wire_compatible;
use tracing::warn;

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v50 as v50, objects_v51 as v51};

wire_compatible!(v50::ClusterId with v51::ClusterId);
wire_compatible!(v50::ClusterKey with v51::ClusterKey);
wire_compatible!(v50::ClusterReplicaKey with v51::ClusterReplicaKey);
wire_compatible!(v50::ClusterScheduleOptionValue with v51::ClusterScheduleOptionValue);
wire_compatible!(v50::MzAclItem with v51::MzAclItem);
wire_compatible!(v50::OptimizerFeatureOverride with v51::OptimizerFeatureOverride);
wire_compatible!(v50::ReplicaLogging with v51::ReplicaLogging);
wire_compatible!(v50::replica_config::ManagedLocation with v51::replica_config::ManagedLocation);
wire_compatible!(v50::replica_config::UnmanagedLocation with v51::replica_config::UnmanagedLocation);
wire_compatible!(v50::RoleId with v51::RoleId);

/// Replace `idle_arrangement_merge_effort` cluster/replica option with `arrangement_merge_effort`.
///
/// Because the semantics differ, there is no way to translate values 1-to-1. We can keep '0'
/// values (which disable continuous merging), but we can't translate other values. So instead we
/// translate all non-'0' values to the default.
///
/// We don't expect to find any non-default values in existing environments anyway, but just in
/// case this assumption is wrong we log any occurrences on non-default values so we can manually
/// follow up on them.
pub fn upgrade(
    snapshot: Vec<v50::StateUpdateKind>,
) -> Vec<MigrationAction<v50::StateUpdateKind, v51::StateUpdateKind>> {
    snapshot
        .into_iter()
        .filter_map(|update| {
            let old = update.clone();
            match update.kind {
                Some(v50::state_update_kind::Kind::Cluster(cluster)) => {
                    let new = v51::StateUpdateKind {
                        kind: Some(v51::state_update_kind::Kind::Cluster(
                            v51::state_update_kind::Cluster {
                                key: convert_option(cluster.key),
                                value: cluster.value.map(|value| v51::ClusterValue {
                                    name: value.name,
                                    owner_id: convert_option(value.owner_id),
                                    privileges: convert_vec(value.privileges),
                                    config: value.config.map(|config| v51::ClusterConfig {
                                        variant: config.variant.map(|variant| match variant {
                                            v50::cluster_config::Variant::Unmanaged(_) => {
                                                v51::cluster_config::Variant::Unmanaged(
                                                    v51::Empty {},
                                                )
                                            }
                                            v50::cluster_config::Variant::Managed(managed) => {
                                                v51::cluster_config::Variant::Managed(
                                                    v51::cluster_config::ManagedCluster {
                                                        size: managed.size,
                                                        replication_factor: managed
                                                            .replication_factor,
                                                        availability_zones: managed
                                                            .availability_zones,
                                                        logging: convert_option(managed.logging),
                                                        arrangement_merge_effort:
                                                            translate_merge_effort(
                                                                managed
                                                                    .idle_arrangement_merge_effort,
                                                            ),
                                                        disk: managed.disk,
                                                        optimizer_feature_overrides: convert_vec(
                                                            managed.optimizer_feature_overrides,
                                                        ),
                                                        schedule: convert_option(managed.schedule),
                                                    },
                                                )
                                            }
                                        }),
                                    }),
                                }),
                            },
                        )),
                    };
                    Some(MigrationAction::Update(old, new))
                }
                Some(v50::state_update_kind::Kind::ClusterReplica(replica)) => {
                    let new = v51::StateUpdateKind {
                        kind: Some(v51::state_update_kind::Kind::ClusterReplica(
                            v51::state_update_kind::ClusterReplica {
                                key: convert_option(replica.key),
                                value: replica.value.map(|value| v51::ClusterReplicaValue {
                                    cluster_id: convert_option(value.cluster_id),
                                    name: value.name,
                                    config: value.config.map(|config| v51::ReplicaConfig {
                                        logging: convert_option(config.logging),
                                        arrangement_merge_effort: translate_merge_effort(
                                            config.idle_arrangement_merge_effort,
                                        ),
                                        location: config.location.map(|location| match location {
                                            v50::replica_config::Location::Unmanaged(unmanaged) => {
                                                v51::replica_config::Location::Unmanaged(convert(
                                                    unmanaged,
                                                ))
                                            }
                                            v50::replica_config::Location::Managed(managed) => {
                                                v51::replica_config::Location::Managed(convert(
                                                    managed,
                                                ))
                                            }
                                        }),
                                    }),
                                    owner_id: convert_option(value.owner_id),
                                }),
                            },
                        )),
                    };
                    Some(MigrationAction::Update(old, new))
                }
                _ => None,
            }
        })
        .collect()
}

fn convert<T1, T2>(v: T1) -> T2
where
    T1: prost::Message,
    T2: prost::Message + WireCompatible<T1>,
{
    WireCompatible::convert(&v)
}

fn convert_option<T1, T2>(v: Option<T1>) -> Option<T2>
where
    T1: prost::Message,
    T2: prost::Message + WireCompatible<T1>,
{
    v.as_ref().map(WireCompatible::convert)
}

fn convert_vec<T1, T2>(v: Vec<T1>) -> Vec<T2>
where
    T1: prost::Message,
    T2: prost::Message + WireCompatible<T1>,
{
    v.iter().map(WireCompatible::convert).collect()
}

fn translate_merge_effort(effort: Option<v50::ReplicaMergeEffort>) -> Option<u32> {
    match effort.map(|e| e.effort) {
        Some(0) => Some(0),
        Some(value) => {
            warn!(%value, "untranslatable `idle_arrangement_merge_effort`");
            None
        }
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test(tokio::test)]
    async fn migrate_default_effort() {
        let v50_cluster = v50::StateUpdateKind {
            kind: Some(v50::state_update_kind::Kind::Cluster(
                v50::state_update_kind::Cluster {
                    value: Some(v50::ClusterValue {
                        config: Some(v50::ClusterConfig {
                            variant: Some(v50::cluster_config::Variant::Managed(
                                v50::cluster_config::ManagedCluster {
                                    idle_arrangement_merge_effort: None,
                                    ..Default::default()
                                },
                            )),
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        };
        let v50_replica = v50::StateUpdateKind {
            kind: Some(v50::state_update_kind::Kind::ClusterReplica(
                v50::state_update_kind::ClusterReplica {
                    value: Some(v50::ClusterReplicaValue {
                        config: Some(v50::ReplicaConfig {
                            idle_arrangement_merge_effort: None,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        };

        let v51_cluster = v51::StateUpdateKind {
            kind: Some(v51::state_update_kind::Kind::Cluster(
                v51::state_update_kind::Cluster {
                    value: Some(v51::ClusterValue {
                        config: Some(v51::ClusterConfig {
                            variant: Some(v51::cluster_config::Variant::Managed(
                                v51::cluster_config::ManagedCluster {
                                    arrangement_merge_effort: None,
                                    ..Default::default()
                                },
                            )),
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        };
        let v51_replica = v51::StateUpdateKind {
            kind: Some(v51::state_update_kind::Kind::ClusterReplica(
                v51::state_update_kind::ClusterReplica {
                    value: Some(v51::ClusterReplicaValue {
                        config: Some(v51::ReplicaConfig {
                            arrangement_merge_effort: None,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        };

        let actions = crate::durable::upgrade::v50_to_v51::upgrade(vec![
            v50_cluster.clone(),
            v50_replica.clone(),
        ]);
        let expected = vec![
            MigrationAction::Update(v50_cluster, v51_cluster),
            MigrationAction::Update(v50_replica, v51_replica),
        ];
        assert_eq!(actions, expected);
    }

    #[mz_ore::test(tokio::test)]
    async fn migrate_zero_effort() {
        let v50_cluster = v50::StateUpdateKind {
            kind: Some(v50::state_update_kind::Kind::Cluster(
                v50::state_update_kind::Cluster {
                    value: Some(v50::ClusterValue {
                        config: Some(v50::ClusterConfig {
                            variant: Some(v50::cluster_config::Variant::Managed(
                                v50::cluster_config::ManagedCluster {
                                    idle_arrangement_merge_effort: Some(v50::ReplicaMergeEffort {
                                        effort: 0,
                                    }),
                                    ..Default::default()
                                },
                            )),
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        };
        let v50_replica = v50::StateUpdateKind {
            kind: Some(v50::state_update_kind::Kind::ClusterReplica(
                v50::state_update_kind::ClusterReplica {
                    value: Some(v50::ClusterReplicaValue {
                        config: Some(v50::ReplicaConfig {
                            idle_arrangement_merge_effort: Some(v50::ReplicaMergeEffort {
                                effort: 0,
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        };

        let v51_cluster = v51::StateUpdateKind {
            kind: Some(v51::state_update_kind::Kind::Cluster(
                v51::state_update_kind::Cluster {
                    value: Some(v51::ClusterValue {
                        config: Some(v51::ClusterConfig {
                            variant: Some(v51::cluster_config::Variant::Managed(
                                v51::cluster_config::ManagedCluster {
                                    arrangement_merge_effort: Some(0),
                                    ..Default::default()
                                },
                            )),
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        };
        let v51_replica = v51::StateUpdateKind {
            kind: Some(v51::state_update_kind::Kind::ClusterReplica(
                v51::state_update_kind::ClusterReplica {
                    value: Some(v51::ClusterReplicaValue {
                        config: Some(v51::ReplicaConfig {
                            arrangement_merge_effort: Some(0),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        };

        let actions = crate::durable::upgrade::v50_to_v51::upgrade(vec![
            v50_cluster.clone(),
            v50_replica.clone(),
        ]);
        let expected = vec![
            MigrationAction::Update(v50_cluster, v51_cluster),
            MigrationAction::Update(v50_replica, v51_replica),
        ];
        assert_eq!(actions, expected);
    }

    #[mz_ore::test(tokio::test)]
    async fn migrate_nonzero_effort() {
        let v50_cluster = v50::StateUpdateKind {
            kind: Some(v50::state_update_kind::Kind::Cluster(
                v50::state_update_kind::Cluster {
                    value: Some(v50::ClusterValue {
                        config: Some(v50::ClusterConfig {
                            variant: Some(v50::cluster_config::Variant::Managed(
                                v50::cluster_config::ManagedCluster {
                                    idle_arrangement_merge_effort: Some(v50::ReplicaMergeEffort {
                                        effort: 42,
                                    }),
                                    ..Default::default()
                                },
                            )),
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        };
        let v50_replica = v50::StateUpdateKind {
            kind: Some(v50::state_update_kind::Kind::ClusterReplica(
                v50::state_update_kind::ClusterReplica {
                    value: Some(v50::ClusterReplicaValue {
                        config: Some(v50::ReplicaConfig {
                            idle_arrangement_merge_effort: Some(v50::ReplicaMergeEffort {
                                effort: 42,
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        };

        let v51_cluster = v51::StateUpdateKind {
            kind: Some(v51::state_update_kind::Kind::Cluster(
                v51::state_update_kind::Cluster {
                    value: Some(v51::ClusterValue {
                        config: Some(v51::ClusterConfig {
                            variant: Some(v51::cluster_config::Variant::Managed(
                                v51::cluster_config::ManagedCluster {
                                    arrangement_merge_effort: None,
                                    ..Default::default()
                                },
                            )),
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        };
        let v51_replica = v51::StateUpdateKind {
            kind: Some(v51::state_update_kind::Kind::ClusterReplica(
                v51::state_update_kind::ClusterReplica {
                    value: Some(v51::ClusterReplicaValue {
                        config: Some(v51::ReplicaConfig {
                            arrangement_merge_effort: None,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        };

        let actions = crate::durable::upgrade::v50_to_v51::upgrade(vec![
            v50_cluster.clone(),
            v50_replica.clone(),
        ]);
        let expected = vec![
            MigrationAction::Update(v50_cluster, v51_cluster),
            MigrationAction::Update(v50_replica, v51_replica),
        ];
        assert_eq!(actions, expected);
    }
}

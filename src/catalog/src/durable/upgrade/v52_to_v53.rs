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

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v52 as v52, objects_v53 as v53};

wire_compatible!(v52::ClusterId with v53::ClusterId);
wire_compatible!(v52::ClusterKey with v53::ClusterKey);
wire_compatible!(v52::ClusterReplicaKey with v53::ClusterReplicaKey);
wire_compatible!(v52::ClusterScheduleOptionValue with v53::ClusterScheduleOptionValue);
wire_compatible!(v52::MzAclItem with v53::MzAclItem);
wire_compatible!(v52::OptimizerFeatureOverride with v53::OptimizerFeatureOverride);
wire_compatible!(v52::ReplicaLogging with v53::ReplicaLogging);
wire_compatible!(v52::replica_config::ManagedLocation with v53::replica_config::ManagedLocation);
wire_compatible!(v52::replica_config::UnmanagedLocation with v53::replica_config::UnmanagedLocation);
wire_compatible!(v52::RoleId with v53::RoleId);

/// Remove the `idle_arrangement_merge_effort` cluster/replica option.
pub fn upgrade(
    snapshot: Vec<v52::StateUpdateKind>,
) -> Vec<MigrationAction<v52::StateUpdateKind, v53::StateUpdateKind>> {
    snapshot
        .into_iter()
        .filter_map(|update| {
            let old = update.clone();
            match update.kind {
                Some(v52::state_update_kind::Kind::Cluster(cluster)) => {
                    let new = v53::StateUpdateKind {
                        kind: Some(v53::state_update_kind::Kind::Cluster(
                            v53::state_update_kind::Cluster {
                                key: convert_option(cluster.key),
                                value: cluster.value.map(|value| v53::ClusterValue {
                                    name: value.name,
                                    owner_id: convert_option(value.owner_id),
                                    privileges: convert_vec(value.privileges),
                                    config: value.config.map(|config| v53::ClusterConfig {
                                        variant: config.variant.map(|variant| match variant {
                                            v52::cluster_config::Variant::Unmanaged(_) => {
                                                v53::cluster_config::Variant::Unmanaged(
                                                    v53::Empty {},
                                                )
                                            }
                                            v52::cluster_config::Variant::Managed(managed) => {
                                                v53::cluster_config::Variant::Managed(
                                                    v53::cluster_config::ManagedCluster {
                                                        size: managed.size,
                                                        replication_factor: managed
                                                            .replication_factor,
                                                        availability_zones: managed
                                                            .availability_zones,
                                                        logging: convert_option(managed.logging),
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
                Some(v52::state_update_kind::Kind::ClusterReplica(replica)) => {
                    let new = v53::StateUpdateKind {
                        kind: Some(v53::state_update_kind::Kind::ClusterReplica(
                            v53::state_update_kind::ClusterReplica {
                                key: convert_option(replica.key),
                                value: replica.value.map(|value| v53::ClusterReplicaValue {
                                    cluster_id: convert_option(value.cluster_id),
                                    name: value.name,
                                    config: value.config.map(|config| v53::ReplicaConfig {
                                        logging: convert_option(config.logging),
                                        location: config.location.map(|location| match location {
                                            v52::replica_config::Location::Unmanaged(unmanaged) => {
                                                v53::replica_config::Location::Unmanaged(convert(
                                                    unmanaged,
                                                ))
                                            }
                                            v52::replica_config::Location::Managed(managed) => {
                                                v53::replica_config::Location::Managed(convert(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test(tokio::test)]
    async fn migrate_default_effort() {
        let v52_cluster = v52::StateUpdateKind {
            kind: Some(v52::state_update_kind::Kind::Cluster(
                v52::state_update_kind::Cluster {
                    value: Some(v52::ClusterValue {
                        config: Some(v52::ClusterConfig {
                            variant: Some(v52::cluster_config::Variant::Managed(
                                v52::cluster_config::ManagedCluster {
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
        let v52_replica = v52::StateUpdateKind {
            kind: Some(v52::state_update_kind::Kind::ClusterReplica(
                v52::state_update_kind::ClusterReplica {
                    value: Some(v52::ClusterReplicaValue {
                        config: Some(v52::ReplicaConfig {
                            idle_arrangement_merge_effort: None,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        };

        let v53_cluster = v53::StateUpdateKind {
            kind: Some(v53::state_update_kind::Kind::Cluster(
                v53::state_update_kind::Cluster {
                    value: Some(v53::ClusterValue {
                        config: Some(v53::ClusterConfig {
                            variant: Some(
                                v53::cluster_config::Variant::Managed(Default::default()),
                            ),
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        };
        let v53_replica = v53::StateUpdateKind {
            kind: Some(v53::state_update_kind::Kind::ClusterReplica(
                v53::state_update_kind::ClusterReplica {
                    value: Some(v53::ClusterReplicaValue {
                        config: Some(Default::default()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        };

        let actions = crate::durable::upgrade::v52_to_v53::upgrade(vec![
            v52_cluster.clone(),
            v52_replica.clone(),
        ]);
        let expected = vec![
            MigrationAction::Update(v52_cluster, v53_cluster),
            MigrationAction::Update(v52_replica, v53_replica),
        ];
        assert_eq!(actions, expected);
    }

    #[mz_ore::test(tokio::test)]
    async fn migrate_zero_effort() {
        let v52_cluster = v52::StateUpdateKind {
            kind: Some(v52::state_update_kind::Kind::Cluster(
                v52::state_update_kind::Cluster {
                    value: Some(v52::ClusterValue {
                        config: Some(v52::ClusterConfig {
                            variant: Some(v52::cluster_config::Variant::Managed(
                                v52::cluster_config::ManagedCluster {
                                    idle_arrangement_merge_effort: Some(v52::ReplicaMergeEffort {
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
        let v52_replica = v52::StateUpdateKind {
            kind: Some(v52::state_update_kind::Kind::ClusterReplica(
                v52::state_update_kind::ClusterReplica {
                    value: Some(v52::ClusterReplicaValue {
                        config: Some(v52::ReplicaConfig {
                            idle_arrangement_merge_effort: Some(v52::ReplicaMergeEffort {
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

        let v53_cluster = v53::StateUpdateKind {
            kind: Some(v53::state_update_kind::Kind::Cluster(
                v53::state_update_kind::Cluster {
                    value: Some(v53::ClusterValue {
                        config: Some(v53::ClusterConfig {
                            variant: Some(
                                v53::cluster_config::Variant::Managed(Default::default()),
                            ),
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        };
        let v53_replica = v53::StateUpdateKind {
            kind: Some(v53::state_update_kind::Kind::ClusterReplica(
                v53::state_update_kind::ClusterReplica {
                    value: Some(v53::ClusterReplicaValue {
                        config: Some(Default::default()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        };

        let actions = crate::durable::upgrade::v52_to_v53::upgrade(vec![
            v52_cluster.clone(),
            v52_replica.clone(),
        ]);
        let expected = vec![
            MigrationAction::Update(v52_cluster, v53_cluster),
            MigrationAction::Update(v52_replica, v53_replica),
        ];
        assert_eq!(actions, expected);
    }

    #[mz_ore::test(tokio::test)]
    async fn migrate_nonzero_effort() {
        let v52_cluster = v52::StateUpdateKind {
            kind: Some(v52::state_update_kind::Kind::Cluster(
                v52::state_update_kind::Cluster {
                    value: Some(v52::ClusterValue {
                        config: Some(v52::ClusterConfig {
                            variant: Some(v52::cluster_config::Variant::Managed(
                                v52::cluster_config::ManagedCluster {
                                    idle_arrangement_merge_effort: Some(v52::ReplicaMergeEffort {
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
        let v52_replica = v52::StateUpdateKind {
            kind: Some(v52::state_update_kind::Kind::ClusterReplica(
                v52::state_update_kind::ClusterReplica {
                    value: Some(v52::ClusterReplicaValue {
                        config: Some(v52::ReplicaConfig {
                            idle_arrangement_merge_effort: Some(v52::ReplicaMergeEffort {
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

        let v53_cluster = v53::StateUpdateKind {
            kind: Some(v53::state_update_kind::Kind::Cluster(
                v53::state_update_kind::Cluster {
                    value: Some(v53::ClusterValue {
                        config: Some(v53::ClusterConfig {
                            variant: Some(
                                v53::cluster_config::Variant::Managed(Default::default()),
                            ),
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        };
        let v53_replica = v53::StateUpdateKind {
            kind: Some(v53::state_update_kind::Kind::ClusterReplica(
                v53::state_update_kind::ClusterReplica {
                    value: Some(v53::ClusterReplicaValue {
                        config: Some(Default::default()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        };

        let actions = crate::durable::upgrade::v52_to_v53::upgrade(vec![
            v52_cluster.clone(),
            v52_replica.clone(),
        ]);
        let expected = vec![
            MigrationAction::Update(v52_cluster, v53_cluster),
            MigrationAction::Update(v52_replica, v53_replica),
        ];
        assert_eq!(actions, expected);
    }
}

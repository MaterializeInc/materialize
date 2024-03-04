// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_stash::upgrade::WireCompatible;

use crate::durable::upgrade::persist::MigrationAction;
use crate::durable::upgrade::{objects_v46 as v46, objects_v47 as v47};

/// Introduce empty `optimizer_feature_overrides` in `ManagedCluster`'s.
pub fn upgrade(
    snapshot: Vec<v46::StateUpdateKind>,
) -> Vec<MigrationAction<v46::StateUpdateKind, v47::StateUpdateKind>> {
    snapshot
        .into_iter()
        .filter_map(|update| {
            let update = update.kind.as_ref().expect("missing field");
            let v46::state_update_kind::Kind::Cluster(update) = update else {
                return None;
            };
            if !update.is_managed() {
                return None;
            };

            let old = v46::StateUpdateKind {
                kind: Some(v46::state_update_kind::Kind::Cluster(
                    v46::state_update_kind::Cluster {
                        key: update.key.clone(),
                        value: update.value.clone(),
                    },
                )),
            };

            let new = v47::StateUpdateKind {
                kind: Some(v47::state_update_kind::Kind::Cluster(
                    v47::state_update_kind::Cluster {
                        key: update.key.as_ref().map(WireCompatible::convert),
                        value: update.value.as_ref().map(|old_val| v47::ClusterValue {
                            name: old_val.name.clone(),
                            owner_id: old_val.owner_id.as_ref().map(WireCompatible::convert),
                            privileges: old_val
                                .privileges
                                .iter()
                                .map(WireCompatible::convert)
                                .collect(),
                            config: old_val.config.as_ref().map(|config| v47::ClusterConfig {
                                variant: config.variant.as_ref().map(|variant| match variant {
                                    v46::cluster_config::Variant::Unmanaged(_) => {
                                        v47::cluster_config::Variant::Unmanaged(v47::Empty {})
                                    }
                                    v46::cluster_config::Variant::Managed(c) => {
                                        v47::cluster_config::Variant::Managed(
                                            v47::cluster_config::ManagedCluster {
                                                size: c.size.clone(),
                                                replication_factor: c.replication_factor,
                                                availability_zones: c.availability_zones.clone(),
                                                logging: c
                                                    .logging
                                                    .as_ref()
                                                    .map(WireCompatible::convert),
                                                idle_arrangement_merge_effort: c
                                                    .idle_arrangement_merge_effort
                                                    .as_ref()
                                                    .map(WireCompatible::convert),
                                                disk: c.disk,
                                                optimizer_feature_overrides: Vec::new(),
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
        })
        .collect()
}

impl v46::state_update_kind::Cluster {
    fn is_managed(&self) -> bool {
        let Some(cluster) = self.value.as_ref() else {
            return false;
        };
        let Some(config) = cluster.config.as_ref() else {
            return false;
        };
        match config.variant.as_ref() {
            Some(v46::cluster_config::Variant::Managed(_)) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test_migration() {
        let v46 = v46::StateUpdateKind {
            kind: Some(v46::state_update_kind::Kind::Cluster(
                v46::state_update_kind::Cluster {
                    key: Some(v46::ClusterKey {
                        id: Some(v46::ClusterId {
                            value: Some(v46::cluster_id::Value::User(Default::default())),
                        }),
                    }),
                    value: Some(v46::ClusterValue {
                        name: Default::default(),
                        owner_id: Some(v46::RoleId {
                            value: Some(v46::role_id::Value::Public(Default::default())),
                        }),
                        privileges: vec![],
                        config: Some(v46::ClusterConfig {
                            variant: Some(v46::cluster_config::Variant::Managed(
                                v46::cluster_config::ManagedCluster {
                                    size: String::from("1cc"),
                                    replication_factor: 2,
                                    availability_zones: vec![
                                        String::from("az1"),
                                        String::from("az2"),
                                    ],
                                    logging: Some(v46::ReplicaLogging {
                                        log_logging: true,
                                        interval: Some(v46::Duration {
                                            secs: 3600,
                                            nanos: 747,
                                        }),
                                    }),
                                    idle_arrangement_merge_effort: Some(v46::ReplicaMergeEffort {
                                        effort: 42,
                                    }),
                                    disk: true,
                                },
                            )),
                        }),
                    }),
                },
            )),
        };

        let v47 = v47::StateUpdateKind {
            kind: Some(v47::state_update_kind::Kind::Cluster(
                v47::state_update_kind::Cluster {
                    key: Some(v47::ClusterKey {
                        id: Some(v47::ClusterId {
                            value: Some(v47::cluster_id::Value::User(Default::default())),
                        }),
                    }),
                    value: Some(v47::ClusterValue {
                        name: Default::default(),
                        owner_id: Some(v47::RoleId {
                            value: Some(v47::role_id::Value::Public(Default::default())),
                        }),
                        privileges: vec![],
                        config: Some(v47::ClusterConfig {
                            variant: Some(v47::cluster_config::Variant::Managed(
                                v47::cluster_config::ManagedCluster {
                                    size: String::from("1cc"),
                                    replication_factor: 2,
                                    availability_zones: vec![
                                        String::from("az1"),
                                        String::from("az2"),
                                    ],
                                    logging: Some(v47::ReplicaLogging {
                                        log_logging: true,
                                        interval: Some(v47::Duration {
                                            secs: 3600,
                                            nanos: 747,
                                        }),
                                    }),
                                    idle_arrangement_merge_effort: Some(v47::ReplicaMergeEffort {
                                        effort: 42,
                                    }),
                                    disk: true,
                                    optimizer_feature_overrides: Vec::new(),
                                },
                            )),
                        }),
                    }),
                },
            )),
        };

        let actions = upgrade(vec![v46.clone()]);

        match &actions[..] {
            [MigrationAction::Update(old, new)] => {
                assert_eq!(old, &v46);
                assert_eq!(new, &v47);
            }
            o => panic!("expected single MigrationAction::Update, got {:?}", o),
        }
    }
}

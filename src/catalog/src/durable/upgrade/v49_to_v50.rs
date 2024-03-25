// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v49 as v49, objects_v50 as v50};
use mz_stash::upgrade::WireCompatible;
use mz_stash::wire_compatible;

wire_compatible!(v49::ClusterKey with v50::ClusterKey);
wire_compatible!(v49::RoleId with v50::RoleId);
wire_compatible!(v49::MzAclItem with v50::MzAclItem);
wire_compatible!(v49::ReplicaLogging with v50::ReplicaLogging);
wire_compatible!(v49::ReplicaMergeEffort with v50::ReplicaMergeEffort);
wire_compatible!(v49::OptimizerFeatureOverride with v50::OptimizerFeatureOverride);

// In v50, we add the `schedule` field to `ClusterConfig`. The migration just needs to make the
// default value of the `schedule` field appear.
pub fn upgrade(
    snapshot: Vec<v49::StateUpdateKind>,
) -> Vec<MigrationAction<v49::StateUpdateKind, v50::StateUpdateKind>> {
    snapshot
        .into_iter()
        .filter_map(|update| {
            let v49::state_update_kind::Kind::Cluster(
                v49::state_update_kind::Cluster { key, value },
            ) = update.kind.as_ref().expect("missing field")
            else {
                return None;
            };

            let current = v49::StateUpdateKind {
                kind: Some(
                    v49::state_update_kind::Kind::Cluster(
                        v49::state_update_kind::Cluster {
                            key: key.clone(),
                            value: value.clone(),
                        },
                    ),
                ),
            };

            let new = v50::StateUpdateKind {
                kind: Some(
                    v50::state_update_kind::Kind::Cluster(
                        v50::state_update_kind::Cluster {
                            key: key.as_ref().map(WireCompatible::convert),
                            value: value.as_ref().map(|old_val| v50::ClusterValue {
                                name: old_val.name.clone(),
                                owner_id: old_val.owner_id.as_ref().map(WireCompatible::convert),
                                privileges: old_val
                                    .privileges
                                    .iter()
                                    .map(WireCompatible::convert)
                                    .collect(),
                                config: old_val.config.as_ref().map(|old_config| v50::ClusterConfig {
                                    variant: old_config.variant.as_ref().map(|variant| match variant {
                                        v49::cluster_config::Variant::Unmanaged(_) => {
                                            v50::cluster_config::Variant::Unmanaged(v50::Empty {})
                                        }
                                        v49::cluster_config::Variant::Managed(c) => {
                                            v50::cluster_config::Variant::Managed(
                                                v50::cluster_config::ManagedCluster {
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
                                                    optimizer_feature_overrides: c
                                                        .optimizer_feature_overrides
                                                        .iter()
                                                        .map(WireCompatible::convert)
                                                        .collect(),
                                                    schedule: Some(v50::ClusterScheduleOptionValue{value: Some(v50::cluster_schedule_option_value::Value::Manual(Default::default()))}), // The new field
                                                },
                                            )
                                        }
                                    }),
                                }),
                            }),
                        },
                    ),
                ),
            };

            Some(MigrationAction::Update(current, new))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test_migration() {
        let v49 = v49::StateUpdateKind {
            kind: Some(v49::state_update_kind::Kind::Cluster(
                v49::state_update_kind::Cluster {
                    key: Some(v49::ClusterKey {
                        id: Some(v49::ClusterId {
                            value: Some(v49::cluster_id::Value::User(Default::default())),
                        }),
                    }),
                    value: Some(v49::ClusterValue {
                        name: Default::default(),
                        owner_id: Some(v49::RoleId {
                            value: Some(v49::role_id::Value::Public(Default::default())),
                        }),
                        privileges: vec![],
                        config: Some(v49::ClusterConfig {
                            variant: Some(v49::cluster_config::Variant::Managed(
                                v49::cluster_config::ManagedCluster {
                                    size: String::from("1cc"),
                                    replication_factor: 2,
                                    availability_zones: vec![
                                        String::from("az1"),
                                        String::from("az2"),
                                    ],
                                    logging: Some(v49::ReplicaLogging {
                                        log_logging: true,
                                        interval: Some(v49::Duration {
                                            secs: 3600,
                                            nanos: 747,
                                        }),
                                    }),
                                    idle_arrangement_merge_effort: Some(v49::ReplicaMergeEffort {
                                        effort: 42,
                                    }),
                                    disk: true,
                                    optimizer_feature_overrides: vec![
                                        v49::OptimizerFeatureOverride {
                                            name: "feature1".to_string(),
                                            value: "false".to_string(),
                                        },
                                        v49::OptimizerFeatureOverride {
                                            name: "feature2".to_string(),
                                            value: "true".to_string(),
                                        },
                                    ],
                                },
                            )),
                        }),
                    }),
                },
            )),
        };

        let v50 = v50::StateUpdateKind {
            kind: Some(v50::state_update_kind::Kind::Cluster(
                v50::state_update_kind::Cluster {
                    key: Some(v50::ClusterKey {
                        id: Some(v50::ClusterId {
                            value: Some(v50::cluster_id::Value::User(Default::default())),
                        }),
                    }),
                    value: Some(v50::ClusterValue {
                        name: Default::default(),
                        owner_id: Some(v50::RoleId {
                            value: Some(v50::role_id::Value::Public(Default::default())),
                        }),
                        privileges: vec![],
                        config: Some(v50::ClusterConfig {
                            variant: Some(v50::cluster_config::Variant::Managed(
                                v50::cluster_config::ManagedCluster {
                                    size: String::from("1cc"),
                                    replication_factor: 2,
                                    availability_zones: vec![
                                        String::from("az1"),
                                        String::from("az2"),
                                    ],
                                    logging: Some(v50::ReplicaLogging {
                                        log_logging: true,
                                        interval: Some(v50::Duration {
                                            secs: 3600,
                                            nanos: 747,
                                        }),
                                    }),
                                    idle_arrangement_merge_effort: Some(v50::ReplicaMergeEffort {
                                        effort: 42,
                                    }),
                                    disk: true,
                                    optimizer_feature_overrides: vec![
                                        v50::OptimizerFeatureOverride {
                                            name: "feature1".to_string(),
                                            value: "false".to_string(),
                                        },
                                        v50::OptimizerFeatureOverride {
                                            name: "feature2".to_string(),
                                            value: "true".to_string(),
                                        },
                                    ],
                                    schedule: Some(v50::ClusterScheduleOptionValue {
                                        value: Some(
                                            v50::cluster_schedule_option_value::Value::Manual(
                                                Default::default(),
                                            ),
                                        ),
                                    }), // The new field
                                },
                            )),
                        }),
                    }),
                },
            )),
        };

        let actions = crate::durable::upgrade::v49_to_v50::upgrade(vec![v49.clone()]);

        match &actions[..] {
            [MigrationAction::Update(old, new)] => {
                assert_eq!(old, &v49);
                assert_eq!(new, &v50);
            }
            o => panic!("expected single MigrationAction::Update, got {:?}", o),
        }
    }
}

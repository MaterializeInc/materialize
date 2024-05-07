// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v54 as v54, objects_v55 as v55};
use mz_proto::wire_compatible;
use mz_proto::wire_compatible::WireCompatible;

wire_compatible!(v54::ClusterKey with v55::ClusterKey);
wire_compatible!(v54::RoleId with v55::RoleId);
wire_compatible!(v54::MzAclItem with v55::MzAclItem);
wire_compatible!(v54::ReplicaLogging with v55::ReplicaLogging);
wire_compatible!(v54::OptimizerFeatureOverride with v55::OptimizerFeatureOverride);

/// In v55, we add a default `REHYDRATION TIME ESTIMATE` of 0 to `SCHEDULE = ON REFRESH`.
pub fn upgrade(
    snapshot: Vec<v54::StateUpdateKind>,
) -> Vec<MigrationAction<v54::StateUpdateKind, v55::StateUpdateKind>> {
    snapshot
        .into_iter()
        .filter_map(|update| {
            let v54::state_update_kind::Kind::Cluster(
                v54::state_update_kind::Cluster { key, value },
            ) = update.kind.as_ref().expect("missing field")
            else {
                return None;
            };

            let current = v54::StateUpdateKind {
                kind: Some(
                    v54::state_update_kind::Kind::Cluster(
                        v54::state_update_kind::Cluster {
                            key: key.clone(),
                            value: value.clone(),
                        },
                    ),
                ),
            };

            let new = v55::StateUpdateKind {
                kind: Some(
                    v55::state_update_kind::Kind::Cluster(
                        v55::state_update_kind::Cluster {
                            key: key.as_ref().map(WireCompatible::convert),
                            value: value.as_ref().map(|old_val| v55::ClusterValue {
                                name: old_val.name.clone(),
                                owner_id: old_val.owner_id.as_ref().map(WireCompatible::convert),
                                privileges: old_val
                                    .privileges
                                    .iter()
                                    .map(WireCompatible::convert)
                                    .collect(),
                                config: old_val.config.as_ref().map(|old_config| v55::ClusterConfig {
                                    variant: old_config.variant.as_ref().map(|variant| match variant {
                                        v54::cluster_config::Variant::Unmanaged(_) => {
                                            v55::cluster_config::Variant::Unmanaged(v55::Empty {})
                                        }
                                        v54::cluster_config::Variant::Managed(c) => {
                                            v55::cluster_config::Variant::Managed(
                                                v55::cluster_config::ManagedCluster {
                                                    size: c.size.clone(),
                                                    replication_factor: c.replication_factor,
                                                    availability_zones: c.availability_zones.clone(),
                                                    logging: c
                                                        .logging
                                                        .as_ref()
                                                        .map(WireCompatible::convert),
                                                    disk: c.disk,
                                                    optimizer_feature_overrides: c
                                                        .optimizer_feature_overrides
                                                        .iter()
                                                        .map(WireCompatible::convert)
                                                        .collect(),
                                                    schedule: match c.schedule.as_ref() {
                                                        None => {
                                                            Some(v55::ClusterSchedule{value: Some(v55::cluster_schedule::Value::Manual(Default::default()))})
                                                        }
                                                        Some(csov) => {
                                                            match csov.value {
                                                                None => {
                                                                    Some(v55::ClusterSchedule{value: Some(v55::cluster_schedule::Value::Manual(Default::default()))})
                                                                }
                                                                Some(v54::cluster_schedule_option_value::Value::Manual(..)) => {
                                                                    Some(v55::ClusterSchedule{value: Some(v55::cluster_schedule::Value::Manual(Default::default()))})
                                                                }
                                                                Some(v54::cluster_schedule_option_value::Value::Refresh(..)) => {
                                                                    Some(v55::ClusterSchedule{value: Some(v55::cluster_schedule::Value::Refresh(v55::ClusterScheduleRefreshOptions{
                                                                        rehydration_time_estimate: Some(v55::Duration {
                                                                        secs: 0,
                                                                        nanos: 0,
                                                                    })}))})
                                                                }
                                                            }
                                                        }
                                                    }
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
        let v54 = v54::StateUpdateKind {
            kind: Some(v54::state_update_kind::Kind::Cluster(
                v54::state_update_kind::Cluster {
                    key: Some(v54::ClusterKey {
                        id: Some(v54::ClusterId {
                            value: Some(v54::cluster_id::Value::User(Default::default())),
                        }),
                    }),
                    value: Some(v54::ClusterValue {
                        name: Default::default(),
                        owner_id: Some(v54::RoleId {
                            value: Some(v54::role_id::Value::Public(Default::default())),
                        }),
                        privileges: vec![],
                        config: Some(v54::ClusterConfig {
                            variant: Some(v54::cluster_config::Variant::Managed(
                                v54::cluster_config::ManagedCluster {
                                    size: String::from("1cc"),
                                    replication_factor: 2,
                                    availability_zones: vec![
                                        String::from("az1"),
                                        String::from("az2"),
                                    ],
                                    logging: Some(v54::ReplicaLogging {
                                        log_logging: true,
                                        interval: Some(v54::Duration {
                                            secs: 3600,
                                            nanos: 747,
                                        }),
                                    }),
                                    disk: true,
                                    optimizer_feature_overrides: vec![
                                        v54::OptimizerFeatureOverride {
                                            name: "feature1".to_string(),
                                            value: "false".to_string(),
                                        },
                                        v54::OptimizerFeatureOverride {
                                            name: "feature2".to_string(),
                                            value: "true".to_string(),
                                        },
                                    ],
                                    schedule: Some(v54::ClusterScheduleOptionValue {
                                        // All users should have `SCHEDULE = MANUAL` in their
                                        // catalogs at the moment, because that's the default, and
                                        // `SCHEDULE = ON REFRESH` is not actually rolled out to
                                        // production yet.
                                        value: Some(
                                            v54::cluster_schedule_option_value::Value::Manual(
                                                v54::Empty {},
                                            ),
                                        ),
                                    }),
                                },
                            )),
                        }),
                    }),
                },
            )),
        };

        let v55 = v55::StateUpdateKind {
            kind: Some(v55::state_update_kind::Kind::Cluster(
                v55::state_update_kind::Cluster {
                    key: Some(v55::ClusterKey {
                        id: Some(v55::ClusterId {
                            value: Some(v55::cluster_id::Value::User(Default::default())),
                        }),
                    }),
                    value: Some(v55::ClusterValue {
                        name: Default::default(),
                        owner_id: Some(v55::RoleId {
                            value: Some(v55::role_id::Value::Public(Default::default())),
                        }),
                        privileges: vec![],
                        config: Some(v55::ClusterConfig {
                            variant: Some(v55::cluster_config::Variant::Managed(
                                v55::cluster_config::ManagedCluster {
                                    size: String::from("1cc"),
                                    replication_factor: 2,
                                    availability_zones: vec![
                                        String::from("az1"),
                                        String::from("az2"),
                                    ],
                                    logging: Some(v55::ReplicaLogging {
                                        log_logging: true,
                                        interval: Some(v55::Duration {
                                            secs: 3600,
                                            nanos: 747,
                                        }),
                                    }),
                                    disk: true,
                                    optimizer_feature_overrides: vec![
                                        v55::OptimizerFeatureOverride {
                                            name: "feature1".to_string(),
                                            value: "false".to_string(),
                                        },
                                        v55::OptimizerFeatureOverride {
                                            name: "feature2".to_string(),
                                            value: "true".to_string(),
                                        },
                                    ],
                                    schedule: Some(v55::ClusterSchedule {
                                        value: Some(v55::cluster_schedule::Value::Manual(
                                            v55::Empty {},
                                        )),
                                    }),
                                },
                            )),
                        }),
                    }),
                },
            )),
        };

        let actions = crate::durable::upgrade::v54_to_v55::upgrade(vec![v54.clone()]);

        match &actions[..] {
            [MigrationAction::Update(old, new)] => {
                assert_eq!(old, &v54);
                assert_eq!(new, &v55);
            }
            o => panic!("expected single MigrationAction::Update, got {:?}", o),
        }
    }
}

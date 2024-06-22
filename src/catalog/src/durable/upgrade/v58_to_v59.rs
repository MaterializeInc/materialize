// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v58 as v58, objects_v59 as v59};
use mz_proto::wire_compatible;
use mz_proto::wire_compatible::WireCompatible;

wire_compatible!(v58::ClusterReplicaKey with v59::ClusterReplicaKey);
wire_compatible!(v58::ClusterId  with v59::ClusterId);
wire_compatible!(v58::RoleId  with v59::RoleId);
wire_compatible!(v58::ReplicaLogging with v59::ReplicaLogging);

/// In v59, we add a pending to cluster replica ManagedLocation
pub fn upgrade(
    snapshot: Vec<v58::StateUpdateKind>,
) -> Vec<MigrationAction<v58::StateUpdateKind, v59::StateUpdateKind>> {
    snapshot
        .into_iter()
        .filter_map(|update| {
            let v58::state_update_kind::Kind::ClusterReplica(
                v58::state_update_kind::ClusterReplica { key, value },
            ) = update.kind.as_ref().expect("missing field")
            else {
                return None;
            };
            let replica_config = value.as_ref()?.config.as_ref()?;
            let v58::replica_config::Location::Managed(ml) = replica_config.location.as_ref()?
            else {
                return None;
            };
            let logging = replica_config.logging.clone();
            let managed_config = ml.clone();

            let current = v58::StateUpdateKind {
                kind: Some(v58::state_update_kind::Kind::ClusterReplica(
                    v58::state_update_kind::ClusterReplica {
                        key: key.clone(),
                        value: value.clone(),
                    },
                )),
            };

            let new = v59::StateUpdateKind {
                kind: Some(v59::state_update_kind::Kind::ClusterReplica(
                    v59::state_update_kind::ClusterReplica {
                        key: key.as_ref().map(WireCompatible::convert),
                        value: value.as_ref().map(|cur_val| v59::ClusterReplicaValue {
                            cluster_id: cur_val.cluster_id.as_ref().map(WireCompatible::convert),
                            name: cur_val.name.clone(),
                            owner_id: cur_val.owner_id.as_ref().map(WireCompatible::convert),
                            config: Some(v59::ReplicaConfig {
                                logging: logging.as_ref().map(WireCompatible::convert),
                                location: Some(v59::replica_config::Location::Managed(
                                    v59::replica_config::ManagedLocation::from(managed_config),
                                )),
                            }),
                        }),
                    },
                )),
            };
            Some(MigrationAction::Update(current, new))
        })
        .collect()
}

impl From<v58::replica_config::ManagedLocation> for v59::replica_config::ManagedLocation {
    fn from(value: v58::replica_config::ManagedLocation) -> Self {
        Self {
            size: value.size,
            availability_zone: value.availability_zone,
            disk: value.disk,
            internal: value.internal,
            billed_as: value.billed_as,
            pending: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test(tokio::test)]
    async fn smoke_test_migration() {
        let v58 = v58::StateUpdateKind {
            kind: Some(v58::state_update_kind::Kind::ClusterReplica(
                v58::state_update_kind::ClusterReplica {
                    key: Some(v58::ClusterReplicaKey {
                        id: Some(v58::ReplicaId {
                            value: Some(v58::replica_id::Value::User(Default::default())),
                        }),
                    }),
                    value: Some(v58::ClusterReplicaValue {
                        name: Default::default(),
                        cluster_id: Some(v58::ClusterId {
                            value: Some(v58::cluster_id::Value::User(Default::default())),
                        }),
                        owner_id: Some(v58::RoleId {
                            value: Some(v58::role_id::Value::Public(Default::default())),
                        }),
                        config: Some(v58::ReplicaConfig {
                            logging: Some(v58::ReplicaLogging {
                                log_logging: Default::default(),
                                interval: Default::default(),
                            }),
                            location: Some(v58::replica_config::Location::Managed(
                                v58::replica_config::ManagedLocation {
                                    size: String::from("1cc"),
                                    availability_zone: Some(String::from("az1")),
                                    disk: true,
                                    internal: false,
                                    billed_as: Some(String::from("false")),
                                },
                            )),
                        }),
                    }),
                },
            )),
        };

        let v59 = v59::StateUpdateKind {
            kind: Some(v59::state_update_kind::Kind::ClusterReplica(
                v59::state_update_kind::ClusterReplica {
                    key: Some(v59::ClusterReplicaKey {
                        id: Some(v59::ReplicaId {
                            value: Some(v59::replica_id::Value::User(Default::default())),
                        }),
                    }),
                    value: Some(v59::ClusterReplicaValue {
                        name: Default::default(),
                        cluster_id: Some(v59::ClusterId {
                            value: Some(v59::cluster_id::Value::User(Default::default())),
                        }),
                        owner_id: Some(v59::RoleId {
                            value: Some(v59::role_id::Value::Public(Default::default())),
                        }),
                        config: Some(v59::ReplicaConfig {
                            logging: Some(v59::ReplicaLogging {
                                log_logging: Default::default(),
                                interval: Default::default(),
                            }),
                            location: Some(v59::replica_config::Location::Managed(
                                v59::replica_config::ManagedLocation {
                                    size: String::from("1cc"),
                                    availability_zone: Some(String::from("az1")),
                                    disk: true,
                                    internal: false,
                                    billed_as: Some(String::from("false")),
                                    pending: false,
                                },
                            )),
                        }),
                    }),
                },
            )),
        };

        let actions = crate::durable::upgrade::v58_to_v59::upgrade(vec![v58.clone()]);

        match &actions[..] {
            [MigrationAction::Update(old, new)] => {
                assert_eq!(old, &v58);
                assert_eq!(new, &v59);
            }
            o => panic!("expected single MigrationAction::Update, got {:?}", o),
        }
    }
}

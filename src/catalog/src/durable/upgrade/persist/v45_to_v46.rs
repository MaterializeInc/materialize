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
use crate::durable::upgrade::{objects_v45 as v45, objects_v46 as v46};

/// Remove `ClusterValue`'s `linked_object_id` field.
pub fn upgrade(
    snapshot: Vec<v45::StateUpdateKind>,
) -> Vec<MigrationAction<v45::StateUpdateKind, v46::StateUpdateKind>> {
    snapshot
        .into_iter()
        .filter_map(|update| {
            let v45::state_update_kind::Kind::Cluster(v45::state_update_kind::Cluster {
                key,
                value,
            }) = update.kind.as_ref().expect("missing field")
            else {
                return None;
            };

            let current = v45::StateUpdateKind {
                kind: Some(v45::state_update_kind::Kind::Cluster(
                    v45::state_update_kind::Cluster {
                        key: key.clone(),
                        value: value.clone(),
                    },
                )),
            };

            let new = v46::StateUpdateKind {
                kind: Some(v46::state_update_kind::Kind::Cluster(
                    v46::state_update_kind::Cluster {
                        key: key.as_ref().map(WireCompatible::convert),
                        value: value.as_ref().map(
                            |v45::ClusterValue {
                                 name,
                                 // Drop `linked_object_id` values.
                                 linked_object_id: _,
                                 owner_id,
                                 privileges,
                                 config,
                             }| v46::ClusterValue {
                                name: name.clone(),
                                owner_id: owner_id.as_ref().map(WireCompatible::convert),
                                privileges: privileges
                                    .iter()
                                    .map(WireCompatible::convert)
                                    .collect(),
                                config: config.as_ref().map(WireCompatible::convert),
                            },
                        ),
                    },
                )),
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
        let v45 = v45::StateUpdateKind {
            kind: Some(v45::state_update_kind::Kind::Cluster(
                v45::state_update_kind::Cluster {
                    key: Some(v45::ClusterKey {
                        id: Some(v45::ClusterId {
                            value: Some(v45::cluster_id::Value::User(Default::default())),
                        }),
                    }),
                    value: Some(v45::ClusterValue {
                        linked_object_id: None,
                        name: Default::default(),
                        owner_id: Some(v45::RoleId {
                            value: Some(v45::role_id::Value::Public(Default::default())),
                        }),
                        privileges: vec![],
                        config: Some(v45::ClusterConfig {
                            variant: Some(v45::cluster_config::Variant::Unmanaged(v45::Empty {})),
                        }),
                    }),
                },
            )),
        };

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
                            variant: Some(v46::cluster_config::Variant::Unmanaged(v46::Empty {})),
                        }),
                    }),
                },
            )),
        };

        let actions = upgrade(vec![v45.clone()]);

        match &actions[..] {
            [MigrationAction::Update(old, new)] => {
                assert_eq!(old, &v45);
                assert_eq!(new, &v46);
            }
            o => panic!("expected single MigrationAction::Update, got {:?}", o),
        }
    }
}

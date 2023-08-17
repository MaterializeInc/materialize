// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::objects::{wire_compatible, WireCompatible};
use crate::upgrade::MigrationAction;
use crate::{StashError, Transaction, TypedCollection};

pub mod objects_v33 {
    include!(concat!(env!("OUT_DIR"), "/objects_v33.rs"));
}

pub mod objects_v34 {
    include!(concat!(env!("OUT_DIR"), "/objects_v34.rs"));
}

wire_compatible!(objects_v33::ClusterReplicaValue with objects_v34::ClusterReplicaValue);
wire_compatible!(objects_v33::IdAllocKey with objects_v34::IdAllocKey);
wire_compatible!(objects_v33::IdAllocValue with objects_v34::IdAllocValue);

/// Rename mz_introspection to mz_support
pub async fn upgrade(tx: &'_ mut Transaction<'_>) -> Result<(), StashError> {
    pub const CLUSTER_REPLICA_COLLECTION: TypedCollection<
        objects_v33::ClusterReplicaKey,
        objects_v33::ClusterReplicaValue,
    > = TypedCollection::new("compute_replicas");

    pub const ID_ALLOCATOR_COLLECTION: TypedCollection<
        objects_v33::IdAllocKey,
        objects_v33::IdAllocValue,
    > = TypedCollection::new("id_alloc");

    CLUSTER_REPLICA_COLLECTION
        .migrate_to::<_, objects_v34::ClusterReplicaValue>(tx, |entries| {
            let mut updates = Vec::new();
            for (key, value) in entries {
                let new_key = match (key, &value.cluster_id) {
                    // Re-use IDs of user clusters
                    (
                        objects_v33::ClusterReplicaKey {
                            id: Some(objects_v33::ReplicaId { value: replica_id }),
                        },
                        Some(objects_v33::ClusterId {
                            value: Some(objects_v33::cluster_id::Value::User(_)),
                        }),
                    ) => objects_v34::replica_id::Value::User(*replica_id),
                    // For system clusters, assign their cluster id. This requires that any system
                    // has at most one replica.
                    (
                        _,
                        Some(objects_v33::ClusterId {
                            value: Some(objects_v33::cluster_id::Value::System(cluster_id)),
                        }),
                    ) => objects_v34::replica_id::Value::System(*cluster_id),
                    _ => panic!("Incorrect cluster replica data in stash {key:?} -> {value:?}"),
                };
                let new_key = objects_v34::ReplicaId {
                    value: Some(new_key),
                };
                updates.push(MigrationAction::Update(
                    key.clone(),
                    (new_key, WireCompatible::convert(value)),
                ));
            }
            updates
        })
        .await?;

    const SYSTEM_REPLICA_ID_ALLOC_KEY: &str = "system_replica";
    const DEFAULT_SYSTEM_REPLICA_ID: u64 = 1;
    ID_ALLOCATOR_COLLECTION
        .migrate_compat(tx, |entries| {
            let mut updates = Vec::new();
            let key = objects_v34::IdAllocKey {
                name: SYSTEM_REPLICA_ID_ALLOC_KEY.to_string(),
            };
            if entries.get(&key).is_none() {
                updates.push(MigrationAction::Insert(
                    key,
                    objects_v34::IdAllocValue {
                        next_id: DEFAULT_SYSTEM_REPLICA_ID + 1,
                    },
                ));
            }
            updates
        })
        .await
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::upgrade;

    use crate::upgrade::v32_to_v33::{objects_v32, objects_v33, MZ_INTROSPECTION_ROLE_ID};
    use crate::{DebugStashFactory, TypedCollection};

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test() {
        let factory: DebugStashFactory = DebugStashFactory::new().await;
        let mut stash = factory.open_debug().await;

        let roles_v32: TypedCollection<objects_v32::RoleKey, objects_v32::RoleValue> =
            TypedCollection::new("role");

        let system_role_id_v32 = objects_v32::RoleId {
            value: Some(objects_v32::role_id::Value::System(1)),
        };
        let introspection_role_id_v32 = objects_v32::RoleId {
            value: Some(objects_v32::role_id::Value::System(2)),
        };

        roles_v32
            .insert_without_overwrite(
                &mut stash,
                vec![
                    (
                        objects_v32::RoleKey {
                            id: Some(system_role_id_v32),
                        },
                        objects_v32::RoleValue {
                            name: "mz_system".to_string(),
                            attributes: Some(objects_v32::RoleAttributes { inherit: true }),
                            membership: Some(objects_v32::RoleMembership {
                                map: Default::default(),
                            }),
                        },
                    ),
                    (
                        objects_v32::RoleKey {
                            id: Some(introspection_role_id_v32),
                        },
                        objects_v32::RoleValue {
                            name: "mz_introspection".to_string(),
                            attributes: Some(objects_v32::RoleAttributes { inherit: true }),
                            membership: Some(objects_v32::RoleMembership {
                                map: Default::default(),
                            }),
                        },
                    ),
                ],
            )
            .await
            .unwrap();

        // Run our migration.
        stash
            .with_transaction(|mut tx| {
                Box::pin(async move {
                    upgrade(&mut tx).await?;
                    Ok(())
                })
            })
            .await
            .expect("migration failed");

        let roles_v33: TypedCollection<objects_v33::RoleKey, objects_v33::RoleValue> =
            TypedCollection::new("role");
        let roles = roles_v33.iter(&mut stash).await.unwrap();

        let system_role_id_v33 = objects_v33::RoleId {
            value: Some(objects_v33::role_id::Value::System(1)),
        };
        let mut roles = roles
            .into_iter()
            .map(|((key, value), _timestamp, _diff)| (key, value))
            .collect_vec();
        roles.sort();
        assert_eq!(
            roles,
            vec![
                (
                    objects_v33::RoleKey {
                        id: Some(system_role_id_v33),
                    },
                    objects_v33::RoleValue {
                        name: "mz_system".to_string(),
                        attributes: Some(objects_v33::RoleAttributes { inherit: true }),
                        membership: Some(objects_v33::RoleMembership {
                            map: Default::default(),
                        }),
                    },
                ),
                (
                    objects_v33::RoleKey {
                        id: Some(MZ_INTROSPECTION_ROLE_ID),
                    },
                    objects_v33::RoleValue {
                        name: "mz_support".to_string(),
                        attributes: Some(objects_v33::RoleAttributes { inherit: true }),
                        membership: Some(objects_v33::RoleMembership {
                            map: Default::default(),
                        }),
                    },
                )
            ]
        );
    }
}

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

pub mod v34 {
    include!(concat!(env!("OUT_DIR"), "/objects_v34.rs"));
}

pub mod v35 {
    include!(concat!(env!("OUT_DIR"), "/objects_v35.rs"));
}

wire_compatible!(v34::ClusterReplicaKey with v35::ClusterItemKey);
wire_compatible!(v34::ClusterId with v35::ClusterId);
wire_compatible!(v34::ReplicaConfig with v35::ReplicaConfig);
wire_compatible!(v34::RoleId with v35::RoleId);

const CLUSTER_REPLICA_COLLECTION: TypedCollection<
    v34::ClusterReplicaKey,
    v34::ClusterReplicaValue,
> = TypedCollection::new("compute_replicas");

/// Migrate replicas to cluster items
pub async fn upgrade(tx: &mut Transaction<'_>) -> Result<(), StashError> {
    migrate_cluster_replicas(tx).await
}

/// Migrate replicas in the "cluster_replicas" collection.
async fn migrate_cluster_replicas(tx: &mut Transaction<'_>) -> Result<(), StashError> {
    let action = |(key, value): (&v34::ClusterReplicaKey, &v34::ClusterReplicaValue)| {
        let new_key = WireCompatible::convert(key);

        let new_value = v35::ClusterItemValue {
            cluster_id: value.cluster_id.as_ref().map(WireCompatible::convert),
            item: Some(v35::cluster_item_value::Item::Replica(
                v35::ClusterReplicaConfig {
                    profile_id: None,
                    replica_config: value.config.as_ref().map(WireCompatible::convert),
                },
            )),
            name: value.name.clone(),
            owner_id: value.owner_id.as_ref().map(WireCompatible::convert),
        };
        MigrationAction::Update(key.clone(), (new_key, new_value))
    };

    CLUSTER_REPLICA_COLLECTION
        .migrate_to::<v35::ClusterItemKey, v35::ClusterItemValue>(tx, |entries| {
            entries.iter().map(action).collect()
        })
        .await
}

#[cfg(test)]
mod tests {
    use crate::DebugStashFactory;

    use super::*;

    const CLUSTER_REPLICA_COLLECTION_V35: TypedCollection<
        v35::ClusterItemKey,
        v35::ClusterItemValue,
    > = TypedCollection::new("compute_replicas");

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test() {
        let factory = DebugStashFactory::new().await;
        let mut stash = factory.open_debug().await;

        CLUSTER_REPLICA_COLLECTION
            .insert_without_overwrite(
                &mut stash,
                vec![
                    (
                        v34::ClusterReplicaKey {
                            id: Some(v34::ReplicaId {
                                value: Some(v34::replica_id::Value::System(5)),
                            }),
                        },
                        v34::ClusterReplicaValue {
                            cluster_id: Some(v34::ClusterId {
                                value: Some(v34::cluster_id::Value::System(5)),
                            }),
                            config: Some(v34::ReplicaConfig {
                                idle_arrangement_merge_effort: Some(v34::ReplicaMergeEffort {
                                    effort: 1234,
                                }),
                                location: None,
                                logging: None,
                            }),
                            name: "system".into(),
                            owner_id: None,
                        },
                    ),
                    (
                        v34::ClusterReplicaKey {
                            id: Some(v34::ReplicaId {
                                value: Some(v34::replica_id::Value::User(200)),
                            }),
                        },
                        v34::ClusterReplicaValue {
                            cluster_id: Some(v34::ClusterId {
                                value: Some(v34::cluster_id::Value::User(7)),
                            }),
                            config: Some(v34::ReplicaConfig {
                                idle_arrangement_merge_effort: Some(v34::ReplicaMergeEffort {
                                    effort: 4321,
                                }),
                                location: None,
                                logging: None,
                            }),
                            name: "user".into(),
                            owner_id: None,
                        },
                    ),
                ],
            )
            .await
            .unwrap();

        // Run the migration.
        stash
            .with_transaction(|mut tx| {
                Box::pin(async move {
                    upgrade(&mut tx).await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let mut replicas: Vec<_> = CLUSTER_REPLICA_COLLECTION_V35
            .peek_one(&mut stash)
            .await
            .unwrap()
            .into_iter()
            .collect();
        replicas.sort();

        assert_eq!(
            replicas,
            vec![
                (
                    v35::ClusterItemKey {
                        id: Some(v35::ClusterItemId {
                            value: Some(v35::cluster_item_id::Value::System(5))
                        }),
                    },
                    v35::ClusterItemValue {
                        cluster_id: Some(v35::ClusterId {
                            value: Some(v35::cluster_id::Value::System(5)),
                        }),
                        name: "system".into(),
                        owner_id: None,
                        item: Some(v35::cluster_item_value::Item::Replica(
                            v35::ClusterReplicaConfig {
                                profile_id: None,
                                replica_config: Some(v35::ReplicaConfig {
                                    logging: None,
                                    idle_arrangement_merge_effort: Some(v35::ReplicaMergeEffort {
                                        effort: 1234,
                                    }),
                                    location: None,
                                })
                            }
                        ))
                    },
                ),
                (
                    v35::ClusterItemKey {
                        id: Some(v35::ClusterItemId {
                            value: Some(v35::cluster_item_id::Value::User(200))
                        }),
                    },
                    v35::ClusterItemValue {
                        cluster_id: Some(v35::ClusterId {
                            value: Some(v35::cluster_id::Value::User(7)),
                        }),
                        name: "user".into(),
                        owner_id: None,
                        item: Some(v35::cluster_item_value::Item::Replica(
                            v35::ClusterReplicaConfig {
                                profile_id: None,
                                replica_config: Some(v35::ReplicaConfig {
                                    logging: None,
                                    idle_arrangement_merge_effort: Some(v35::ReplicaMergeEffort {
                                        effort: 4321,
                                    }),
                                    location: None,
                                })
                            }
                        ))
                    },
                ),
            ]
        );
    }
}

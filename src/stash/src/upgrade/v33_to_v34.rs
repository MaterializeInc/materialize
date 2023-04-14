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

pub mod v33 {
    include!(concat!(env!("OUT_DIR"), "/objects_v33.rs"));
}

pub mod v34 {
    include!(concat!(env!("OUT_DIR"), "/objects_v34.rs"));
}

wire_compatible!(v33::ClusterReplicaValue with v34::ClusterReplicaValue);
wire_compatible!(v33::IdAllocKey with v34::IdAllocKey);
wire_compatible!(v33::IdAllocValue with v34::IdAllocValue);

const CLUSTER_REPLICA_COLLECTION: TypedCollection<
    v33::ClusterReplicaKey,
    v33::ClusterReplicaValue,
> = TypedCollection::new("compute_replicas");

const ID_ALLOCATOR_COLLECTION: TypedCollection<v33::IdAllocKey, v33::IdAllocValue> =
    TypedCollection::new("id_alloc");

const SYSTEM_REPLICA_ID_ALLOC_KEY: &str = "system_replica";

/// Migrate replica IDs from 'NNN' to 'uNNN'/'sNNN' format.
pub async fn upgrade(tx: &mut Transaction<'_>) -> Result<(), StashError> {
    initialize_system_replica_id_allocator(tx).await?;
    migrate_cluster_replicas(tx).await?;
    Ok(())
}

/// Initialize the "system_replica" ID allocator.
async fn initialize_system_replica_id_allocator(
    tx: &mut Transaction<'_>,
) -> Result<(), StashError> {
    let coll = CLUSTER_REPLICA_COLLECTION.from_tx(tx).await?;
    let mut max_system_id = 0;
    for (_, value) in tx.peek_one(coll).await? {
        let cluster_id = value.cluster_id.unwrap().value.unwrap();
        if let v33::cluster_id::Value::System(id) = cluster_id {
            max_system_id = std::cmp::max(max_system_id, id);
        }
    }
    let next_id = max_system_id + 1;

    ID_ALLOCATOR_COLLECTION
        .migrate_compat(tx, |entries| {
            let key = v34::IdAllocKey {
                name: SYSTEM_REPLICA_ID_ALLOC_KEY.into(),
            };
            assert!(!entries.contains_key(&key));
            let value = v34::IdAllocValue { next_id };
            vec![MigrationAction::Insert(key, value)]
        })
        .await
}

/// Migrate replica IDs in the "cluster_replicas" collection.
async fn migrate_cluster_replicas(tx: &mut Transaction<'_>) -> Result<(), StashError> {
    let action = |(key, value): (&v33::ClusterReplicaKey, &v33::ClusterReplicaValue)| {
        let old_id = key.id.as_ref().unwrap().value;
        let cluster_id = value.cluster_id.as_ref().unwrap().value.as_ref().unwrap();

        // Construct the new ID format based on the plain ID and the cluster type.
        // System clusters currently always have a single replica, so we can set the replica ID to
        // the cluster ID. This does not work for user clusters.
        let new_id = match cluster_id {
            v33::cluster_id::Value::User(_) => v34::replica_id::Value::User(old_id),
            v33::cluster_id::Value::System(id) => v34::replica_id::Value::System(*id),
        };

        let new_key = v34::ClusterReplicaKey {
            id: Some(v34::ReplicaId {
                value: Some(new_id),
            }),
        };
        let new_value = WireCompatible::convert(value.clone());
        MigrationAction::Update(key.clone(), (new_key, new_value))
    };

    CLUSTER_REPLICA_COLLECTION
        .migrate_to::<_, v34::ClusterReplicaValue>(tx, |entries| {
            entries.iter().map(action).collect()
        })
        .await
}

#[cfg(test)]
mod tests {
    use crate::DebugStashFactory;

    use super::*;

    const CLUSTER_REPLICA_COLLECTION_V34: TypedCollection<
        v34::ClusterReplicaKey,
        v34::ClusterReplicaValue,
    > = TypedCollection::new("compute_replicas");

    const ID_ALLOCATOR_COLLECTION_V34: TypedCollection<v34::IdAllocKey, v34::IdAllocValue> =
        TypedCollection::new("id_alloc");

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
                        v33::ClusterReplicaKey {
                            id: Some(v33::ReplicaId { value: 100 }),
                        },
                        v33::ClusterReplicaValue {
                            cluster_id: Some(v33::ClusterId {
                                value: Some(v33::cluster_id::Value::System(5)),
                            }),
                            config: None,
                            name: "system".into(),
                            owner_id: None,
                        },
                    ),
                    (
                        v33::ClusterReplicaKey {
                            id: Some(v33::ReplicaId { value: 200 }),
                        },
                        v33::ClusterReplicaValue {
                            cluster_id: Some(v33::ClusterId {
                                value: Some(v33::cluster_id::Value::User(7)),
                            }),
                            config: None,
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

        let mut replicas: Vec<_> = CLUSTER_REPLICA_COLLECTION_V34
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
                    v34::ClusterReplicaKey {
                        id: Some(v34::ReplicaId {
                            value: Some(v34::replica_id::Value::System(5))
                        }),
                    },
                    v34::ClusterReplicaValue {
                        cluster_id: Some(v34::ClusterId {
                            value: Some(v34::cluster_id::Value::System(5)),
                        }),
                        config: None,
                        name: "system".into(),
                        owner_id: None,
                    },
                ),
                (
                    v34::ClusterReplicaKey {
                        id: Some(v34::ReplicaId {
                            value: Some(v34::replica_id::Value::User(200))
                        }),
                    },
                    v34::ClusterReplicaValue {
                        cluster_id: Some(v34::ClusterId {
                            value: Some(v34::cluster_id::Value::User(7)),
                        }),
                        config: None,
                        name: "user".into(),
                        owner_id: None,
                    },
                ),
            ]
        );

        let key = v34::IdAllocKey {
            name: SYSTEM_REPLICA_ID_ALLOC_KEY.into(),
        };
        let next_system_replica_id = ID_ALLOCATOR_COLLECTION_V34
            .peek_key_one(&mut stash, key)
            .await
            .unwrap();

        assert_eq!(
            next_system_replica_id,
            Some(v34::IdAllocValue { next_id: 6 })
        );
    }
}

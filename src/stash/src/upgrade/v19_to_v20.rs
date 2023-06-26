// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::upgrade::MigrationAction;
use crate::{StashError, Transaction, TypedCollection};

pub mod objects_v19 {
    include!(concat!(env!("OUT_DIR"), "/objects_v19.rs"));
}
pub mod objects_v20 {
    include!(concat!(env!("OUT_DIR"), "/objects_v20.rs"));
}

/// Migrate existing clusters to unmanaged configurations.
pub async fn upgrade(tx: &'_ mut Transaction<'_>) -> Result<(), StashError> {
    const CLUSTER_COLLECTION: TypedCollection<objects_v19::ClusterKey, objects_v19::ClusterValue> =
        TypedCollection::new("compute_instance");

    CLUSTER_COLLECTION
        .migrate_to(tx, |entries| {
            let mut updates = vec![];

            for (cluster_key, cluster_value) in entries {
                let mut new_value = objects_v20::ClusterValue::from(cluster_value.clone());
                let new_key = objects_v20::ClusterKey::from(cluster_key.clone());
                if new_value.config.is_none() {
                    let config = objects_v20::ClusterConfig {
                        variant: Some(objects_v20::cluster_config::Variant::Unmanaged(
                            objects_v20::Empty {},
                        )),
                    };
                    new_value.config = Some(config);
                    updates.push(MigrationAction::Update(
                        cluster_key.clone(),
                        (new_key, new_value),
                    ));
                }
            }
            updates
        })
        .await?;

    Ok(())
}

impl From<objects_v19::ClusterKey> for objects_v20::ClusterKey {
    fn from(key: objects_v19::ClusterKey) -> Self {
        Self {
            id: key.id.map(Into::into),
        }
    }
}

impl From<objects_v19::ClusterValue> for objects_v20::ClusterValue {
    fn from(cluster: objects_v19::ClusterValue) -> Self {
        Self {
            name: cluster.name,
            owner_id: cluster.owner_id.map(Into::into),
            privileges: cluster.privileges.into_iter().map(Into::into).collect(),
            linked_object_id: cluster.linked_object_id.map(Into::into),
            config: None,
        }
    }
}

impl From<objects_v19::ClusterId> for objects_v20::ClusterId {
    fn from(cluster_id: objects_v19::ClusterId) -> Self {
        Self {
            value: cluster_id.value.map(Into::into),
        }
    }
}

impl From<objects_v19::cluster_id::Value> for objects_v20::cluster_id::Value {
    fn from(value: objects_v19::cluster_id::Value) -> Self {
        match value {
            objects_v19::cluster_id::Value::System(id) => {
                objects_v20::cluster_id::Value::System(id)
            }
            objects_v19::cluster_id::Value::User(id) => objects_v20::cluster_id::Value::User(id),
        }
    }
}

impl From<objects_v19::GlobalId> for objects_v20::GlobalId {
    fn from(id: objects_v19::GlobalId) -> Self {
        let value = match id.value {
            None => None,
            Some(objects_v19::global_id::Value::User(id)) => {
                Some(objects_v20::global_id::Value::User(id))
            }
            Some(objects_v19::global_id::Value::System(id)) => {
                Some(objects_v20::global_id::Value::System(id))
            }
            Some(objects_v19::global_id::Value::Transient(id)) => {
                Some(objects_v20::global_id::Value::Transient(id))
            }
            Some(objects_v19::global_id::Value::Explain(_)) => {
                Some(objects_v20::global_id::Value::Explain(Default::default()))
            }
        };
        objects_v20::GlobalId { value }
    }
}

impl From<objects_v19::RoleId> for objects_v20::RoleId {
    fn from(id: objects_v19::RoleId) -> Self {
        let value = match id.value {
            None => None,
            Some(objects_v19::role_id::Value::User(id)) => {
                Some(objects_v20::role_id::Value::User(id))
            }
            Some(objects_v19::role_id::Value::System(id)) => {
                Some(objects_v20::role_id::Value::System(id))
            }
            Some(objects_v19::role_id::Value::Public(_)) => {
                Some(objects_v20::role_id::Value::Public(Default::default()))
            }
        };
        objects_v20::RoleId { value }
    }
}

impl From<objects_v19::MzAclItem> for objects_v20::MzAclItem {
    fn from(item: objects_v19::MzAclItem) -> Self {
        Self {
            grantee: item.grantee.map(Into::into),
            grantor: item.grantor.map(Into::into),
            acl_mode: item.acl_mode.map(Into::into),
        }
    }
}

impl From<objects_v19::AclMode> for objects_v20::AclMode {
    fn from(mode: objects_v19::AclMode) -> Self {
        Self {
            bitflags: mode.bitflags,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::objects_v19::{self, ClusterKey as ClusterKeyV19, ClusterValue as ClusterValueV19};
    use super::objects_v20::{
        self, cluster_config::Variant, cluster_id::Value as ValueV20, ClusterConfig,
        ClusterId as ClusterIdV20, ClusterKey as ClusterKeyV20, ClusterValue as ClusterValueV20,
    };
    use super::upgrade;

    use crate::{DebugStashFactory, TypedCollection};

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test() {
        // Connect to the Stash.
        let factory = DebugStashFactory::new().await;
        let mut stash = factory.open_debug().await;

        // Insert some items.
        let cluster_collection_v19: TypedCollection<
            objects_v19::ClusterKey,
            objects_v19::ClusterValue,
        > = TypedCollection::new("compute_instance");
        cluster_collection_v19
            .insert_without_overwrite(
                &mut stash,
                vec![(
                    ClusterKeyV19 {
                        id: Some(objects_v19::ClusterId {
                            value: Some(objects_v19::cluster_id::Value::User(42)),
                        }),
                    },
                    ClusterValueV19 {
                        ..Default::default()
                    },
                )],
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

        // Read back the clusters.
        let cluster_collection_v20: TypedCollection<ClusterKeyV20, ClusterValueV20> =
            TypedCollection::new("compute_instance");
        let clusters = cluster_collection_v20.iter(&mut stash).await.unwrap();

        // Filter down to just GlobalIds and SchemaIds to make comparisons easier.
        let mut ids: Vec<_> = clusters
            .into_iter()
            .map(|((key, value), _, _)| {
                let id = key.id.unwrap();
                let config = value.config.unwrap();
                (id, config)
            })
            .collect();
        ids.sort();

        assert_eq!(
            ids,
            vec![
                // Woo! Our value got migrated.
                (
                    ClusterIdV20 {
                        value: Some(ValueV20::User(42))
                    },
                    ClusterConfig {
                        variant: Some(Variant::Unmanaged(objects_v20::Empty {}))
                    }
                )
            ]
        );
    }
}

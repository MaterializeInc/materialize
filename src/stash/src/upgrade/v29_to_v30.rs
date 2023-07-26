// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::objects::wire_compatible;
use crate::upgrade::MigrationAction;
use crate::{StashError, Transaction, TypedCollection};
use std::collections::BTreeMap;

pub mod objects_v29 {
    include!(concat!(env!("OUT_DIR"), "/objects_v29.rs"));
}

pub mod objects_v30 {
    include!(concat!(env!("OUT_DIR"), "/objects_v30.rs"));
}

wire_compatible!(objects_v30::ItemKey with objects_v29::ItemKey);
wire_compatible!(objects_v30::ItemValue with objects_v29::ItemValue);

wire_compatible!(objects_v30::ClusterKey with objects_v29::ClusterKey);
wire_compatible!(objects_v30::ClusterValue with objects_v29::ClusterValue);

wire_compatible!(objects_v30::ClusterReplicaKey with objects_v29::ClusterReplicaKey);
wire_compatible!(objects_v30::ClusterReplicaValue with objects_v29::ClusterReplicaValue);

/// Force linked cluster and linked cluster replica owners in sync with the linked object.
///
/// Author - jkosh44
pub async fn upgrade(tx: &'_ mut Transaction<'_>) -> Result<(), StashError> {
    const ITEM_COLLECTION: TypedCollection<objects_v29::ItemKey, objects_v29::ItemValue> =
        TypedCollection::new("item");
    const CLUSTER_COLLECTION: TypedCollection<objects_v29::ClusterKey, objects_v29::ClusterValue> =
        TypedCollection::new("compute_instance");
    const CLUSTER_REPLICA_COLLECTION: TypedCollection<
        objects_v29::ClusterReplicaKey,
        objects_v29::ClusterReplicaValue,
    > = TypedCollection::new("compute_replicas");

    let item_owners: BTreeMap<_, _> = tx
        .peek_one(
            tx.collection::<objects_v29::ItemKey, objects_v29::ItemValue>(ITEM_COLLECTION.name())
                .await?,
        )
        .await?
        .into_iter()
        .map(|(key, value)| {
            (
                key.gid.clone().expect("missing value ItemKey::gid"),
                value
                    .owner_id
                    .clone()
                    .expect("missing value ItemValue::owner_id"),
            )
        })
        .collect();

    let cluster_linked_objects: BTreeMap<_, _> = tx
        .peek_one(
            tx.collection::<objects_v29::ClusterKey, objects_v29::ClusterValue>(
                CLUSTER_COLLECTION.name(),
            )
            .await?,
        )
        .await?
        .into_iter()
        .filter_map(|(key, value)| match &value.linked_object_id {
            Some(gid) => Some((
                key.id.clone().expect("missing value ClusterKey::id"),
                gid.clone(),
            )),
            None => None,
        })
        .collect();

    CLUSTER_COLLECTION
        .migrate_compat::<objects_v30::ClusterKey, objects_v30::ClusterValue>(tx, |entries| {
            let mut updates = Vec::new();

            for (key, value) in entries {
                let new_key = key.clone();
                let mut new_value = value.clone();
                if let Some(gid) = &new_value.linked_object_id {
                    new_value.owner_id = Some(
                        item_owners
                            .get(&(gid.clone().into()))
                            .cloned()
                            .unwrap_or_else(|| panic!("dangling reference to item '{gid:?}'"))
                            .into(),
                    );
                }
                updates.push(MigrationAction::Update(key.clone(), (new_key, new_value)));
            }

            updates
        })
        .await?;

    CLUSTER_REPLICA_COLLECTION
        .migrate_compat::<objects_v30::ClusterReplicaKey, objects_v30::ClusterReplicaValue>(
            tx,
            |entries| {
                let mut updates = Vec::new();

                for (key, value) in entries {
                    let new_key = key.clone();
                    let mut new_value = value.clone();
                    let cluster_id = value
                        .cluster_id
                        .clone()
                        .expect("missing value ClusterReplicaValue::cluster_id");
                    if let Some(gid) = cluster_linked_objects.get(&cluster_id.into()) {
                        new_value.owner_id = Some(
                            item_owners
                                .get(&(gid.clone().into()))
                                .cloned()
                                .unwrap_or_else(|| panic!("dangling reference to item '{gid:?}'"))
                                .into(),
                        );
                    }
                    updates.push(MigrationAction::Update(key.clone(), (new_key, new_value)));
                }

                updates
            },
        )
        .await?;

    Ok(())
}

impl From<objects_v29::GlobalId> for objects_v30::GlobalId {
    fn from(id: objects_v29::GlobalId) -> Self {
        let value = match id.value {
            None => None,
            Some(objects_v29::global_id::Value::User(id)) => {
                Some(objects_v30::global_id::Value::User(id))
            }
            Some(objects_v29::global_id::Value::System(id)) => {
                Some(objects_v30::global_id::Value::System(id))
            }
            Some(objects_v29::global_id::Value::Transient(id)) => {
                Some(objects_v30::global_id::Value::Transient(id))
            }
            Some(objects_v29::global_id::Value::Explain(_)) => {
                Some(objects_v30::global_id::Value::Explain(Default::default()))
            }
        };
        objects_v30::GlobalId { value }
    }
}

impl From<objects_v30::GlobalId> for objects_v29::GlobalId {
    fn from(id: objects_v30::GlobalId) -> Self {
        let value = match id.value {
            None => None,
            Some(objects_v30::global_id::Value::User(id)) => {
                Some(objects_v29::global_id::Value::User(id))
            }
            Some(objects_v30::global_id::Value::System(id)) => {
                Some(objects_v29::global_id::Value::System(id))
            }
            Some(objects_v30::global_id::Value::Transient(id)) => {
                Some(objects_v29::global_id::Value::Transient(id))
            }
            Some(objects_v30::global_id::Value::Explain(_)) => {
                Some(objects_v29::global_id::Value::Explain(Default::default()))
            }
        };
        objects_v29::GlobalId { value }
    }
}

impl From<objects_v29::RoleId> for objects_v30::RoleId {
    fn from(id: objects_v29::RoleId) -> Self {
        let value = match id.value {
            None => None,
            Some(objects_v29::role_id::Value::User(id)) => {
                Some(objects_v30::role_id::Value::User(id))
            }
            Some(objects_v29::role_id::Value::System(id)) => {
                Some(objects_v30::role_id::Value::System(id))
            }
            Some(objects_v29::role_id::Value::Public(_)) => {
                Some(objects_v30::role_id::Value::Public(Default::default()))
            }
        };
        objects_v30::RoleId { value }
    }
}

impl From<objects_v30::ClusterId> for objects_v29::ClusterId {
    fn from(cluster_id: objects_v30::ClusterId) -> Self {
        let value = match cluster_id.value {
            None => None,
            Some(objects_v30::cluster_id::Value::System(id)) => {
                Some(objects_v29::cluster_id::Value::System(id))
            }
            Some(objects_v30::cluster_id::Value::User(id)) => {
                Some(objects_v29::cluster_id::Value::User(id))
            }
        };
        Self { value }
    }
}

#[cfg(test)]
mod tests {
    use super::upgrade;

    use crate::upgrade::v29_to_v30::{objects_v29, objects_v30};
    use crate::{DebugStashFactory, TypedCollection};

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test() {
        const OWNER_ROLE_ID_v29: objects_v29::RoleId = objects_v29::RoleId {
            value: Some(objects_v29::role_id::Value::User(1)),
        };
        const OWNER_ROLE_ID_v30: objects_v30::RoleId = objects_v30::RoleId {
            value: Some(objects_v30::role_id::Value::User(1)),
        };
        const OTHER_OWNER_ROLE_ID_v29: objects_v29::RoleId = objects_v29::RoleId {
            value: Some(objects_v29::role_id::Value::User(2)),
        };
        const OTHER_OWNER_ROLE_ID_v30: objects_v30::RoleId = objects_v30::RoleId {
            value: Some(objects_v30::role_id::Value::User(2)),
        };

        // Connect to the Stash.
        let factory = DebugStashFactory::new().await;
        let mut stash = factory.open_debug().await;

        // Insert an item.
        let item_v29: TypedCollection<objects_v29::ItemKey, objects_v29::ItemValue> =
            TypedCollection::new("item");
        item_v29
            .insert_without_overwrite(
                &mut stash,
                vec![(
                    objects_v29::ItemKey {
                        id: Some(objects_v29::GlobalId {
                            value: Some(objects_v29::global_id::Value::User(42)),
                        }),
                    },
                    objects_v29::ItemValue {
                        schema_id: Some()
                        name: "db".into(),
                        owner_id: Some(OWNER_ROLE_ID_v29),
                        privileges: vec![objects_v29::MzAclItem {
                            grantee: Some(objects_v29::RoleId {
                                value: Some(objects_v29::role_id::Value::User(2)),
                            }),
                            grantor: Some(OWNER_ROLE_ID_v29),
                            acl_mode: Some(ACL_MODE_USAGE_v29),
                        }],
                    },
                )],
            )
            .await
            .unwrap();

        // Insert a schema.
        let schemas_v29: TypedCollection<objects_v29::SchemaKey, objects_v29::SchemaValue> =
            TypedCollection::new("schema");
        schemas_v29
            .insert_without_overwrite(
                &mut stash,
                vec![(
                    objects_v29::SchemaKey {
                        id: Some(objects_v29::SchemaId {
                            value: Some(objects_v29::schema_id::Value::User(42)),
                        }),
                    },
                    objects_v29::SchemaValue {
                        database_id: Some(objects_v29::DatabaseId {
                            value: Some(objects_v29::database_id::Value::User(42)),
                        }),
                        name: "sch".into(),
                        owner_id: Some(OWNER_ROLE_ID_v29),
                        privileges: vec![objects_v29::MzAclItem {
                            grantee: Some(objects_v29::RoleId {
                                value: Some(objects_v29::role_id::Value::User(2)),
                            }),
                            grantor: Some(OWNER_ROLE_ID_v29),
                            acl_mode: Some(ACL_MODE_USAGE_v29),
                        }],
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

        // Read back the default privileges.
        let default_privileges: TypedCollection<
            objects_v30::DefaultPrivilegesKey,
            objects_v30::DefaultPrivilegesValue,
        > = TypedCollection::new("default_privileges");
        let privileges = default_privileges.iter(&mut stash).await.unwrap();
        // Filter down to just the keys and values to make comparisons easier.
        let privileges: Vec<_> = privileges
            .into_iter()
            .map(|((key, value), _timestamp, _diff)| (key, value))
            .collect();

        assert!(privileges.contains(&(
            objects_v30::DefaultPrivilegesKey {
                role_id: Some(PUBLIC_ROLE_ID),
                database_id: None,
                schema_id: None,
                object_type: objects_v30::ObjectType::Database.into(),
                grantee: Some(MZ_INTROSPECTION_ROLE_ID),
            },
            objects_v30::DefaultPrivilegesValue {
                privileges: Some(ACL_MODE_USAGE),
            }
        )));
        assert!(privileges.contains(&(
            objects_v30::DefaultPrivilegesKey {
                role_id: Some(PUBLIC_ROLE_ID),
                database_id: None,
                schema_id: None,
                object_type: objects_v30::ObjectType::Schema.into(),
                grantee: Some(MZ_INTROSPECTION_ROLE_ID),
            },
            objects_v30::DefaultPrivilegesValue {
                privileges: Some(ACL_MODE_USAGE),
            }
        )));

        // Read back the databases.
        let databases_v30: TypedCollection<objects_v30::DatabaseKey, objects_v30::DatabaseValue> =
            TypedCollection::new("database");
        let databases = databases_v30.iter(&mut stash).await.unwrap();
        // Filter down to just the keys and values to make comparisons easier.
        let mut databases_v30: Vec<_> = databases
            .into_iter()
            .map(|((key, value), _timestamp, _diff)| (key, value))
            .collect();
        databases_v30.sort();

        assert_eq!(
            databases_v30,
            vec![(
                objects_v30::DatabaseKey {
                    id: Some(objects_v30::DatabaseId {
                        value: Some(objects_v30::database_id::Value::User(42)),
                    }),
                },
                objects_v30::DatabaseValue {
                    name: "db".into(),
                    owner_id: Some(OWNER_ROLE_ID_v30),
                    privileges: vec![
                        objects_v30::MzAclItem {
                            grantee: Some(objects_v30::RoleId {
                                value: Some(objects_v30::role_id::Value::User(2)),
                            }),
                            grantor: Some(OWNER_ROLE_ID_v30),
                            acl_mode: Some(ACL_MODE_USAGE),
                        },
                        objects_v30::MzAclItem {
                            grantee: Some(MZ_INTROSPECTION_ROLE_ID),
                            grantor: Some(OWNER_ROLE_ID_v30),
                            acl_mode: Some(ACL_MODE_USAGE),
                        }
                    ],
                },
            )],
        );

        // Read back the schemas.
        let schemas_v30: TypedCollection<objects_v30::SchemaKey, objects_v30::SchemaValue> =
            TypedCollection::new("schema");
        let schemas = schemas_v30.iter(&mut stash).await.unwrap();
        // Filter down to just the keys and values to make comparisons easier.
        let mut schemas_v30: Vec<_> = schemas
            .into_iter()
            .map(|((key, value), _timestamp, _diff)| (key, value))
            .collect();
        schemas_v30.sort();

        assert_eq!(
            schemas_v30,
            vec![(
                objects_v30::SchemaKey {
                    id: Some(objects_v30::SchemaId {
                        value: Some(objects_v30::schema_id::Value::User(42)),
                    }),
                },
                objects_v30::SchemaValue {
                    database_id: Some(objects_v30::DatabaseId {
                        value: Some(objects_v30::database_id::Value::User(42)),
                    }),
                    name: "sch".into(),
                    owner_id: Some(OWNER_ROLE_ID_v30),
                    privileges: vec![
                        objects_v30::MzAclItem {
                            grantee: Some(objects_v30::RoleId {
                                value: Some(objects_v30::role_id::Value::User(2)),
                            }),
                            grantor: Some(OWNER_ROLE_ID_v30),
                            acl_mode: Some(ACL_MODE_USAGE),
                        },
                        objects_v30::MzAclItem {
                            grantee: Some(MZ_INTROSPECTION_ROLE_ID),
                            grantor: Some(OWNER_ROLE_ID_v30),
                            acl_mode: Some(ACL_MODE_USAGE),
                        }
                    ],
                },
            )],
        );
    }
}

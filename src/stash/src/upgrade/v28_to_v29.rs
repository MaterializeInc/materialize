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
use std::collections::BTreeSet;

pub mod objects_v28 {
    include!(concat!(env!("OUT_DIR"), "/objects_v28.rs"));
}

pub mod objects_v29 {
    include!(concat!(env!("OUT_DIR"), "/objects_v29.rs"));
}

/// Remove dangling references from default privileges.
///
/// Author - jkosh44
pub async fn upgrade(tx: &'_ mut Transaction<'_>) -> Result<(), StashError> {
    const DEFAULT_PRIVILEGES_COLLECTION: TypedCollection<
        objects_v28::DefaultPrivilegesKey,
        objects_v28::DefaultPrivilegesValue,
    > = TypedCollection::new("default_privileges");
    const DATABASES_COLLECTION: TypedCollection<
        objects_v28::DatabaseKey,
        objects_v28::DatabaseValue,
    > = TypedCollection::new("database");
    const SCHEMAS_COLLECTION: TypedCollection<objects_v28::SchemaKey, objects_v28::SchemaValue> =
        TypedCollection::new("schema");

    let database_ids: BTreeSet<_> = tx
        .peek_one(
            tx.collection::<objects_v28::DatabaseKey, objects_v28::DatabaseValue>(
                DATABASES_COLLECTION.name(),
            )
            .await?,
        )
        .await?
        .keys()
        .filter_map(|key: &objects_v28::DatabaseKey| key.id.clone())
        .collect();

    let schema_ids: BTreeSet<_> = tx
        .peek_one(
            tx.collection::<objects_v28::SchemaKey, objects_v28::SchemaValue>(
                SCHEMAS_COLLECTION.name(),
            )
            .await?,
        )
        .await?
        .keys()
        .filter_map(|key: &objects_v28::SchemaKey| key.id.clone())
        .collect();

    DEFAULT_PRIVILEGES_COLLECTION
        .migrate_to(tx, |entries| {
            let mut updates = Vec::new();

            for (key, value) in entries {
                if matches!(&key.database_id, Some(database_id) if !database_ids.contains(database_id))
                    || matches!(&key.schema_id, Some(schema_id) if !schema_ids.contains(schema_id)) {
                    updates.push(MigrationAction::Delete(key.clone()));
                } else {
                    let new_key: objects_v29::DefaultPrivilegesKey = key.clone().into();
                    let new_value: objects_v29::DefaultPrivilegesValue = value.clone().into();
                    updates.push(MigrationAction::Update(key.clone(), (new_key, new_value)));
                }
            }

            updates
        })
        .await?;

    Ok(())
}

impl From<objects_v28::DatabaseId> for objects_v29::DatabaseId {
    fn from(id: objects_v28::DatabaseId) -> Self {
        let value = match id.value {
            None => None,
            Some(objects_v28::database_id::Value::User(id)) => {
                Some(objects_v29::database_id::Value::User(id))
            }
            Some(objects_v28::database_id::Value::System(id)) => {
                Some(objects_v29::database_id::Value::System(id))
            }
        };
        objects_v29::DatabaseId { value }
    }
}

impl From<objects_v28::DatabaseKey> for objects_v29::DatabaseKey {
    fn from(key: objects_v28::DatabaseKey) -> Self {
        Self {
            id: key.id.map(Into::into),
        }
    }
}

impl From<objects_v28::RoleId> for objects_v29::RoleId {
    fn from(id: objects_v28::RoleId) -> Self {
        let value = match id.value {
            None => None,
            Some(objects_v28::role_id::Value::User(id)) => {
                Some(objects_v29::role_id::Value::User(id))
            }
            Some(objects_v28::role_id::Value::System(id)) => {
                Some(objects_v29::role_id::Value::System(id))
            }
            Some(objects_v28::role_id::Value::Public(_)) => {
                Some(objects_v29::role_id::Value::Public(Default::default()))
            }
        };
        objects_v29::RoleId { value }
    }
}

impl From<objects_v28::AclMode> for objects_v29::AclMode {
    fn from(acl_mode: objects_v28::AclMode) -> Self {
        objects_v29::AclMode {
            bitflags: acl_mode.bitflags,
        }
    }
}

impl From<objects_v28::MzAclItem> for objects_v29::MzAclItem {
    fn from(item: objects_v28::MzAclItem) -> Self {
        Self {
            grantee: item.grantee.map(Into::into),
            grantor: item.grantor.map(Into::into),
            acl_mode: item.acl_mode.map(Into::into),
        }
    }
}

impl From<objects_v28::DatabaseValue> for objects_v29::DatabaseValue {
    fn from(value: objects_v28::DatabaseValue) -> Self {
        Self {
            name: value.name,
            owner_id: value.owner_id.map(Into::into),
            privileges: value.privileges.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<objects_v28::SchemaId> for objects_v29::SchemaId {
    fn from(id: objects_v28::SchemaId) -> Self {
        let value = match id.value {
            None => None,
            Some(objects_v28::schema_id::Value::User(id)) => {
                Some(objects_v29::schema_id::Value::User(id))
            }
            Some(objects_v28::schema_id::Value::System(id)) => {
                Some(objects_v29::schema_id::Value::System(id))
            }
        };
        objects_v29::SchemaId { value }
    }
}

impl From<objects_v28::SchemaKey> for objects_v29::SchemaKey {
    fn from(key: objects_v28::SchemaKey) -> Self {
        Self {
            id: key.id.map(Into::into),
        }
    }
}

impl From<objects_v28::SchemaValue> for objects_v29::SchemaValue {
    fn from(value: objects_v28::SchemaValue) -> Self {
        Self {
            database_id: value.database_id.map(Into::into),
            name: value.name,
            owner_id: value.owner_id.map(Into::into),
            privileges: value.privileges.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<objects_v28::ObjectType> for objects_v29::ObjectType {
    fn from(object_type: objects_v28::ObjectType) -> Self {
        match object_type {
            objects_v28::ObjectType::Unknown => objects_v29::ObjectType::Unknown,
            objects_v28::ObjectType::Table => objects_v29::ObjectType::Table,
            objects_v28::ObjectType::View => objects_v29::ObjectType::View,
            objects_v28::ObjectType::MaterializedView => objects_v29::ObjectType::MaterializedView,
            objects_v28::ObjectType::Source => objects_v29::ObjectType::Source,
            objects_v28::ObjectType::Sink => objects_v29::ObjectType::Sink,
            objects_v28::ObjectType::Index => objects_v29::ObjectType::Index,
            objects_v28::ObjectType::Type => objects_v29::ObjectType::Type,
            objects_v28::ObjectType::Role => objects_v29::ObjectType::Role,
            objects_v28::ObjectType::Cluster => objects_v29::ObjectType::Cluster,
            objects_v28::ObjectType::ClusterReplica => objects_v29::ObjectType::ClusterReplica,
            objects_v28::ObjectType::Secret => objects_v29::ObjectType::Secret,
            objects_v28::ObjectType::Connection => objects_v29::ObjectType::Connection,
            objects_v28::ObjectType::Database => objects_v29::ObjectType::Database,
            objects_v28::ObjectType::Schema => objects_v29::ObjectType::Schema,
            objects_v28::ObjectType::Func => objects_v29::ObjectType::Func,
        }
    }
}

impl From<objects_v28::DefaultPrivilegesKey> for objects_v29::DefaultPrivilegesKey {
    fn from(key: objects_v28::DefaultPrivilegesKey) -> Self {
        objects_v29::DefaultPrivilegesKey {
            role_id: key.role_id.map(Into::into),
            database_id: key.database_id.map(Into::into),
            schema_id: key.schema_id.map(Into::into),
            object_type: key.object_type,
            grantee: key.grantee.map(Into::into),
        }
    }
}

impl From<objects_v28::DefaultPrivilegesValue> for objects_v29::DefaultPrivilegesValue {
    fn from(value: objects_v28::DefaultPrivilegesValue) -> Self {
        objects_v29::DefaultPrivilegesValue {
            privileges: value.privileges.map(Into::into),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::upgrade;

    use crate::upgrade::v28_to_v29::{objects_v28, objects_v29};
    use crate::{DebugStashFactory, TypedCollection};

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test() {
        const DATABASE_ID_V28: objects_v28::DatabaseKey = objects_v28::DatabaseKey {
            id: Some(objects_v28::DatabaseId {
                value: Some(objects_v28::database_id::Value::User(42)),
            }),
        };
        const DANGLING_DATABASE_ID_V28: objects_v28::DatabaseKey = objects_v28::DatabaseKey {
            id: Some(objects_v28::DatabaseId {
                value: Some(objects_v28::database_id::Value::User(1)),
            }),
        };
        const DATABASE_ID_V29: objects_v29::DatabaseKey = objects_v29::DatabaseKey {
            id: Some(objects_v29::DatabaseId {
                value: Some(objects_v29::database_id::Value::User(42)),
            }),
        };

        const SCHEMA_ID_V28: objects_v28::SchemaKey = objects_v28::SchemaKey {
            id: Some(objects_v28::SchemaId {
                value: Some(objects_v28::schema_id::Value::User(42)),
            }),
        };
        const DANGLING_SCHEMA_ID_V28: objects_v28::SchemaKey = objects_v28::SchemaKey {
            id: Some(objects_v28::SchemaId {
                value: Some(objects_v28::schema_id::Value::User(1)),
            }),
        };
        const SCHEMA_ID_V29: objects_v29::SchemaKey = objects_v29::SchemaKey {
            id: Some(objects_v29::SchemaId {
                value: Some(objects_v29::schema_id::Value::User(42)),
            }),
        };

        const ACL_MODE_USAGE_V28: objects_v28::AclMode = objects_v28::AclMode { bitflags: 1 << 8 };
        const ACL_MODE_USAGE_V29: objects_v29::AclMode = objects_v29::AclMode { bitflags: 1 << 8 };
        const ROLE_ID_V28: objects_v28::RoleId = objects_v28::RoleId {
            value: Some(objects_v28::role_id::Value::User(1)),
        };
        const ROLE_ID_V29: objects_v29::RoleId = objects_v29::RoleId {
            value: Some(objects_v29::role_id::Value::User(1)),
        };

        // Connect to the Stash.
        let factory = DebugStashFactory::new().await;
        let mut stash = factory.open_debug().await;

        // Insert a database.
        let databases_v28: TypedCollection<objects_v28::DatabaseKey, objects_v28::DatabaseValue> =
            TypedCollection::new("database");
        databases_v28
            .insert_without_overwrite(
                &mut stash,
                vec![(
                    DATABASE_ID_V28,
                    objects_v28::DatabaseValue {
                        name: "db".into(),
                        owner_id: Some(ROLE_ID_V28),
                        privileges: vec![],
                    },
                )],
            )
            .await
            .unwrap();

        // Insert a schema.
        let schemas_v28: TypedCollection<objects_v28::SchemaKey, objects_v28::SchemaValue> =
            TypedCollection::new("schema");
        schemas_v28
            .insert_without_overwrite(
                &mut stash,
                vec![(
                    SCHEMA_ID_V28,
                    objects_v28::SchemaValue {
                        database_id: Some(objects_v28::DatabaseId {
                            value: Some(objects_v28::database_id::Value::User(42)),
                        }),
                        name: "sch".into(),
                        owner_id: Some(ROLE_ID_V28),
                        privileges: vec![],
                    },
                )],
            )
            .await
            .unwrap();

        // Insert default privileges.
        let default_privileges_v28: TypedCollection<
            objects_v28::DefaultPrivilegesKey,
            objects_v28::DefaultPrivilegesValue,
        > = TypedCollection::new("default_privileges");
        default_privileges_v28
            .insert_without_overwrite(
                &mut stash,
                vec![
                    // Valid references
                    (
                        objects_v28::DefaultPrivilegesKey {
                            role_id: Some(ROLE_ID_V28),
                            database_id: DATABASE_ID_V28.id,
                            schema_id: SCHEMA_ID_V28.id,
                            object_type: objects_v28::ObjectType::Table.into(),
                            grantee: Some(ROLE_ID_V28),
                        },
                        objects_v28::DefaultPrivilegesValue {
                            privileges: Some(ACL_MODE_USAGE_V28),
                        },
                    ),
                    // Dangling database.
                    (
                        objects_v28::DefaultPrivilegesKey {
                            role_id: Some(ROLE_ID_V28),
                            database_id: DANGLING_DATABASE_ID_V28.id,
                            schema_id: SCHEMA_ID_V28.id,
                            object_type: objects_v28::ObjectType::Table.into(),
                            grantee: Some(ROLE_ID_V28),
                        },
                        objects_v28::DefaultPrivilegesValue {
                            privileges: Some(ACL_MODE_USAGE_V28),
                        },
                    ),
                    // Dangling schema.
                    (
                        objects_v28::DefaultPrivilegesKey {
                            role_id: Some(ROLE_ID_V28),
                            database_id: DATABASE_ID_V28.id,
                            schema_id: DANGLING_SCHEMA_ID_V28.id,
                            object_type: objects_v28::ObjectType::Table.into(),
                            grantee: Some(ROLE_ID_V28),
                        },
                        objects_v28::DefaultPrivilegesValue {
                            privileges: Some(ACL_MODE_USAGE_V28),
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

        // Read back the default privileges.
        let default_privileges: TypedCollection<
            objects_v29::DefaultPrivilegesKey,
            objects_v29::DefaultPrivilegesValue,
        > = TypedCollection::new("default_privileges");
        let privileges = default_privileges.iter(&mut stash).await.unwrap();
        // Filter down to just the keys and values to make comparisons easier.
        let privileges: Vec<_> = privileges
            .into_iter()
            .map(|((key, value), _timestamp, _diff)| (key, value))
            .collect();

        assert_eq!(
            privileges,
            vec![(
                objects_v29::DefaultPrivilegesKey {
                    role_id: Some(ROLE_ID_V29),
                    database_id: DATABASE_ID_V29.id,
                    schema_id: SCHEMA_ID_V29.id,
                    object_type: objects_v29::ObjectType::Table.into(),
                    grantee: Some(ROLE_ID_V29),
                },
                objects_v29::DefaultPrivilegesValue {
                    privileges: Some(ACL_MODE_USAGE_V29),
                }
            )]
        );
    }
}

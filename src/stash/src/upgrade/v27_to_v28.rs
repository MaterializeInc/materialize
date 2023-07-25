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

pub mod objects_v27 {
    include!(concat!(env!("OUT_DIR"), "/objects_v27.rs"));
}

pub mod objects_v28 {
    include!(concat!(env!("OUT_DIR"), "/objects_v28.rs"));
}

wire_compatible!(objects_v28::DefaultPrivilegesKey with objects_v27::DefaultPrivilegesKey);
wire_compatible!(objects_v28::DefaultPrivilegesValue with objects_v27::DefaultPrivilegesValue);

wire_compatible!(objects_v28::DatabaseKey with objects_v27::DatabaseKey);
wire_compatible!(objects_v28::DatabaseValue with objects_v27::DatabaseValue);

wire_compatible!(objects_v28::SchemaKey with objects_v27::SchemaKey);
wire_compatible!(objects_v28::SchemaValue with objects_v27::SchemaValue);

const MZ_INTROSPECTION_ROLE_ID: objects_v28::RoleId = objects_v28::RoleId {
    value: Some(objects_v28::role_id::Value::System(2)),
};
const PUBLIC_ROLE_ID: objects_v28::RoleId = objects_v28::RoleId {
    value: Some(objects_v28::role_id::Value::Public(objects_v28::Empty {})),
};
const ACL_MODE_USAGE: objects_v28::AclMode = objects_v28::AclMode { bitflags: 1 << 8 };

/// Add USAGE privileges on all databases and schemas to the
/// mz_introspection user.
///
/// Author - jkosh44
pub async fn upgrade(tx: &'_ mut Transaction<'_>) -> Result<(), StashError> {
    const DEFAULT_PRIVILEGES_COLLECTION: TypedCollection<
        objects_v27::DefaultPrivilegesKey,
        objects_v27::DefaultPrivilegesValue,
    > = TypedCollection::new("default_privileges");
    const DATABASES_COLLECTION: TypedCollection<
        objects_v27::DatabaseKey,
        objects_v27::DatabaseValue,
    > = TypedCollection::new("database");
    const SCHEMAS_COLLECTION: TypedCollection<objects_v27::SchemaKey, objects_v27::SchemaValue> =
        TypedCollection::new("schema");

    DEFAULT_PRIVILEGES_COLLECTION
        .migrate_compat::<objects_v28::DefaultPrivilegesKey, objects_v28::DefaultPrivilegesValue>(
            tx,
            |entries| {
                let mut updates = vec![
                    MigrationAction::Insert(
                        objects_v28::DefaultPrivilegesKey {
                            role_id: Some(PUBLIC_ROLE_ID),
                            database_id: None,
                            schema_id: None,
                            object_type: objects_v28::ObjectType::Database.into(),
                            grantee: Some(MZ_INTROSPECTION_ROLE_ID),
                        },
                        objects_v28::DefaultPrivilegesValue {
                            privileges: Some(ACL_MODE_USAGE),
                        },
                    ),
                    MigrationAction::Insert(
                        objects_v28::DefaultPrivilegesKey {
                            role_id: Some(PUBLIC_ROLE_ID),
                            database_id: None,
                            schema_id: None,
                            object_type: objects_v27::ObjectType::Schema.into(),
                            grantee: Some(MZ_INTROSPECTION_ROLE_ID),
                        },
                        objects_v28::DefaultPrivilegesValue {
                            privileges: Some(ACL_MODE_USAGE),
                        },
                    ),
                ];

                for (key, value) in entries {
                    let new_key = key.clone();
                    let new_value = value.clone();
                    updates.push(MigrationAction::Update(key.clone(), (new_key, new_value)));
                }

                updates
            },
        )
        .await?;

    DATABASES_COLLECTION
        .migrate_compat::<objects_v28::DatabaseKey, objects_v28::DatabaseValue>(tx, |entries| {
            let mut updates = Vec::with_capacity(entries.len());

            for (key, value) in entries {
                let new_key = key.clone();
                let mut new_value = value.clone();
                new_value.privileges.push(objects_v28::MzAclItem {
                    grantee: Some(MZ_INTROSPECTION_ROLE_ID),
                    grantor: value.owner_id.clone().map(Into::into),
                    acl_mode: Some(ACL_MODE_USAGE),
                });
                updates.push(MigrationAction::Update(key.clone(), (new_key, new_value)));
            }

            updates
        })
        .await?;

    SCHEMAS_COLLECTION
        .migrate_compat::<objects_v28::SchemaKey, objects_v28::SchemaValue>(tx, |entries| {
            let mut updates = Vec::with_capacity(entries.len());

            for (key, value) in entries {
                let new_key = key.clone();
                let mut new_value = value.clone();
                new_value.privileges.push(objects_v28::MzAclItem {
                    grantee: Some(MZ_INTROSPECTION_ROLE_ID),
                    grantor: value.owner_id.clone(),
                    acl_mode: Some(ACL_MODE_USAGE),
                });
                updates.push(MigrationAction::Update(key.clone(), (new_key, new_value)));
            }

            updates
        })
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::upgrade;

    use crate::upgrade::v27_to_v28::{
        objects_v27, objects_v28, ACL_MODE_USAGE, MZ_INTROSPECTION_ROLE_ID, PUBLIC_ROLE_ID,
    };
    use crate::{DebugStashFactory, TypedCollection};

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test() {
        const ACL_MODE_USAGE_V27: objects_v27::AclMode = objects_v27::AclMode { bitflags: 1 << 8 };
        const OWNER_ROLE_ID_V27: objects_v27::RoleId = objects_v27::RoleId {
            value: Some(objects_v27::role_id::Value::User(1)),
        };
        const OWNER_ROLE_ID_V28: objects_v28::RoleId = objects_v28::RoleId {
            value: Some(objects_v28::role_id::Value::User(1)),
        };

        // Connect to the Stash.
        let factory = DebugStashFactory::new().await;
        let mut stash = factory.open_debug().await;

        // Insert a database.
        let databases_v27: TypedCollection<objects_v27::DatabaseKey, objects_v27::DatabaseValue> =
            TypedCollection::new("database");
        databases_v27
            .insert_without_overwrite(
                &mut stash,
                vec![(
                    objects_v27::DatabaseKey {
                        id: Some(objects_v27::DatabaseId {
                            value: Some(objects_v27::database_id::Value::User(42)),
                        }),
                    },
                    objects_v27::DatabaseValue {
                        name: "db".into(),
                        owner_id: Some(OWNER_ROLE_ID_V27),
                        privileges: vec![objects_v27::MzAclItem {
                            grantee: Some(objects_v27::RoleId {
                                value: Some(objects_v27::role_id::Value::User(2)),
                            }),
                            grantor: Some(OWNER_ROLE_ID_V27),
                            acl_mode: Some(ACL_MODE_USAGE_V27),
                        }],
                    },
                )],
            )
            .await
            .unwrap();

        // Insert a schema.
        let schemas_v27: TypedCollection<objects_v27::SchemaKey, objects_v27::SchemaValue> =
            TypedCollection::new("schema");
        schemas_v27
            .insert_without_overwrite(
                &mut stash,
                vec![(
                    objects_v27::SchemaKey {
                        id: Some(objects_v27::SchemaId {
                            value: Some(objects_v27::schema_id::Value::User(42)),
                        }),
                    },
                    objects_v27::SchemaValue {
                        database_id: Some(objects_v27::DatabaseId {
                            value: Some(objects_v27::database_id::Value::User(42)),
                        }),
                        name: "sch".into(),
                        owner_id: Some(OWNER_ROLE_ID_V27),
                        privileges: vec![objects_v27::MzAclItem {
                            grantee: Some(objects_v27::RoleId {
                                value: Some(objects_v27::role_id::Value::User(2)),
                            }),
                            grantor: Some(OWNER_ROLE_ID_V27),
                            acl_mode: Some(ACL_MODE_USAGE_V27),
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
            objects_v28::DefaultPrivilegesKey,
            objects_v28::DefaultPrivilegesValue,
        > = TypedCollection::new("default_privileges");
        let privileges = default_privileges.iter(&mut stash).await.unwrap();
        // Filter down to just the keys and values to make comparisons easier.
        let privileges: Vec<_> = privileges
            .into_iter()
            .map(|((key, value), _timestamp, _diff)| (key, value))
            .collect();

        assert!(privileges.contains(&(
            objects_v28::DefaultPrivilegesKey {
                role_id: Some(PUBLIC_ROLE_ID),
                database_id: None,
                schema_id: None,
                object_type: objects_v28::ObjectType::Database.into(),
                grantee: Some(MZ_INTROSPECTION_ROLE_ID),
            },
            objects_v28::DefaultPrivilegesValue {
                privileges: Some(ACL_MODE_USAGE),
            }
        )));
        assert!(privileges.contains(&(
            objects_v28::DefaultPrivilegesKey {
                role_id: Some(PUBLIC_ROLE_ID),
                database_id: None,
                schema_id: None,
                object_type: objects_v28::ObjectType::Schema.into(),
                grantee: Some(MZ_INTROSPECTION_ROLE_ID),
            },
            objects_v28::DefaultPrivilegesValue {
                privileges: Some(ACL_MODE_USAGE),
            }
        )));

        // Read back the databases.
        let databases_v28: TypedCollection<objects_v28::DatabaseKey, objects_v28::DatabaseValue> =
            TypedCollection::new("database");
        let databases = databases_v28.iter(&mut stash).await.unwrap();
        // Filter down to just the keys and values to make comparisons easier.
        let mut databases_v28: Vec<_> = databases
            .into_iter()
            .map(|((key, value), _timestamp, _diff)| (key, value))
            .collect();
        databases_v28.sort();

        assert_eq!(
            databases_v28,
            vec![(
                objects_v28::DatabaseKey {
                    id: Some(objects_v28::DatabaseId {
                        value: Some(objects_v28::database_id::Value::User(42)),
                    }),
                },
                objects_v28::DatabaseValue {
                    name: "db".into(),
                    owner_id: Some(OWNER_ROLE_ID_V28),
                    privileges: vec![
                        objects_v28::MzAclItem {
                            grantee: Some(objects_v28::RoleId {
                                value: Some(objects_v28::role_id::Value::User(2)),
                            }),
                            grantor: Some(OWNER_ROLE_ID_V28),
                            acl_mode: Some(ACL_MODE_USAGE),
                        },
                        objects_v28::MzAclItem {
                            grantee: Some(MZ_INTROSPECTION_ROLE_ID),
                            grantor: Some(OWNER_ROLE_ID_V28),
                            acl_mode: Some(ACL_MODE_USAGE),
                        }
                    ],
                },
            )],
        );

        // Read back the schemas.
        let schemas_v28: TypedCollection<objects_v28::SchemaKey, objects_v28::SchemaValue> =
            TypedCollection::new("schema");
        let schemas = schemas_v28.iter(&mut stash).await.unwrap();
        // Filter down to just the keys and values to make comparisons easier.
        let mut schemas_v28: Vec<_> = schemas
            .into_iter()
            .map(|((key, value), _timestamp, _diff)| (key, value))
            .collect();
        schemas_v28.sort();

        assert_eq!(
            schemas_v28,
            vec![(
                objects_v28::SchemaKey {
                    id: Some(objects_v28::SchemaId {
                        value: Some(objects_v28::schema_id::Value::User(42)),
                    }),
                },
                objects_v28::SchemaValue {
                    database_id: Some(objects_v28::DatabaseId {
                        value: Some(objects_v28::database_id::Value::User(42)),
                    }),
                    name: "sch".into(),
                    owner_id: Some(OWNER_ROLE_ID_V28),
                    privileges: vec![
                        objects_v28::MzAclItem {
                            grantee: Some(objects_v28::RoleId {
                                value: Some(objects_v28::role_id::Value::User(2)),
                            }),
                            grantor: Some(OWNER_ROLE_ID_V28),
                            acl_mode: Some(ACL_MODE_USAGE),
                        },
                        objects_v28::MzAclItem {
                            grantee: Some(MZ_INTROSPECTION_ROLE_ID),
                            grantor: Some(OWNER_ROLE_ID_V28),
                            acl_mode: Some(ACL_MODE_USAGE),
                        }
                    ],
                },
            )],
        );
    }
}

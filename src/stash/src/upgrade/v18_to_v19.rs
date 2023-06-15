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

pub mod objects_v18 {
    include!(concat!(env!("OUT_DIR"), "/objects_v18.rs"));
}
pub mod objects_v19 {
    include!(concat!(env!("OUT_DIR"), "/objects_v19.rs"));
}

/// Some Items have the incorrect schema namespace of User, this migration fixes them to have a
/// namespace of System.
pub async fn upgrade(tx: &'_ mut Transaction<'_>) -> Result<(), StashError> {
    const ITEM_COLLECTION: TypedCollection<objects_v18::ItemKey, objects_v18::ItemValue> =
        TypedCollection::new("item");

    ITEM_COLLECTION
        .migrate_to(tx, |entries| {
            let mut updates = vec![];

            for (item_key, item_value) in entries {
                if let Some(gid) = &item_key.gid {
                    // If the GlobalId is in the system namespace, the SchemaId should be too.
                    if gid.is_system() {
                        let new_key = objects_v19::ItemKey::from(item_key.clone());
                        let mut new_value = objects_v19::ItemValue::from(item_value.clone());

                        if let Some(schema_id) = new_value.schema_id {
                            // Our GlobalId is in the system namespace, but our SchemaId is in the
                            // user namespace? We need to update the SchemaId.
                            if let Some(objects_v19::schema_id::Value::User(id)) = schema_id.value {
                                // ** THE ACTUAL MIGRATION **
                                //
                                // Map from the user namespace to the system namespace.
                                //
                                let new_schema_id = objects_v19::SchemaId {
                                    value: Some(objects_v19::schema_id::Value::System(id)),
                                };
                                new_value.schema_id = Some(new_schema_id);

                                updates.push(MigrationAction::Update(
                                    item_key.clone(),
                                    (new_key, new_value),
                                ));
                            }
                        } else {
                            // schema_id in ItemValue should __never__ be None, so complain loudly
                            // if it is. But don't panic because maybe some downstream code handles
                            // this already?
                            tracing::error!("Found None for an ItemValue schema_id! {new_value:?}");
                        }
                    }
                }
            }

            updates
        })
        .await?;

    Ok(())
}

impl objects_v18::GlobalId {
    fn is_system(&self) -> bool {
        matches!(self.value, Some(objects_v18::global_id::Value::System(_)))
    }
}

impl From<objects_v18::ItemKey> for objects_v19::ItemKey {
    fn from(key: objects_v18::ItemKey) -> Self {
        objects_v19::ItemKey {
            gid: key.gid.map(Into::into),
        }
    }
}

impl From<objects_v18::ItemValue> for objects_v19::ItemValue {
    fn from(item: objects_v18::ItemValue) -> Self {
        objects_v19::ItemValue {
            schema_id: item.schema_id.map(Into::into),
            name: item.name,
            definition: item.definition.map(Into::into),
            owner_id: item.owner_id.map(Into::into),
            privileges: item.privileges.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<objects_v18::GlobalId> for objects_v19::GlobalId {
    fn from(id: objects_v18::GlobalId) -> Self {
        let value = match id.value {
            None => None,
            Some(objects_v18::global_id::Value::User(id)) => {
                Some(objects_v19::global_id::Value::User(id))
            }
            Some(objects_v18::global_id::Value::System(id)) => {
                Some(objects_v19::global_id::Value::System(id))
            }
            Some(objects_v18::global_id::Value::Transient(id)) => {
                Some(objects_v19::global_id::Value::Transient(id))
            }
            Some(objects_v18::global_id::Value::Explain(_)) => {
                Some(objects_v19::global_id::Value::Explain(Default::default()))
            }
        };
        objects_v19::GlobalId { value }
    }
}

impl From<objects_v18::SchemaId> for objects_v19::SchemaId {
    fn from(id: objects_v18::SchemaId) -> Self {
        let value = match id.value {
            None => None,
            Some(objects_v18::schema_id::Value::User(id)) => {
                Some(objects_v19::schema_id::Value::User(id))
            }
            Some(objects_v18::schema_id::Value::System(id)) => {
                Some(objects_v19::schema_id::Value::System(id))
            }
        };
        objects_v19::SchemaId { value }
    }
}

impl From<objects_v18::CatalogItem> for objects_v19::CatalogItem {
    fn from(item: objects_v18::CatalogItem) -> Self {
        let value = match item.value {
            None => None,
            Some(objects_v18::catalog_item::Value::V1(v1)) => Some(
                objects_v19::catalog_item::Value::V1(objects_v19::catalog_item::V1 {
                    create_sql: v1.create_sql,
                }),
            ),
        };
        objects_v19::CatalogItem { value }
    }
}

impl From<objects_v18::RoleId> for objects_v19::RoleId {
    fn from(id: objects_v18::RoleId) -> Self {
        let value = match id.value {
            None => None,
            Some(objects_v18::role_id::Value::User(id)) => {
                Some(objects_v19::role_id::Value::User(id))
            }
            Some(objects_v18::role_id::Value::System(id)) => {
                Some(objects_v19::role_id::Value::System(id))
            }
            Some(objects_v18::role_id::Value::Public(_)) => {
                Some(objects_v19::role_id::Value::Public(Default::default()))
            }
        };
        objects_v19::RoleId { value }
    }
}

impl From<objects_v18::MzAclItem> for objects_v19::MzAclItem {
    fn from(item: objects_v18::MzAclItem) -> Self {
        objects_v19::MzAclItem {
            grantee: item.grantee.map(Into::into),
            grantor: item.grantor.map(Into::into),
            acl_mode: item.acl_mode.map(Into::into),
        }
    }
}

impl From<objects_v18::AclMode> for objects_v19::AclMode {
    fn from(mode: objects_v18::AclMode) -> Self {
        objects_v19::AclMode {
            bitflags: mode.bitflags,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::objects_v18::{
        self, global_id::Value as GlobalIdInnerV18, schema_id::Value as SchemaIdInnerV18,
        GlobalId as GlobalIdV18, ItemKey as ItemKeyV18, ItemValue as ItemValueV18,
        SchemaId as SchemaIdV18,
    };
    use super::objects_v19::{
        self, global_id::Value as GlobalIdInnerV19, schema_id::Value as SchemaIdInnerV19,
        GlobalId as GlobalIdV19, SchemaId as SchemaIdV19,
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
        let items_v18: TypedCollection<objects_v18::ItemKey, objects_v18::ItemValue> =
            TypedCollection::new("item");
        items_v18
            .insert_without_overwrite(
                &mut stash,
                vec![
                    (
                        ItemKeyV18 {
                            gid: Some(GlobalIdV18 {
                                value: Some(GlobalIdInnerV18::User(42)),
                            }),
                        },
                        ItemValueV18 {
                            schema_id: Some(SchemaIdV18 {
                                value: Some(SchemaIdInnerV18::User(2)),
                            }),
                            ..Default::default()
                        },
                    ),
                    (
                        ItemKeyV18 {
                            gid: Some(GlobalIdV18 {
                                value: Some(GlobalIdInnerV18::System(43)),
                            }),
                        },
                        ItemValueV18 {
                            schema_id: Some(SchemaIdV18 {
                                value: Some(SchemaIdInnerV18::User(2)),
                            }),
                            ..Default::default()
                        },
                    ),
                    (
                        ItemKeyV18 {
                            gid: Some(GlobalIdV18 {
                                value: Some(GlobalIdInnerV18::System(44)),
                            }),
                        },
                        ItemValueV18 {
                            schema_id: Some(SchemaIdV18 {
                                value: Some(SchemaIdInnerV18::System(1)),
                            }),
                            ..Default::default()
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

        // Read back the items.
        let items_v19: TypedCollection<objects_v19::ItemKey, objects_v19::ItemValue> =
            TypedCollection::new("item");
        let items = items_v19.iter(&mut stash).await.unwrap();

        // Filter down to just GlobalIds and SchemaIds to make comparisons easier.
        let mut ids: Vec<_> = items
            .into_iter()
            .map(|((key, value), _, _)| {
                let gid = key.gid.unwrap();
                let schema_id = value.schema_id.unwrap();

                (gid, schema_id)
            })
            .collect();
        ids.sort();

        assert_eq!(
            ids,
            vec![
                // Woo! Our value got migrated.
                (
                    GlobalIdV19 {
                        value: Some(GlobalIdInnerV19::System(43))
                    },
                    SchemaIdV19 {
                        value: Some(SchemaIdInnerV19::System(2))
                    }
                ),
                // Other values did not get migrated.
                (
                    GlobalIdV19 {
                        value: Some(GlobalIdInnerV19::System(44))
                    },
                    SchemaIdV19 {
                        value: Some(SchemaIdInnerV19::System(1))
                    }
                ),
                (
                    GlobalIdV19 {
                        value: Some(GlobalIdInnerV19::User(42))
                    },
                    SchemaIdV19 {
                        value: Some(SchemaIdInnerV19::User(2))
                    }
                )
            ]
        );
    }
}

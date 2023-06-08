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

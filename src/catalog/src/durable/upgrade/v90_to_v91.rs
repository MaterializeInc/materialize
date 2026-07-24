// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::json_compatible::JsonCompatible;
use crate::durable::upgrade::objects_v90 as v90;
use crate::durable::upgrade::objects_v91 as v91;

crate::json_compatible!(v90::SchemaKey with v91::SchemaKey);
crate::json_compatible!(v90::ItemKey with v91::ItemKey);
crate::json_compatible!(v90::SchemaId with v91::SchemaId);
crate::json_compatible!(v90::DatabaseId with v91::DatabaseId);
crate::json_compatible!(v90::RoleId with v91::RoleId);
crate::json_compatible!(v90::MzAclItem with v91::MzAclItem);
crate::json_compatible!(v90::CatalogItem with v91::CatalogItem);
crate::json_compatible!(v90::GlobalId with v91::GlobalId);
crate::json_compatible!(v90::ItemVersion with v91::ItemVersion);

/// Adds the `ephemeral_owner_session` field to items and schemas, backfilling
/// it as `None`. Existing records all describe durable objects, temporary
/// objects were never written to the catalog before this field existed.
///
/// `Item` and `Schema` records gained a new field, so their stored JSON is no
/// longer readable as the v91 type and every such record is rewritten. All
/// other records are unchanged and pass through untouched. The new `Session`
/// collection starts empty, so it needs no backfill.
pub fn upgrade(
    snapshot: Vec<v90::StateUpdateKind>,
) -> Vec<MigrationAction<v90::StateUpdateKind, v91::StateUpdateKind>> {
    let mut migrations = Vec::new();
    for update in snapshot {
        match update {
            v90::StateUpdateKind::Schema(old_schema) => {
                let new_schema = migrate_schema(old_schema.clone());
                migrations.push(MigrationAction::Update(
                    v90::StateUpdateKind::Schema(old_schema),
                    v91::StateUpdateKind::Schema(new_schema),
                ));
            }
            v90::StateUpdateKind::Item(old_item) => {
                let new_item = migrate_item(old_item.clone());
                migrations.push(MigrationAction::Update(
                    v90::StateUpdateKind::Item(old_item),
                    v91::StateUpdateKind::Item(new_item),
                ));
            }
            _ => {}
        }
    }
    migrations
}

fn migrate_schema(old: v90::Schema) -> v91::Schema {
    let v90::Schema { key, value } = old;
    v91::Schema {
        key: JsonCompatible::convert(&key),
        value: v91::SchemaValue {
            database_id: value.database_id.as_ref().map(JsonCompatible::convert),
            name: value.name,
            owner_id: JsonCompatible::convert(&value.owner_id),
            privileges: value
                .privileges
                .iter()
                .map(JsonCompatible::convert)
                .collect(),
            oid: value.oid,
            ephemeral_owner_session: None,
        },
    }
}

fn migrate_item(old: v90::Item) -> v91::Item {
    let v90::Item { key, value } = old;
    v91::Item {
        key: JsonCompatible::convert(&key),
        value: v91::ItemValue {
            schema_id: JsonCompatible::convert(&value.schema_id),
            name: value.name,
            definition: JsonCompatible::convert(&value.definition),
            owner_id: JsonCompatible::convert(&value.owner_id),
            privileges: value
                .privileges
                .iter()
                .map(JsonCompatible::convert)
                .collect(),
            oid: value.oid,
            global_id: JsonCompatible::convert(&value.global_id),
            extra_versions: value
                .extra_versions
                .iter()
                .map(JsonCompatible::convert)
                .collect(),
            ephemeral_owner_session: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use crate::durable::upgrade::MigrationAction;
    use crate::durable::upgrade::v90_to_v91::upgrade;
    use crate::durable::upgrade::{objects_v90 as v90, objects_v91 as v91};

    fn schema(id: u64) -> v90::Schema {
        v90::Schema {
            key: v90::SchemaKey {
                id: v90::SchemaId::User(id),
            },
            value: v90::SchemaValue {
                database_id: Some(v90::DatabaseId::User(1)),
                name: format!("schema{id}"),
                owner_id: v90::RoleId::User(1),
                privileges: Vec::new(),
                oid: 20_000,
            },
        }
    }

    fn item(id: u64) -> v90::Item {
        v90::Item {
            key: v90::ItemKey {
                gid: v90::CatalogItemId::User(id),
            },
            value: v90::ItemValue {
                schema_id: v90::SchemaId::User(1),
                name: format!("item{id}"),
                definition: v90::CatalogItem::V1(v90::CatalogItemV1 {
                    create_sql: "CREATE VIEW v AS SELECT 1".to_string(),
                }),
                owner_id: v90::RoleId::User(1),
                privileges: Vec::new(),
                oid: 20_001,
                global_id: v90::GlobalId::User(id),
                extra_versions: Vec::new(),
            },
        }
    }

    #[mz_ore::test]
    fn backfills_items_and_schemas_as_none() {
        let migrations = upgrade(vec![
            v90::StateUpdateKind::Schema(schema(1)),
            v90::StateUpdateKind::Item(item(1)),
            v90::StateUpdateKind::Database(v90::Database {
                key: v90::DatabaseKey {
                    id: v90::DatabaseId::User(1),
                },
                value: v90::DatabaseValue {
                    name: "db".to_string(),
                    owner_id: v90::RoleId::User(1),
                    privileges: Vec::new(),
                    oid: 20_002,
                },
            }),
        ]);
        // The schema and item migrate; the database passes through.
        assert_eq!(migrations.len(), 2);

        let MigrationAction::Update(_, v91::StateUpdateKind::Schema(schema)) = &migrations[0]
        else {
            panic!("expected a schema update");
        };
        assert_eq!(schema.value.ephemeral_owner_session, None);

        let MigrationAction::Update(_, v91::StateUpdateKind::Item(item)) = &migrations[1] else {
            panic!("expected an item update");
        };
        assert_eq!(item.value.ephemeral_owner_session, None);
    }
}

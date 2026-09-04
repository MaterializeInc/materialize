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

crate::json_compatible!(v90::ItemKey with v91::ItemKey);
crate::json_compatible!(v90::SchemaId with v91::SchemaId);
crate::json_compatible!(v90::RoleId with v91::RoleId);
crate::json_compatible!(v90::MzAclItem with v91::MzAclItem);
crate::json_compatible!(v90::CatalogItem with v91::CatalogItem);
crate::json_compatible!(v90::GlobalId with v91::GlobalId);
crate::json_compatible!(v90::ItemVersion with v91::ItemVersion);

/// Adds the `ephemeral_owner_session` field to items, backfilling it as
/// `None`. Existing records all describe durable items, temporary items were
/// never written to the catalog before this field existed.
///
/// `Item` records gained a new field, so their stored JSON is no longer
/// readable as the v91 type and every such record is rewritten. All other
/// records are unchanged and pass through untouched.
///
/// NOTE: The explicit rewrite matters even though serde would default the
/// missing field to `None` on read. A later edit to an item retracts the
/// record by writing its v91 encoding (with `ephemeral_owner_session: None`)
/// at diff -1. Without the backfill, the stored record lacks the field, so
/// the retraction doesn't match it and the collection is left with negative
/// multiplicity.
pub fn upgrade(
    snapshot: Vec<v90::StateUpdateKind>,
) -> Vec<MigrationAction<v90::StateUpdateKind, v91::StateUpdateKind>> {
    let mut migrations = Vec::new();
    for update in snapshot {
        match update {
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
    fn backfills_items_as_none() {
        let migrations = upgrade(vec![
            v90::StateUpdateKind::Schema(schema(1)),
            v90::StateUpdateKind::Item(item(1)),
        ]);
        // The item migrates; the schema passes through.
        assert_eq!(migrations.len(), 1);

        let MigrationAction::Update(_, v91::StateUpdateKind::Item(item)) = &migrations[0] else {
            panic!("expected an item update");
        };
        assert_eq!(item.value.ephemeral_owner_session, None);
    }
}

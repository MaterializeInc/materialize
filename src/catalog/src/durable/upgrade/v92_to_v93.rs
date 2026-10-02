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
use crate::durable::upgrade::objects_v92 as v92;
use crate::durable::upgrade::objects_v93 as v93;

crate::json_compatible!(v92::ItemKey with v93::ItemKey);
crate::json_compatible!(v92::SchemaId with v93::SchemaId);
crate::json_compatible!(v92::RoleId with v93::RoleId);
crate::json_compatible!(v92::MzAclItem with v93::MzAclItem);
crate::json_compatible!(v92::CatalogItem with v93::CatalogItem);
crate::json_compatible!(v92::GlobalId with v93::GlobalId);
crate::json_compatible!(v92::ItemVersion with v93::ItemVersion);

/// Adds the `columns` and `index_keys` fields to items, backfilling both as
/// `None`. Only a planner can fill them in, which the catalog does at the
/// first bootstrap on this version (see `Item::columns`).
///
/// `Item` records gained new fields, so every such record is rewritten. All
/// other records are unchanged and pass through untouched.
///
/// NOTE: The explicit rewrite matters even though serde would default the
/// missing fields to `None` on read. A later edit to an item retracts the
/// record by writing its v93 encoding (with both fields present) at diff -1.
/// Without the backfill, the stored record lacks the fields, so the
/// retraction doesn't match it and the collection is left with negative
/// multiplicity.
pub fn upgrade(
    snapshot: Vec<v92::StateUpdateKind>,
) -> Vec<MigrationAction<v92::StateUpdateKind, v93::StateUpdateKind>> {
    let mut migrations = Vec::new();
    for update in snapshot {
        match update {
            v92::StateUpdateKind::Item(old_item) => {
                let new_item = migrate_item(old_item.clone());
                migrations.push(MigrationAction::Update(
                    v92::StateUpdateKind::Item(old_item),
                    v93::StateUpdateKind::Item(new_item),
                ));
            }
            _ => {}
        }
    }
    migrations
}

fn migrate_item(old: v92::Item) -> v93::Item {
    let v92::Item { key, value } = old;
    v93::Item {
        key: JsonCompatible::convert(&key),
        value: v93::ItemValue {
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
            ephemeral_owner_session: value.ephemeral_owner_session,
            columns: None,
            index_keys: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use crate::durable::upgrade::MigrationAction;
    use crate::durable::upgrade::v92_to_v93::upgrade;
    use crate::durable::upgrade::{objects_v92 as v92, objects_v93 as v93};

    fn schema(id: u64) -> v92::Schema {
        v92::Schema {
            key: v92::SchemaKey {
                id: v92::SchemaId::User(id),
            },
            value: v92::SchemaValue {
                database_id: Some(v92::DatabaseId::User(1)),
                name: format!("schema{id}"),
                owner_id: v92::RoleId::User(1),
                privileges: Vec::new(),
                oid: 20_000,
            },
        }
    }

    fn item(id: u64) -> v92::Item {
        v92::Item {
            key: v92::ItemKey {
                gid: v92::CatalogItemId::User(id),
            },
            value: v92::ItemValue {
                schema_id: v92::SchemaId::User(1),
                name: format!("item{id}"),
                definition: v92::CatalogItem::V1(v92::CatalogItemV1 {
                    create_sql: "CREATE VIEW v AS SELECT 1".to_string(),
                }),
                owner_id: v92::RoleId::User(1),
                privileges: Vec::new(),
                oid: 20_001,
                global_id: v92::GlobalId::User(id),
                extra_versions: Vec::new(),
                ephemeral_owner_session: None,
            },
        }
    }

    #[mz_ore::test]
    fn backfills_items_as_none() {
        let migrations = upgrade(vec![
            v92::StateUpdateKind::Schema(schema(1)),
            v92::StateUpdateKind::Item(item(1)),
        ]);
        // The item migrates; the schema passes through.
        assert_eq!(migrations.len(), 1);

        let MigrationAction::Update(_, v93::StateUpdateKind::Item(item)) = &migrations[0] else {
            panic!("expected an item update");
        };
        assert_eq!(item.value.columns, None);
        assert_eq!(item.value.index_keys, None);
    }
}

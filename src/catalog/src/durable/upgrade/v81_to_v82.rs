// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v81 as v81;
use crate::durable::upgrade::objects_v82 as v82;

/// Backfills `SSL MODE 'disable'` on existing Postgres connections that do not
/// specify an explicit SSL MODE.
///
/// The default SSL MODE for `CREATE CONNECTION ... TO POSTGRES` changed from
/// `disable` to `require` in v82. Without this migration, existing connections
/// that relied on the implicit `disable` default would silently switch to
/// `require` on next restart, potentially breaking connections to upstream
/// Postgres servers that do not support TLS.
pub fn upgrade(
    snapshot: Vec<v81::StateUpdateKind>,
) -> Vec<MigrationAction<v81::StateUpdateKind, v82::StateUpdateKind>> {
    let mut migrations = Vec::new();

    for update in snapshot {
        let v81::StateUpdateKind::Item(item) = &update else {
            continue;
        };

        let v81::CatalogItem::V1(def) = &item.value.definition;
        let create_sql = &def.create_sql;

        // Only process Postgres connections (case-insensitive check).
        let upper = create_sql.to_uppercase();
        if !upper.contains("TO POSTGRES") {
            continue;
        }
        // Skip if SSL MODE is already explicitly set.
        if upper.contains("SSL MODE") {
            continue;
        }

        // Inject `SSL MODE 'disable'` to preserve the prior default behavior.
        // Insert before the final closing paren of the connection options.
        let new_sql = match create_sql.rfind(')') {
            Some(pos) => {
                let (before, after) = create_sql.split_at(pos);
                format!("{},\n    SSL MODE 'disable'{}", before, after)
            }
            None => {
                // Malformed SQL — leave it alone rather than corrupt it.
                continue;
            }
        };

        // Build the updated item. Since v81 and v82 have identical schemas, we
        // clone the original and just swap in the new create_sql.
        let mut new_item = item.clone();
        new_item.value.definition = v81::CatalogItem::V1(v81::CatalogItemV1 {
            create_sql: new_sql,
        });

        // The v81 and v82 types are schema-identical, so we can roundtrip
        // through serde to convert between them.
        let old = v81::StateUpdateKind::Item(item.clone());
        let new: v82::StateUpdateKind = serde_json::from_value(
            serde_json::to_value(&v81::StateUpdateKind::Item(new_item))
                .expect("serialization cannot fail"),
        )
        .expect("v81 and v82 are schema-identical");

        migrations.push(MigrationAction::Update(old, new));
    }

    migrations
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_item(create_sql: &str) -> v81::StateUpdateKind {
        v81::StateUpdateKind::Item(v81::Item {
            key: v81::ItemKey {
                gid: v81::CatalogItemId::User(1),
            },
            value: v81::ItemValue {
                schema_id: v81::SchemaId::User(3),
                name: "pg_conn".to_string(),
                definition: v81::CatalogItem::V1(v81::CatalogItemV1 {
                    create_sql: create_sql.to_string(),
                }),
                owner_id: v81::RoleId::User(1),
                privileges: vec![],
                oid: 20000,
                global_id: v81::GlobalId::User(1),
                extra_versions: vec![],
            },
        })
    }

    #[mz_ore::test]
    fn test_backfills_postgres_without_ssl_mode() {
        let input = vec![make_item(
            "CREATE CONNECTION pg TO POSTGRES (HOST 'db.example.com', PORT 5432, DATABASE 'mydb', USER 'postgres', PASSWORD SECRET s1)",
        )];
        let actions = upgrade(input);
        assert_eq!(actions.len(), 1);
        let MigrationAction::Update(_, new) = &actions[0] else {
            panic!("expected Update action");
        };
        let v82::StateUpdateKind::Item(item) = new else {
            panic!("expected Item");
        };
        let v82::CatalogItem::V1(def) = &item.value.definition;
        assert!(
            def.create_sql.contains("SSL MODE 'disable'"),
            "should have injected SSL MODE: {}",
            def.create_sql
        );
    }

    #[mz_ore::test]
    fn test_skips_postgres_with_explicit_ssl_mode() {
        let input = vec![make_item(
            "CREATE CONNECTION pg TO POSTGRES (HOST 'db.example.com', SSL MODE 'require', DATABASE 'mydb', USER 'postgres')",
        )];
        let actions = upgrade(input);
        assert_eq!(actions.len(), 0);
    }

    #[mz_ore::test]
    fn test_skips_non_postgres_connections() {
        let input = vec![make_item(
            "CREATE CONNECTION kafka_conn TO KAFKA (BROKER 'localhost:9092')",
        )];
        let actions = upgrade(input);
        assert_eq!(actions.len(), 0);
    }

    #[mz_ore::test]
    fn test_skips_mysql_connections() {
        let input = vec![make_item(
            "CREATE CONNECTION mysql_conn TO MYSQL (HOST 'db.example.com', USER 'root')",
        )];
        let actions = upgrade(input);
        assert_eq!(actions.len(), 0);
    }
}

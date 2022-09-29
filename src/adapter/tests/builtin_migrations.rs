// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// This file is derived from the sqlparser-rs project, available at
// https://github.com/andygrove/sqlparser-rs. It was incorporated
// directly into Materialize on December 21, 2019.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use mz_adapter::catalog::{Catalog, CatalogItem, MaterializedView, Op, Table, SYSTEM_CONN_ID};
use mz_adapter::session::DEFAULT_DATABASE_NAME;
use mz_ore::now::NOW_ZERO;
use mz_repr::{GlobalId, RelationDesc, RelationType, ScalarType};
use mz_sql::ast::Expr;
use mz_sql::catalog::CatalogDatabase;
use mz_sql::names::{ObjectQualifiers, QualifiedObjectName, ResolvedDatabaseSpecifier};

use itertools::Itertools;
use mz_expr::{MirRelationExpr, OptimizedMirRelationExpr};
use mz_sql::DEFAULT_SCHEMA;
use mz_stash::Sqlite;
use std::collections::HashMap;
use std::error::Error;

enum ItemNamespace {
    System,
    User,
}

enum SimplifiedItem {
    Table,
    MaterializedView { depends_on: Vec<String> },
}

struct SimplifiedCatalogEntry {
    name: String,
    namespace: ItemNamespace,
    item: SimplifiedItem,
}

impl SimplifiedCatalogEntry {
    // A lot of the fields here aren't actually used in the test so we can fill them in with dummy
    // values.
    fn to_catalog_item(
        self,
        id_mapping: &HashMap<String, GlobalId>,
    ) -> (String, ItemNamespace, CatalogItem) {
        let item = match self.item {
            SimplifiedItem::Table => CatalogItem::Table(Table {
                create_sql: "TODO".to_string(),
                desc: RelationDesc::empty()
                    .with_column("a", ScalarType::Int32.nullable(true))
                    .with_key(vec![0]),
                defaults: vec![Expr::null(); 1],
                conn_id: None,
                depends_on: vec![],
            }),
            SimplifiedItem::MaterializedView { depends_on } => {
                let table_list = depends_on.iter().join(",");
                let depends_on = convert_name_vec_to_id_vec(depends_on, id_mapping);
                CatalogItem::MaterializedView(MaterializedView {
                    create_sql: format!(
                        "CREATE MATERIALIZED VIEW mv AS SELECT * FROM {table_list}"
                    ),
                    optimized_expr: OptimizedMirRelationExpr(MirRelationExpr::Constant {
                        rows: Ok(Vec::new()),
                        typ: RelationType {
                            column_types: Vec::new(),
                            keys: Vec::new(),
                        },
                    }),
                    desc: RelationDesc::empty()
                        .with_column("a", ScalarType::Int32.nullable(true))
                        .with_key(vec![0]),
                    depends_on,
                    compute_instance: 1,
                })
            }
        };
        (self.name, self.namespace, item)
    }
}

struct BuiltinMigrationTestCase {
    test_name: &'static str,
    initial_state: Vec<SimplifiedCatalogEntry>,
    migrated_names: Vec<String>,
    expected_previous_sink_names: Vec<String>,
    expected_previous_materialized_view_names: Vec<String>,
    expected_previous_source_names: Vec<String>,
    expected_all_drop_ops: Vec<String>,
    expected_user_drop_ops: Vec<String>,
    expected_all_create_ops: Vec<String>,
    expected_user_create_ops: Vec<String>,
}

#[tokio::test]
async fn test_builtin_migration() -> Result<(), Box<dyn Error>> {
    let test_cases = vec![
        BuiltinMigrationTestCase {
            test_name: "no_migrations",
            initial_state: vec![SimplifiedCatalogEntry {
                name: "s1".to_string(),
                namespace: ItemNamespace::System,
                item: SimplifiedItem::Table,
            }],
            migrated_names: vec![],
            expected_previous_sink_names: vec![],
            expected_previous_materialized_view_names: vec![],
            expected_previous_source_names: vec![],
            expected_all_drop_ops: vec![],
            expected_user_drop_ops: vec![],
            expected_all_create_ops: vec![],
            expected_user_create_ops: vec![],
        },
        BuiltinMigrationTestCase {
            test_name: "single_migrations",
            initial_state: vec![SimplifiedCatalogEntry {
                name: "s1".to_string(),
                namespace: ItemNamespace::System,
                item: SimplifiedItem::Table,
            }],
            migrated_names: vec!["s1".to_string()],
            expected_previous_sink_names: vec![],
            expected_previous_materialized_view_names: vec![],
            expected_previous_source_names: vec!["s1".to_string()],
            expected_all_drop_ops: vec!["s1".to_string()],
            expected_user_drop_ops: vec![],
            expected_all_create_ops: vec!["s1".to_string()],
            expected_user_create_ops: vec![],
        },
        BuiltinMigrationTestCase {
            test_name: "child_migrations",
            initial_state: vec![
                SimplifiedCatalogEntry {
                    name: "s1".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::Table,
                },
                SimplifiedCatalogEntry {
                    name: "u1".to_string(),
                    namespace: ItemNamespace::User,
                    item: SimplifiedItem::MaterializedView {
                        depends_on: vec!["s1".to_string()],
                    },
                },
            ],
            migrated_names: vec!["s1".to_string()],
            expected_previous_sink_names: vec![],
            expected_previous_materialized_view_names: vec!["u1".to_string()],
            expected_previous_source_names: vec!["s1".to_string()],
            expected_all_drop_ops: vec!["u1".to_string(), "s1".to_string()],
            expected_user_drop_ops: vec!["u1".to_string()],
            expected_all_create_ops: vec!["s1".to_string(), "u1".to_string()],
            expected_user_create_ops: vec!["u1".to_string()],
        },
        BuiltinMigrationTestCase {
            test_name: "multi_child_migrations",
            initial_state: vec![
                SimplifiedCatalogEntry {
                    name: "s1".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::Table,
                },
                SimplifiedCatalogEntry {
                    name: "u1".to_string(),
                    namespace: ItemNamespace::User,
                    item: SimplifiedItem::MaterializedView {
                        depends_on: vec!["s1".to_string()],
                    },
                },
                SimplifiedCatalogEntry {
                    name: "u2".to_string(),
                    namespace: ItemNamespace::User,
                    item: SimplifiedItem::MaterializedView {
                        depends_on: vec!["s1".to_string()],
                    },
                },
            ],
            migrated_names: vec!["s1".to_string()],
            expected_previous_sink_names: vec![],
            expected_previous_materialized_view_names: vec!["u1".to_string(), "u2".to_string()],
            expected_previous_source_names: vec!["s1".to_string()],
            expected_all_drop_ops: vec!["u2".to_string(), "u1".to_string(), "s1".to_string()],
            expected_user_drop_ops: vec!["u2".to_string(), "u1".to_string()],
            expected_all_create_ops: vec!["s1".to_string(), "u1".to_string(), "u2".to_string()],
            expected_user_create_ops: vec!["u1".to_string(), "u2".to_string()],
        },
        BuiltinMigrationTestCase {
            test_name: "topological_sort",
            initial_state: vec![
                SimplifiedCatalogEntry {
                    name: "s1".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::Table,
                },
                SimplifiedCatalogEntry {
                    name: "s2".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::Table,
                },
                SimplifiedCatalogEntry {
                    name: "u1".to_string(),
                    namespace: ItemNamespace::User,
                    item: SimplifiedItem::MaterializedView {
                        depends_on: vec!["s2".to_string()],
                    },
                },
                SimplifiedCatalogEntry {
                    name: "u2".to_string(),
                    namespace: ItemNamespace::User,
                    item: SimplifiedItem::MaterializedView {
                        depends_on: vec!["s1".to_string(), "u1".to_string()],
                    },
                },
            ],
            migrated_names: vec!["s1".to_string(), "s2".to_string()],
            expected_previous_sink_names: vec![],
            expected_previous_materialized_view_names: vec!["u2".to_string(), "u1".to_string()],
            expected_previous_source_names: vec!["s2".to_string(), "s1".to_string()],
            expected_all_drop_ops: vec![
                "u1".to_string(),
                "u2".to_string(),
                "s2".to_string(),
                "s1".to_string(),
            ],
            expected_user_drop_ops: vec!["u1".to_string(), "u2".to_string()],
            expected_all_create_ops: vec![
                "s1".to_string(),
                "s2".to_string(),
                "u2".to_string(),
                "u1".to_string(),
            ],
            expected_user_create_ops: vec!["u2".to_string(), "u1".to_string()],
        },
    ];

    for test_case in test_cases {
        let mut catalog = Catalog::open_debug_sqlite(NOW_ZERO.clone()).await?;

        let mut id_mapping = HashMap::new();
        for entry in test_case.initial_state {
            let (name, namespace, item) = entry.to_catalog_item(&id_mapping);
            let id = add_item(&mut catalog, name.clone(), item, namespace).await;
            id_mapping.insert(name, id);
        }

        let migrated_ids = test_case
            .migrated_names
            .into_iter()
            // We don't use the new fingerprint in this test, so we can just hard code it
            .map(|name| (id_mapping[&name], 666))
            .collect();
        let migration_metadata = catalog
            .generate_builtin_migration_metadata(migrated_ids)
            .await?;

        assert_eq!(
            migration_metadata.previous_sink_ids,
            convert_name_vec_to_id_vec(test_case.expected_previous_sink_names, &id_mapping),
            "{} test failed with wrong previous sink ids",
            test_case.test_name
        );
        assert_eq!(
            migration_metadata.previous_materialized_view_ids,
            convert_name_vec_to_id_vec(
                test_case.expected_previous_materialized_view_names,
                &id_mapping
            ),
            "{} test failed with wrong previous materialized view ids",
            test_case.test_name
        );
        assert_eq!(
            migration_metadata.previous_source_ids,
            convert_name_vec_to_id_vec(test_case.expected_previous_source_names, &id_mapping),
            "{} test failed with wrong previous source ids",
            test_case.test_name
        );
        assert_eq!(
            migration_metadata.all_drop_ops,
            convert_name_vec_to_id_vec(test_case.expected_all_drop_ops, &id_mapping),
            "{} test failed with wrong all drop ops",
            test_case.test_name
        );
        assert_eq!(
            migration_metadata.user_drop_ops,
            convert_name_vec_to_id_vec(test_case.expected_user_drop_ops, &id_mapping),
            "{} test failed with wrong user drop ops",
            test_case.test_name
        );
        assert_eq!(
            migration_metadata
                .all_create_ops
                .into_iter()
                .map(|(_, _, name, _)| name.item)
                .collect::<Vec<_>>(),
            test_case.expected_all_create_ops,
            "{} test failed with wrong all create ops",
            test_case.test_name
        );
        assert_eq!(
            migration_metadata
                .user_create_ops
                .into_iter()
                .map(|(_, _, name)| name)
                .collect::<Vec<_>>(),
            test_case.expected_user_create_ops,
            "{} test failed with wrong user create ops",
            test_case.test_name
        );
    }

    Ok(())
}

async fn add_item(
    catalog: &mut Catalog<Sqlite>,
    name: String,
    item: CatalogItem,
    item_namespace: ItemNamespace,
) -> GlobalId {
    let id = match item_namespace {
        ItemNamespace::User => catalog.allocate_user_id().await.unwrap(),
        ItemNamespace::System => catalog
            .test_only_dont_use_in_production_allocate_system_id()
            .await
            .unwrap(),
    };
    let oid = catalog.allocate_oid().unwrap();
    let database_id = catalog
        .resolve_database(DEFAULT_DATABASE_NAME)
        .unwrap()
        .id();
    let database_spec = ResolvedDatabaseSpecifier::Id(database_id);
    let schema_spec = catalog
        .resolve_schema_in_database(&database_spec, DEFAULT_SCHEMA, SYSTEM_CONN_ID)
        .unwrap()
        .id
        .clone();
    catalog
        .transact(
            None,
            vec![Op::CreateItem {
                id,
                oid,
                name: QualifiedObjectName {
                    qualifiers: ObjectQualifiers {
                        database_spec,
                        schema_spec,
                    },
                    item: name,
                },
                item,
            }],
            |_| Ok(()),
        )
        .await
        .unwrap();
    id
}

fn convert_name_vec_to_id_vec(
    name_vec: Vec<String>,
    id_lookup: &HashMap<String, GlobalId>,
) -> Vec<GlobalId> {
    name_vec.into_iter().map(|name| id_lookup[&name]).collect()
}

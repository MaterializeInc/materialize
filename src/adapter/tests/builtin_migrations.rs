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

use mz_adapter::catalog::{Catalog, CatalogItem, Op, Table, SYSTEM_CONN_ID};
use mz_adapter::session::DEFAULT_DATABASE_NAME;
use mz_ore::now::NOW_ZERO;
use mz_repr::{GlobalId, RelationDesc, ScalarType};
use mz_sql::ast::Expr;
use mz_sql::catalog::{CatalogDatabase, SessionCatalog};
use mz_sql::names::{
    ObjectQualifiers, PartialObjectName, QualifiedObjectName, ResolvedDatabaseSpecifier,
};

use mz_sql::DEFAULT_SCHEMA;
use mz_stash::Sqlite;
use std::collections::HashMap;
use std::error::Error;

#[tokio::test]
async fn test_builtin_migration() -> Result<(), Box<dyn Error>> {
    struct BuiltinMigrationTestCase {
        test_name: &'static str,
        initial_state: Vec<(String, CatalogItem)>,
        migrated_names: Vec<String>,
        // TODO(jkosh44) This is all wrong, we need to use names :'(
        expected_previous_sink_ids: Vec<GlobalId>,
        expected_previous_materialized_view_ids: Vec<GlobalId>,
        expected_previous_source_ids: Vec<GlobalId>,
        expected_all_drop_ops: Vec<GlobalId>,
        expected_all_create_ops: Vec<GlobalId>,
        expected_user_drop_ops: Vec<GlobalId>,
        expected_user_create_ops: Vec<GlobalId>,
    }

    let test_cases = vec![BuiltinMigrationTestCase {
        test_name: "no_migrations",
        initial_state: vec![(
            "s1".to_string(),
            CatalogItem::Table(Table {
                create_sql: "TODO".to_string(),
                desc: RelationDesc::empty()
                    .with_column("a", ScalarType::Int32.nullable(true))
                    .with_key(vec![0]),
                defaults: vec![Expr::null(); 1],
                conn_id: None,
                depends_on: vec![],
            }),
        )],
        migrated_names: vec![],
        expected_previous_sink_ids: vec![],
        expected_previous_materialized_view_ids: vec![],
        expected_previous_source_ids: vec![],
        expected_all_drop_ops: vec![],
        expected_all_create_ops: vec![],
        expected_user_drop_ops: vec![],
        expected_user_create_ops: vec![],
    }];

    for test_case in test_cases {
        let mut catalog = Catalog::open_debug_sqlite(NOW_ZERO.clone()).await?;

        let initial_state: HashMap<_, _> = test_case.initial_state.into_iter().collect();

        for (name, item) in initial_state {
            add_item(&mut catalog, name, item).await;
        }

        let conn_catalog = catalog.for_system_session();
        let migrated_ids = test_case
            .migrated_names
            .into_iter()
            .map(|name| {
                let entry = conn_catalog
                    .resolve_item(&PartialObjectName {
                        database: None,
                        schema: None,
                        item: name,
                    })
                    .unwrap();
                (entry.id(), 666)
            })
            .collect();
        let migration_metadata = catalog
            .generate_builtin_migration_metadata(migrated_ids)
            .await?;

        assert_eq!(
            migration_metadata.previous_sink_ids, test_case.expected_previous_sink_ids,
            "{} test failed with wrong previous sink ids",
            test_case.test_name
        );
        assert_eq!(
            migration_metadata.previous_materialized_view_ids,
            test_case.expected_previous_materialized_view_ids,
            "{} test failed with wrong previous materialized view ids",
            test_case.test_name
        );
        assert_eq!(
            migration_metadata.previous_source_ids, test_case.expected_previous_source_ids,
            "{} test failed with wrong previous source ids",
            test_case.test_name
        );
        assert_eq!(
            migration_metadata.all_drop_ops, test_case.expected_all_drop_ops,
            "{} test failed with wrong all drop ops",
            test_case.test_name
        );
        assert_eq!(
            migration_metadata
                .all_create_ops
                .into_iter()
                .map(|(id, _, _, _)| id)
                .collect::<Vec<_>>(),
            test_case.expected_all_create_ops,
            "{} test failed with wrong all create ops",
            test_case.test_name
        );
        assert_eq!(
            migration_metadata.user_drop_ops, test_case.expected_user_drop_ops,
            "{} test failed with wrong user drop ops",
            test_case.test_name
        );
        assert_eq!(
            migration_metadata
                .user_create_ops
                .into_iter()
                .map(|(id, _, _)| id)
                .collect::<Vec<_>>(),
            test_case.expected_user_create_ops,
            "{} test failed with wrong user create ops",
            test_case.test_name
        );
    }

    Ok(())
}

// TODO(jkosh44) Add system id version
async fn add_item(catalog: &mut Catalog<Sqlite>, name: String, item: CatalogItem) {
    let id = catalog.allocate_user_id().await.unwrap();
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
}

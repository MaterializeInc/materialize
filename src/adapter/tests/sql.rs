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

use std::collections::BTreeSet;
use std::sync::Arc;

use mz_adapter::catalog::{Catalog, Op};
use mz_adapter::session::{Session, DEFAULT_DATABASE_NAME};
use mz_catalog::memory::objects::{CatalogItem, Table};
use mz_catalog::SYSTEM_CONN_ID;
use mz_ore::now::NOW_ZERO;
use mz_repr::RelationDesc;
use mz_sql::ast::{Expr, Statement};
use mz_sql::catalog::CatalogDatabase;
use mz_sql::names::{
    self, ItemQualifiers, QualifiedItemName, ResolvedDatabaseSpecifier, ResolvedIds,
};
use mz_sql::plan::{PlanContext, QueryContext, QueryLifetime, StatementContext};
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use mz_sql::DEFAULT_SCHEMA;
use tokio::sync::Mutex;

// This morally tests the name resolution stuff, but we need access to a
// catalog.

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn datadriven() {
    datadriven::walk_async("tests/testdata/sql", |mut f| async {
        // The datadriven API takes an `FnMut` closure, and can't express to Rust that
        // it will finish polling each returned future before calling the closure
        // again, so we have to wrap the catalog in a share-able type. Datadriven
        // could be changed to maybe take some context that it passes as a &mut to each
        // test_case invocation, act on a stream of test_cases, or take and return a
        // Context. This is just a test, so the performance hit of this doesn't matter
        // (and in practice there will be no contention).

        Catalog::with_debug(NOW_ZERO.clone(), |catalog| async move {
            let catalog = Arc::new(Mutex::new(catalog));
            f.run_async(|test_case| {
                let catalog = Arc::clone(&catalog);
                async move {
                    let mut catalog = catalog.lock().await;
                    match test_case.directive.as_str() {
                        "add-table" => {
                            let id = catalog.allocate_user_id().await.unwrap();
                            let database_id = catalog
                                .resolve_database(DEFAULT_DATABASE_NAME)
                                .unwrap()
                                .id();
                            let database_spec = ResolvedDatabaseSpecifier::Id(database_id);
                            let schema_spec = catalog
                                .resolve_schema_in_database(
                                    &database_spec,
                                    DEFAULT_SCHEMA,
                                    &SYSTEM_CONN_ID,
                                )
                                .unwrap()
                                .id
                                .clone();
                            catalog
                                .transact(
                                    None,
                                    mz_repr::Timestamp::MIN,
                                    None,
                                    vec![Op::CreateItem {
                                        id,
                                        name: QualifiedItemName {
                                            qualifiers: ItemQualifiers {
                                                database_spec,
                                                schema_spec,
                                            },
                                            item: test_case.input.trim_end().to_string(),
                                        },
                                        item: CatalogItem::Table(Table {
                                            create_sql: Some(format!(
                                                "CREATE TABLE {} ()",
                                                test_case.input.trim_end()
                                            )),
                                            desc: RelationDesc::empty(),
                                            defaults: vec![Expr::null(); 0],
                                            conn_id: None,
                                            resolved_ids: ResolvedIds(BTreeSet::new()),
                                            custom_logical_compaction_window: None,
                                            is_retained_metrics_object: false,
                                        }),
                                        owner_id: MZ_SYSTEM_ROLE_ID,
                                    }],
                                )
                                .await
                                .unwrap();
                            format!("{}\n", id)
                        }
                        "resolve" => {
                            let sess = Session::dummy();
                            let catalog = catalog.for_session(&sess);

                            let parsed = mz_sql::parse::parse(&test_case.input).unwrap();
                            let pcx = &PlanContext::zero();
                            let scx = StatementContext::new(Some(pcx), &catalog);
                            let qcx = QueryContext::root(&scx, QueryLifetime::OneShot);
                            let q = parsed[0].ast.clone();
                            let q = match q {
                                Statement::Select(s) => s.query,
                                _ => unreachable!(),
                            };
                            let resolved = names::resolve(qcx.scx.catalog, q);
                            match resolved {
                                Ok((q, _depends_on)) => format!("{}\n", q),
                                Err(e) => format!("error: {}\n", e),
                            }
                        }
                        dir => panic!("unhandled directive {}", dir),
                    }
                }
            })
            .await;
            if let Some(catalog) = Arc::into_inner(catalog) {
                let catalog = catalog.into_inner();
                catalog.expire().await;
            }
            f
        })
        .await
    })
    .await;
}

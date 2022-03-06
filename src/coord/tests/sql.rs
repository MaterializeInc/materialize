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

use tempfile::NamedTempFile;

use mz_coord::catalog::{Catalog, CatalogItem, Op, Table};
use mz_coord::session::Session;
use mz_ore::now::NOW_ZERO;
use mz_repr::RelationDesc;
use mz_sql::ast::{Expr, Statement};
use mz_sql::names::{resolve_names, DatabaseSpecifier, FullName};
use mz_sql::plan::{PlanContext, QueryContext, QueryLifetime, StatementContext};

// This morally tests the name resolution stuff, but we need access to a
// catalog.

#[tokio::test]
async fn datadriven() {
    datadriven::walk_async("tests/testdata", |mut f| async {
        let catalog_file = NamedTempFile::new().unwrap();
        let mut catalog = Catalog::open_debug(catalog_file.path(), NOW_ZERO.clone())
            .await
            .unwrap();
        f.run(|test_case| -> String {
            match test_case.directive.as_str() {
                "add-table" => {
                    let id = catalog.allocate_user_id().unwrap();
                    let oid = catalog.allocate_oid().unwrap();
                    catalog
                        .transact(
                            vec![Op::CreateItem {
                                id,
                                oid,
                                name: FullName {
                                    database: DatabaseSpecifier::Name("materialize".into()),
                                    schema: "public".into(),
                                    item: test_case.input.trim_end().to_string(),
                                },
                                item: CatalogItem::Table(Table {
                                    create_sql: "TODO".to_string(),
                                    desc: RelationDesc::empty(),
                                    defaults: vec![Expr::null(); 0],
                                    conn_id: None,
                                    depends_on: vec![],
                                    persist_name: None,
                                }),
                            }],
                            |_| Ok(()),
                        )
                        .unwrap();
                    format!("{}\n", id)
                }
                "resolve" => {
                    let sess = Session::dummy();
                    let catalog = catalog.for_session(&sess);

                    let parsed = mz_sql::parse::parse(&test_case.input).unwrap();
                    let pcx = &PlanContext::zero();
                    let scx = StatementContext::new(Some(pcx), &catalog);
                    let mut qcx =
                        QueryContext::root(&scx, QueryLifetime::OneShot(scx.pcx().unwrap()));
                    let q = parsed[0].clone();
                    let q = match q {
                        Statement::Select(s) => s.query,
                        _ => unreachable!(),
                    };
                    let resolved = resolve_names(&mut qcx, q);
                    match resolved {
                        Ok(q) => format!("{}\n", q),
                        Err(e) => format!("error: {}\n", e),
                    }
                }
                dir => panic!("unhandled directive {}", dir),
            }
        });
        f
    })
    .await;
}

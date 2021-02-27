// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. All rights reserved.
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

use std::{
    cell::RefCell,
    collections::{BTreeMap, HashSet},
    rc::Rc,
};

use expr::GlobalId;
use repr::{RelationDesc, RelationType};
use tempfile::NamedTempFile;

use coord::{
    catalog::{Catalog, CatalogItem, Table},
    session::Session,
};
use sql::{
    ast::{Expr, Statement},
    names::{DatabaseSpecifier, FullName},
    plan::{resolve_names, PlanContext, QueryContext, QueryLifetime, StatementContext},
};
use sql_parser::parser::parse_statements;

// This morally tests the name resolution stuff, but we need access to a
// catalog.

#[test]
fn datadriven() {
    use datadriven::walk;

    walk("tests/testdata", |f| {
        let catalog_file = NamedTempFile::new().unwrap();
        let mut catalog = Catalog::open_debug(catalog_file.path()).unwrap();
        let mut id: u32 = 1;
        f.run(|test_case| -> String {
            match test_case.directive.as_str() {
                "add-table" => {
                    let _ = catalog.insert_item(
                        GlobalId::User(id.into()),
                        id,
                        FullName {
                            database: DatabaseSpecifier::Name("materialize".into()),
                            schema: "public".into(),
                            item: test_case.input.trim_end().to_string(),
                        },
                        CatalogItem::Table(Table {
                            create_sql: "TODO".to_string(),
                            plan_cx: PlanContext::default(),
                            desc: RelationDesc::new(
                                RelationType::new(Vec::new()),
                                Vec::<Option<String>>::new(),
                            ),
                            defaults: vec![Expr::null(); 0],
                            conn_id: None,
                            depends_on: vec![],
                        }),
                    );
                    id += 1;
                    format!("{}\n", GlobalId::User((id - 1).into()))
                }
                "resolve" => {
                    // let catalog = catalog.for_system_session();
                    let sess = Session::dummy();
                    let catalog = catalog.for_session(&sess);

                    let parsed = parse_statements(&test_case.input).unwrap();
                    let scx = StatementContext {
                        catalog: &catalog,
                        pcx: &PlanContext::default(),
                        ids: HashSet::new(),
                        param_types: Rc::new(RefCell::new(BTreeMap::new())),
                    };
                    let mut qcx = QueryContext::root(&scx, QueryLifetime::OneShot);
                    let q = parsed[0].clone();
                    let q = match q {
                        Statement::Select(s) => s.query,
                        _ => unreachable!(),
                    };
                    let resolved = resolve_names(&mut qcx, q);
                    match resolved {
                        Ok(q) => format!("{}\n", q),
                        Err(e) => format!("error: {:?}\n", e),
                    }
                }
                dir => panic!("unhandled directive {}", dir),
            }
        })
    });
}

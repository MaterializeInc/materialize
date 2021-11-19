// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::catalog::{
    CatalogConfig, CatalogDatabase, CatalogError, CatalogItem, CatalogRole, CatalogSchema,
    SessionCatalog,
};
use crate::names::{FullName, PartialName};
use crate::plan::query::QueryLifetime;
use crate::plan::{StatementContext, StatementDesc};
use build_info::DUMMY_BUILD_INFO;
use chrono::MIN_DATETIME;
use expr::{DummyHumanizer, ExprHumanizer, GlobalId};
use lazy_static::lazy_static;
use ore::now::{EpochMillis, NOW_ZERO};
use repr::ScalarType;
use std::time::{Duration, Instant};
use uuid::Uuid;

use crate::query_model;

lazy_static! {
    static ref DUMMY_CONFIG: CatalogConfig = CatalogConfig {
        start_time: MIN_DATETIME,
        start_instant: Instant::now(),
        nonce: 0,
        cluster_id: Uuid::from_u128(0),
        session_id: Uuid::from_u128(0),
        experimental_mode: false,
        safe_mode: false,
        build_info: &DUMMY_BUILD_INFO,
        num_workers: 0,
        timestamp_frequency: Duration::from_secs(1),
        now: NOW_ZERO.clone(),
        disable_user_indexes: false,
    };
}

/// A dummy [`SessionCatalog`] implementation.
///
/// This implementation is suitable for use in tests that plan queries which are
/// not demanding of the catalog, as many methods are unimplemented.
#[derive(Debug)]
pub struct TestCatalog;

impl SessionCatalog for TestCatalog {
    fn search_path(&self, _: bool) -> Vec<&str> {
        vec!["dummy"]
    }

    fn user(&self) -> &str {
        "dummy"
    }

    fn get_prepared_statement_desc(&self, _: &str) -> Option<&StatementDesc> {
        None
    }

    fn default_database(&self) -> &str {
        "dummy"
    }

    fn resolve_database(&self, _: &str) -> Result<&dyn CatalogDatabase, CatalogError> {
        unimplemented!();
    }

    fn resolve_schema(
        &self,
        _: Option<String>,
        _: &str,
    ) -> Result<&dyn CatalogSchema, CatalogError> {
        unimplemented!();
    }

    fn resolve_role(&self, _: &str) -> Result<&dyn CatalogRole, CatalogError> {
        unimplemented!();
    }

    fn resolve_item(&self, _: &PartialName) -> Result<&dyn CatalogItem, CatalogError> {
        unimplemented!();
    }

    fn resolve_function(&self, _: &PartialName) -> Result<&dyn CatalogItem, CatalogError> {
        unimplemented!();
    }

    fn get_item_by_id(&self, _: &GlobalId) -> &dyn CatalogItem {
        unimplemented!();
    }

    fn try_get_item_by_id(&self, _: &GlobalId) -> Option<&dyn CatalogItem> {
        unimplemented!();
    }

    fn get_item_by_oid(&self, _: &u32) -> &dyn CatalogItem {
        unimplemented!();
    }

    fn item_exists(&self, _: &FullName) -> bool {
        false
    }

    fn try_get_lossy_scalar_type_by_id(&self, _: &GlobalId) -> Option<ScalarType> {
        None
    }

    fn config(&self) -> &CatalogConfig {
        &DUMMY_CONFIG
    }

    fn now(&self) -> EpochMillis {
        (self.config().now)()
    }
}

impl ExprHumanizer for TestCatalog {
    fn humanize_id(&self, id: GlobalId) -> Option<String> {
        DummyHumanizer.humanize_id(id)
    }

    fn humanize_scalar_type(&self, ty: &ScalarType) -> String {
        DummyHumanizer.humanize_scalar_type(ty)
    }
}

#[test]
fn test_hir_generator() {
    datadriven::walk("tests/querymodel", |f| {
        let catalog = TestCatalog {};

        f.run(move |s| -> String {
            let build_stmt = |stmt, dot_graph, lower| -> String {
                let scx = &StatementContext::new(None, &catalog);
                if let sql_parser::ast::Statement::Select(query) = stmt {
                    let planned_query = match crate::plan::query::plan_root_query(
                        scx,
                        query.query,
                        QueryLifetime::Static,
                    ) {
                        Ok(planned_query) => planned_query,
                        Err(e) => return format!("unable to plan query: {}: {}", s.input, e),
                    };

                    let model = query_model::Model::from(&planned_query.expr);

                    let mut output = String::new();

                    if dot_graph {
                        output += &match query_model::dot::DotGenerator::new()
                            .generate(&model, &s.input)
                        {
                            Ok(graph) => graph,
                            Err(e) => return format!("graph generation error: {}", e),
                        };
                    }

                    if lower {
                        output += &model.lower().pretty_humanized(&catalog);
                    }

                    output
                } else {
                    format!("invalid query: {}", s.input)
                }
            };

            let execute_command = |dot_graph, lower| -> String {
                let stmts = match sql_parser::parser::parse_statements(&s.input) {
                    Ok(stmts) => stmts,
                    Err(e) => return format!("unable to parse SQL: {}: {}", s.input, e),
                };

                stmts.into_iter().fold("".to_string(), |c, stmt| {
                    c + &build_stmt(stmt, dot_graph, lower)
                })
            };

            match s.directive.as_str() {
                "build" => execute_command(true, false),
                "lower" => execute_command(false, true),
                _ => panic!("unknown directive: {}", s.directive),
            }
        })
    });
}

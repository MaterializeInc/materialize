// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub(crate) mod catalog;
pub(crate) mod util;

use crate::plan::query::QueryLifetime;
use crate::plan::StatementContext;
use expr_test_util::generate_explanation;

use crate::query_model;
use catalog::TestCatalog;

#[test]
fn test_hir_generator() {
    datadriven::walk("tests/querymodel", |f| {
        let mut catalog = TestCatalog::default();

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

                    let model = query_model::Model::from(planned_query.expr);

                    let mut output = String::new();

                    if dot_graph {
                        output += &match model.as_dot(&s.input) {
                            Ok(graph) => graph,
                            Err(e) => return format!("graph generation error: {}", e),
                        };
                    }

                    if lower {
                        output +=
                            &generate_explanation(&catalog, &model.into(), s.args.get("format"));
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
                "cat" => match catalog.execute_commands(&s.input) {
                    Ok(ok) => ok,
                    Err(err) => err,
                },
                _ => panic!("unknown directive: {}", s.directive),
            }
        })
    });
}

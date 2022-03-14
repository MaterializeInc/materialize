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
use mz_expr_test_util::generate_explanation;
use mz_lowertest::*;

use crate::query_model::Model;
use catalog::TestCatalog;

use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Tests to run on a Query Graph Model.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, MzReflect)]
enum Directive {
    // TODO: support build apply=(<stuff>)
    /// Apply any number of specific changes to the model.
    Build,
    /// Decorrelate the model and convert it to a `MirRelationExpr`.
    Lower,
    /// Optimize the model.
    Opt,
    /// Optimize and decorrelate the model. Then convert it to a `MirRelationExpr`.
    EndToEnd,
}

lazy_static! {
    pub static ref RTI: ReflectedTypeInfo = {
        let mut rti = ReflectedTypeInfo::default();
        Directive::add_to_reflected_type_info(&mut rti);
        rti
    };
}

/// Convert the input string to a Query Graph Model.
fn convert_input_to_model(input: &str, catalog: &TestCatalog) -> Result<Model, String> {
    // TODO (#9347): Support parsing specs for HirRelationExpr.
    // TODO (#10518): Support parsing specs for QGM.
    // match parse_input_as_qgm(input) {
    //   Ok(model) => Ok(model),
    //   Err(err) => {
    //      let hir = match parse_input_as_hir(input) {
    //         Ok(hir) => hir,
    //         Err(err2) =>
    //      }
    //      Model::from(hir)
    //   }
    // }
    match mz_sql_parser::parser::parse_statements(input) {
        Ok(mut stmts) => {
            assert!(stmts.len() == 1);
            let stmt = stmts.pop().unwrap();
            let scx = &StatementContext::new(None, catalog);
            if let mz_sql_parser::ast::Statement::Select(query) = stmt {
                let planned_query = match crate::plan::query::plan_root_query(
                    scx,
                    query.query,
                    QueryLifetime::Static,
                ) {
                    Ok(planned_query) => planned_query,
                    Err(e) => return Err(format!("unable to plan query: {}: {}", input, e)),
                };
                Model::try_from(planned_query.expr).map_err(|e| e.into())
            } else {
                Err(format!("invalid query: {}", input))
            }
        }
        Err(e) => {
            // TODO: try to parse the input as a spec for an HIR.
            // If that fails, try to parse the input as a spec for a QGM.
            // Change this error message.
            Err(format!("unable to parse SQL: {}: {}", input, e))
        }
    }
}

fn run_command(
    command: &str,
    input: &str,
    args: &HashMap<String, Vec<String>>,
    catalog: &TestCatalog,
) -> Result<String, String> {
    let mut model = convert_input_to_model(input, catalog)?;
    let directive: Directive = deserialize(
        &mut tokenize(command)?.into_iter(),
        "Directive",
        &RTI,
        &mut GenericTestDeserializeContext::default(),
    )?;

    if matches!(directive, Directive::Opt | Directive::EndToEnd) {
        model.optimize();
    }

    // TODO: allow printing multiple stages of the transformation of the query.
    if matches!(directive, Directive::Lower | Directive::EndToEnd) {
        match model.try_into() {
            Ok(mir) => Ok(generate_explanation(catalog, &mir, args.get("format"))),
            Err(err) => Err(err.to_string()),
        }
    } else {
        match model.as_dot(input, catalog, false) {
            Ok(graph) => Ok(graph),
            Err(e) => return Err(format!("graph generation error: {}", e)),
        }
    }
}

#[test]
fn test_qgm() {
    datadriven::walk("tests/querymodel", |f| {
        let mut catalog = TestCatalog::default();

        f.run(move |s| -> String {
            match s.directive.as_str() {
                "cat" => match catalog.execute_commands(&s.input) {
                    Ok(ok) => ok,
                    Err(err) => err,
                },
                other => match run_command(other, &s.input, &s.args, &catalog) {
                    Ok(ok) => ok,
                    Err(err) => err,
                },
            }
        })
    });
}

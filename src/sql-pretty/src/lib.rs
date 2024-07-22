// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod doc;
mod util;

use mz_sql_parser::ast::*;
use mz_sql_parser::parser::{parse_statements, ParserStatementError};
use pretty::RcDoc;
use thiserror::Error;

use crate::doc::{
    doc_copy, doc_create_materialized_view, doc_create_source, doc_create_view, doc_display,
    doc_insert, doc_select_statement, doc_subscribe,
};

pub use crate::doc::doc_expr;

const TAB: isize = 4;

fn to_doc<T: AstInfo>(v: &Statement<T>) -> RcDoc {
    match v {
        Statement::Select(v) => doc_select_statement(v),
        Statement::Insert(v) => doc_insert(v),
        Statement::CreateView(v) => doc_create_view(v),
        Statement::CreateMaterializedView(v) => doc_create_materialized_view(v),
        Statement::Copy(v) => doc_copy(v),
        Statement::Subscribe(v) => doc_subscribe(v),
        Statement::CreateSource(v) => doc_create_source(v),
        _ => doc_display(v, "statement"),
    }
}

/// Pretty prints a statement at a width.
pub fn to_pretty<T: AstInfo>(stmt: &Statement<T>, width: usize) -> String {
    format!("{};", to_doc(stmt).pretty(width))
}

/// Parses `str` into SQL statements and pretty prints them.
pub fn pretty_strs(str: &str, width: usize) -> Result<Vec<String>, Error> {
    let stmts = parse_statements(str)?;
    Ok(stmts.iter().map(|s| to_pretty(&s.ast, width)).collect())
}

/// Parses `str` into a single SQL statement and pretty prints it.
pub fn pretty_str(str: &str, width: usize) -> Result<String, Error> {
    let stmts = parse_statements(str)?;
    if stmts.len() != 1 {
        return Err(Error::ExpectedOne);
    }
    Ok(to_pretty(&stmts[0].ast, width))
}

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Parser(#[from] ParserStatementError),
    #[error("expected exactly one statement")]
    ExpectedOne,
}

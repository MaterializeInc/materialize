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

use mz_sql_parser::ast::display::FormatMode;
use mz_sql_parser::ast::*;
use mz_sql_parser::parser::{parse_statements, ParserStatementError};
use pretty::RcDoc;
use thiserror::Error;

use crate::doc::{
    doc_copy, doc_create_materialized_view, doc_create_source, doc_create_view, doc_display,
    doc_insert, doc_select_statement, doc_subscribe,
};

pub use crate::doc::doc_expr;

pub const DEFAULT_WIDTH: usize = 100;

const TAB: isize = 4;

fn to_doc<T: AstInfo>(v: &Statement<T>, config: PrettyConfig) -> RcDoc {
    match v {
        Statement::Select(v) => doc_select_statement(v, config),
        Statement::Insert(v) => doc_insert(v, config),
        Statement::CreateView(v) => doc_create_view(v, config),
        Statement::CreateMaterializedView(v) => doc_create_materialized_view(v, config),
        Statement::Copy(v) => doc_copy(v, config),
        Statement::Subscribe(v) => doc_subscribe(v, config),
        Statement::CreateSource(v) => doc_create_source(v, config),
        _ => doc_display(v, config, "statement"),
    }
}

#[derive(Clone, Copy)]
pub struct PrettyConfig {
    pub width: usize,
    pub format_mode: FormatMode,
}

/// Pretty prints a statement at a width.
pub fn to_pretty<T: AstInfo>(stmt: &Statement<T>, config: PrettyConfig) -> String {
    format!("{};", to_doc(stmt, config).pretty(config.width))
}

/// Parses `str` into SQL statements and pretty prints them.
pub fn pretty_strs(str: &str, config: PrettyConfig) -> Result<Vec<String>, Error> {
    let stmts = parse_statements(str)?;
    Ok(stmts.iter().map(|s| to_pretty(&s.ast, config)).collect())
}

/// Parses `str` into a single SQL statement and pretty prints it.
pub fn pretty_str(str: &str, config: PrettyConfig) -> Result<String, Error> {
    let stmts = parse_statements(str)?;
    if stmts.len() != 1 {
        return Err(Error::ExpectedOne);
    }
    Ok(to_pretty(&stmts[0].ast, config))
}

/// Parses `str` into SQL statements and pretty prints them in `Simple` mode.
pub fn pretty_strs_simple(str: &str, width: usize) -> Result<Vec<String>, Error> {
    pretty_strs(
        str,
        PrettyConfig {
            width,
            format_mode: FormatMode::Simple,
        },
    )
}

/// Parses `str` into a single SQL statement and pretty prints it in `Simple` mode.
pub fn pretty_str_simple(str: &str, width: usize) -> Result<String, Error> {
    pretty_str(
        str,
        PrettyConfig {
            width,
            format_mode: FormatMode::Simple,
        },
    )
}

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Parser(#[from] ParserStatementError),
    #[error("expected exactly one statement")]
    ExpectedOne,
}

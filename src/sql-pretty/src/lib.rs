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
use mz_sql_parser::parser::{ParserStatementError, parse_statements};
use pretty::RcDoc;
use thiserror::Error;

pub const DEFAULT_WIDTH: usize = 100;

const TAB: isize = 4;

#[derive(Clone, Copy)]
pub struct PrettyConfig {
    pub width: usize,
    pub format_mode: FormatMode,
}

/// Pretty prints a statement at a width.
pub fn to_pretty<T: AstInfo>(stmt: &Statement<T>, config: PrettyConfig) -> String {
    format!("{};", Pretty { config }.to_doc(stmt).pretty(config.width))
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

/// (Public only for tests)
pub struct Pretty {
    pub config: PrettyConfig,
}

impl Pretty {
    fn to_doc<'a, T: AstInfo>(&'a self, v: &'a Statement<T>) -> RcDoc<'a> {
        match v {
            Statement::Select(v) => self.doc_select_statement(v),
            Statement::Insert(v) => self.doc_insert(v),
            Statement::CreateView(v) => self.doc_create_view(v),
            Statement::CreateMaterializedView(v) => self.doc_create_materialized_view(v),
            Statement::Copy(v) => self.doc_copy(v),
            Statement::Subscribe(v) => self.doc_subscribe(v),
            Statement::CreateSource(v) => self.doc_create_source(v),
            Statement::CreateWebhookSource(v) => self.doc_create_webhook_source(v),
            _ => self.doc_display(v, "statement"),
        }
    }
}

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
            Statement::CreateCluster(v) => self.doc_create_cluster(v),
            Statement::CreateType(v) => self.doc_create_type(v),
            Statement::Copy(v) => self.doc_copy(v),
            Statement::Subscribe(v) => self.doc_subscribe(v),
            Statement::CreateSource(v) => self.doc_create_source(v),
            _ => self.doc_display(v, "statement"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn test_create_cluster() {
        let input = "CREATE CLUSTER foo (SIZE = '1')";
        let expected = "CREATE CLUSTER foo (SIZE = '1');";
        let result = pretty_str_simple(input, 100).unwrap();
        assert_eq!(result, expected);
    }

    #[mz_ore::test]
    fn test_create_cluster_multiple_options() {
        let input = "CREATE CLUSTER foo (SIZE = '1', REPLICATION FACTOR = 2)";
        let expected = "CREATE CLUSTER foo (SIZE = '1', REPLICATION FACTOR = 2);";
        let result = pretty_str_simple(input, 100).unwrap();
        assert_eq!(result, expected);
    }

    #[mz_ore::test]
    fn test_create_cluster_with_features() {
        let input = "CREATE CLUSTER foo (SIZE = '1') FEATURES (ENABLE EAGER DELTA JOINS = true)";
        let expected =
            "CREATE CLUSTER foo (SIZE = '1') FEATURES (ENABLE EAGER DELTA JOINS = true);";
        let result = pretty_str_simple(input, 100).unwrap();
        assert_eq!(result, expected);
    }

    #[mz_ore::test]
    fn test_create_cluster_wrapping() {
        let input = "CREATE CLUSTER foo (SIZE = '1', REPLICATION FACTOR = 2)";
        let expected = "CREATE CLUSTER foo\n(\n    SIZE = '1',\n    REPLICATION FACTOR = 2\n);";
        let result = pretty_str_simple(input, 30).unwrap();
        assert_eq!(result, expected);
    }

    #[mz_ore::test]
    fn test_create_type_list() {
        let input = "CREATE TYPE custom AS LIST (ELEMENT TYPE = text)";
        let expected = "CREATE TYPE custom AS LIST (ELEMENT TYPE = text);";
        let result = pretty_str_simple(input, 100).unwrap();
        assert_eq!(result, expected);
    }

    #[mz_ore::test]
    fn test_create_type_map() {
        let input = "CREATE TYPE custom AS MAP (KEY TYPE = text, VALUE TYPE = bool)";
        let expected = "CREATE TYPE custom AS MAP (KEY TYPE = text, VALUE TYPE = bool);";
        let result = pretty_str_simple(input, 100).unwrap();
        assert_eq!(result, expected);
    }

    #[mz_ore::test]
    fn test_create_type_record() {
        let input = "CREATE TYPE custom AS (a int, b text)";
        let expected = "CREATE TYPE custom AS (a int4, b text);";
        let result = pretty_str_simple(input, 100).unwrap();
        assert_eq!(result, expected);
    }

    #[mz_ore::test]
    fn test_create_type_map_wrapping() {
        let input = "CREATE TYPE custom AS MAP (KEY TYPE = text, VALUE TYPE = bool)";
        let expected =
            "CREATE TYPE custom\nAS MAP (\n    KEY TYPE = text,\n    VALUE TYPE = bool\n);";
        let result = pretty_str_simple(input, 30).unwrap();
        assert_eq!(result, expected);
    }
}

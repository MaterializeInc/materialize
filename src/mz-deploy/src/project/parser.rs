//! SQL parsing with `mz_sql_parser`.
//!
//! Wraps `mz_sql_parser` to parse `.sql` files into AST statements, attaching
//! file-path context to error messages so that parse failures point back to
//! the originating source file.
//!
//! ## Variable Resolution and Parsing
//!
//! [`parse_statements_with_context`] runs variable resolution *before* parsing:
//! 1. Resolve psql-style variables (`:foo`, `:'foo'`, `:"foo"`) via
//!    [`super::variables::resolve_variables`]
//! 2. Check for unresolved variables — error or warning based on pragma
//! 3. Parse the resolved SQL via `mz_sql_parser`
//! 4. Wrap any parse errors with file path and SQL content for context

use super::error::ParseError;
use super::variables::VariableError;
use crate::info;
use mz_sql_parser::ast::{Raw, Statement};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Parses one or more SQL statements from an iterable collection of strings.
///
/// This function is only used in tests for simple parsing without file context.
#[cfg(test)]
pub fn parse_statements<I, S>(raw: I) -> Result<Vec<Statement<Raw>>, ParseError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut statements = vec![];
    for s in raw {
        let parsed_results = mz_sql_parser::parser::parse_statements_with_limit(s.as_ref())
            .map_err(|e| ParseError::StatementsParseFailed {
                message: format!("Parser limit error: {}", e),
            })?
            .map_err(|e| ParseError::StatementsParseFailed {
                message: format!("Parse error: {}", e.error),
            })?;

        let mut parsed: Vec<Statement<Raw>> = parsed_results
            .into_iter()
            .map(|result| result.ast)
            .collect();

        statements.append(&mut parsed);
    }

    Ok(statements)
}

/// Parses SQL and returns [`LocatedStatement`]s with zero byte offsets.
///
/// Test-only helper for constructing [`super::raw::ObjectVariant`] values.
#[cfg(test)]
pub fn parse_statements_located<I, S>(raw: I) -> Result<Vec<LocatedStatement>, ParseError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    parse_statements(raw).map(|stmts| {
        stmts
            .into_iter()
            .map(|ast| LocatedStatement::at_zero(ast))
            .collect()
    })
}

/// A parsed SQL statement paired with its byte offset within the source file.
#[derive(Debug, Clone)]
pub struct LocatedStatement {
    /// The parsed AST node.
    pub ast: Statement<Raw>,
    /// Byte offset of the statement's start within the (resolved) SQL text.
    pub byte_offset: usize,
}

impl LocatedStatement {
    /// Wrap a bare statement with a zero byte offset.
    ///
    /// Useful in tests where the exact source position is irrelevant.
    #[cfg(test)]
    pub fn at_zero(ast: Statement<Raw>) -> Self {
        Self {
            ast,
            byte_offset: 0,
        }
    }
}

/// Parse SQL statements and add file context to any errors.
///
/// Resolves psql-style variables (`:foo`, `:'foo'`, `:"foo"`) before parsing.
/// Returns each statement together with its byte offset within the resolved SQL
/// so that downstream validation errors can point to the exact location.
pub fn parse_statements_with_context(
    sql: &str,
    path: PathBuf,
    variables: &BTreeMap<String, String>,
) -> Result<Vec<LocatedStatement>, ParseError> {
    let resolved = super::variables::resolve_variables(sql, variables);

    if !resolved.unresolved.is_empty() {
        if resolved.has_warn_pragma {
            let formatted: Vec<String> = resolved
                .unresolved
                .iter()
                .map(|v| format!(":{}", v.name))
                .collect();
            info!(
                "\x1b[33mwarning\x1b[0m: unresolved variables in {}: {}",
                path.display(),
                formatted.join(", ")
            );
        } else {
            return Err(ParseError::UnresolvedVariables(VariableError {
                unresolved: resolved.unresolved,
                path,
            }));
        }
    }

    let sql = resolved.sql;

    let mut statements = vec![];

    let parsed_results = mz_sql_parser::parser::parse_statements_with_limit(&sql)
        .map_err(|e| ParseError::StatementsParseFailed {
            message: format!("Parser limit error in file {}: {}", path.display(), e),
        })?
        .map_err(|e| ParseError::SqlParseFailed {
            path: path.clone(),
            sql: sql.to_string(),
            source: e,
        })?;

    // Compute byte offsets via pointer arithmetic on `StatementParseResult.sql`.
    //
    // `result.sql` is a `&'a str` subslice of the input we passed to
    // `parse_statements_with_limit` — the parser produces it by indexing
    // `self.sql[before..after].trim()` inside `Parser::parse_statement`.
    // Rust's lifetime parameter on `StatementParseResult<'a>` enforces this
    // at the type level: the returned slice cannot outlive the input.
    //
    // Because both pointers reference the same allocation, subtracting the
    // base pointer from the slice pointer yields a valid byte offset.
    //
    // Note: offsets are relative to the *variable-resolved* SQL text (the
    // `sql` local above), not the raw file contents. When the LSP converts
    // these to line/column positions it must build the Rope from the same
    // resolved text, or re-resolve variables before lookup.
    #[allow(clippy::as_conversions)]
    let sql_base = sql.as_ptr() as usize;
    let mut parsed: Vec<LocatedStatement> = parsed_results
        .into_iter()
        .map(|result| {
            #[allow(clippy::as_conversions)]
            let byte_offset = result.sql.as_ptr() as usize - sql_base;
            LocatedStatement {
                ast: result.ast,
                byte_offset,
            }
        })
        .collect();

    statements.append(&mut parsed);

    Ok(statements)
}

/// Get a human-readable name for a statement type.
///
/// Used by resource definition modules (clusters, roles) to produce clear
/// error messages when an unsupported statement type is encountered.
pub fn statement_type_name(stmt: &Statement<Raw>) -> &'static str {
    match stmt {
        Statement::CreateTable(_) => "CREATE TABLE",
        Statement::CreateView(_) => "CREATE VIEW",
        Statement::CreateMaterializedView(_) => "CREATE MATERIALIZED VIEW",
        Statement::CreateSource(_) => "CREATE SOURCE",
        Statement::CreateSink(_) => "CREATE SINK",
        Statement::CreateIndex(_) => "CREATE INDEX",
        Statement::CreateConstraint(_) => "CREATE CONSTRAINT",
        Statement::CreateCluster(_) => "CREATE CLUSTER",
        Statement::CreateConnection(_) => "CREATE CONNECTION",
        Statement::CreateSecret(_) => "CREATE SECRET",
        Statement::CreateSchema(_) => "CREATE SCHEMA",
        Statement::CreateDatabase(_) => "CREATE DATABASE",
        Statement::CreateRole(_) => "CREATE ROLE",
        Statement::CreateNetworkPolicy(_) => "CREATE NETWORK POLICY",
        Statement::AlterRole(_) => "ALTER ROLE",
        Statement::AlterCluster(_) => "ALTER CLUSTER",
        Statement::GrantRole(_) => "GRANT ROLE",
        Statement::GrantPrivileges(_) => "GRANT",
        Statement::Comment(_) => "COMMENT",
        _ => "unsupported statement",
    }
}

#[cfg(test)]
mod test {
    use crate::project::parser::parse_statements;

    #[test]
    fn validate() {
        let _ = parse_statements(vec!["CREATE CLUSTER c (INTROSPECTION INTERVAL = 0)"]).unwrap();
    }

    // TODO: Re-enable when mz_sql_parser supports IN CLUSTER for indexes
    // #[test]
    // fn test_index_in_cluster() {
    //     let result = parse_statements(vec!["CREATE INDEX test_idx ON test (id) IN CLUSTER quickstart"]);
    //     println!("Parse result for INDEX: {:?}", result);
    //     assert!(result.is_ok(), "Failed to parse INDEX with IN CLUSTER: {:?}", result.err());
    // }

    #[test]
    fn test_mv_in_cluster() {
        let result = parse_statements(vec![
            "CREATE MATERIALIZED VIEW mv IN CLUSTER quickstart AS SELECT 1",
        ]);
        println!("Parse result for MV: {:?}", result);
        assert!(
            result.is_ok(),
            "Failed to parse MV with IN CLUSTER: {:?}",
            result.err()
        );
    }
}

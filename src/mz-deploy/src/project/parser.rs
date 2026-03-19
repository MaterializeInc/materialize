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

/// Parse SQL statements and add file context to any errors.
///
/// Resolves psql-style variables (`:foo`, `:'foo'`, `:"foo"`) before parsing.
/// This function directly parses SQL and creates SqlParseFailed errors with full context
/// including file path and SQL content for better error reporting.
pub fn parse_statements_with_context(
    sql: &str,
    path: PathBuf,
    variables: &BTreeMap<String, String>,
) -> Result<Vec<Statement<Raw>>, ParseError> {
    let resolved = super::variables::resolve_variables(sql, variables);

    if !resolved.unresolved.is_empty() {
        if resolved.has_warn_pragma {
            let formatted: Vec<String> = resolved
                .unresolved
                .iter()
                .map(|v| format!(":{}", v))
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

    let mut parsed: Vec<Statement<Raw>> = parsed_results
        .into_iter()
        .map(|result| result.ast)
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
